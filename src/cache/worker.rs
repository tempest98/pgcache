use std::ops::Deref;
use std::time::Instant;

use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::Sender;
use tokio_postgres::{Client, SimpleQueryMessage};
use tokio_util::bytes::{BufMut, BytesMut};
use tracing::{debug, error, info, instrument};

use crate::pg::protocol::encode::*;
use crate::query::ast::Deparse;

use super::{
    CacheError, CacheResult, MapIntoReport,
    query_cache::{QueryType, WorkerRequest},
};

const BUFFER_SIZE_THRESHOLD: usize = 64 * 1024;

/// Guard that ensures a connection is returned to the pool.
///
/// Returns the connection via async `release()` when done, or via
/// sync `try_send()` on drop if the function exits early due to an error.
struct ConnectionGuard {
    client: Option<Client>,
    return_tx: Sender<Client>,
}

impl ConnectionGuard {
    fn new(client: Client, return_tx: Sender<Client>) -> Self {
        Self {
            client: Some(client),
            return_tx,
        }
    }

    /// Return the connection to the pool.
    async fn release(mut self) -> CacheResult<()> {
        if let Some(client) = self.client.take() {
            self.return_tx
                .send(client)
                .await
                .map_err(|_| CacheError::NoConnection)?;
        }
        Ok(())
    }
}

impl Deref for ConnectionGuard {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        self.client.as_ref().expect("connection available")
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            // try_send won't block; channel always has capacity since
            // pool size equals channel size
            let _ = self.return_tx.try_send(client);
        }
    }
}

#[instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub async fn handle_cached_query(
    client: Client,
    return_tx: Sender<Client>,
    msg: &mut WorkerRequest,
) -> CacheResult<()> {
    debug!("message query generation {}", msg.generation);

    let rv = if msg.result_formats.first().is_none_or(|&f| f == 0) {
        handle_cached_query_text(client, return_tx, msg).await
    } else {
        handle_cached_query_binary(client, return_tx, msg).await
    };

    info!("cache hit");
    rv
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn handle_cached_query_text(
    client: Client,
    return_tx: Sender<Client>,
    msg: &mut WorkerRequest,
) -> CacheResult<()> {
    let conn = ConnectionGuard::new(client, return_tx);

    // Generate SQL query from resolved AST (with schema-qualified table names)
    let mut sql = String::new();
    msg.resolved.deparse(&mut sql);

    let combined_sql = format!("SET mem.query_generation = {}; {};", msg.generation, &sql);

    // Execute query against cache database
    let res = conn
        .simple_query(&combined_sql)
        .await
        .map_into_report::<CacheError>()?;

    // Return connection early - query is done, only writing to client remains
    conn.release().await?;

    // Record query completion time
    msg.timing.query_done_at = Some(Instant::now());

    let [
        SimpleQueryMessage::CommandComplete(_),
        SimpleQueryMessage::RowDescription(desc),
        data_rows @ ..,
        SimpleQueryMessage::CommandComplete(cnt),
    ] = res.as_slice()
    else {
        return Err(CacheError::InvalidMessage.into());
    };

    let mut buf = BytesMut::new();

    if msg.query_type == QueryType::Simple {
        row_description_encode(desc, &mut buf);
        // trace!("(w) client write {:?}", buf);
        if msg.client_socket.write_all_buf(&mut buf).await.is_err() {
            error!("no client");
            return Err(CacheError::Write.into());
        }
        // Buffer is guaranteed to be clear if there was no error
    }

    for query_msg in data_rows {
        let SimpleQueryMessage::Row(row) = query_msg else {
            return Err(CacheError::InvalidMessage.into());
        };
        // Use raw buffer directly to avoid decode/encode overhead
        // The raw buffer contains field data but not the field count
        let raw_data = row.raw_buffer_bytes();
        let field_count = row.len() as u16;

        buf.put_u8(b'D'); // DATA_ROW_TAG
        buf.put_i32(4 + 2 + raw_data.len() as i32); // 4 (length field) + 2 (field count) + data
        buf.put_u16(field_count);
        buf.put_slice(raw_data);

        // Send data if more than 64kB have been accumulated
        if buf.len() > BUFFER_SIZE_THRESHOLD
            && msg.client_socket.write_all_buf(&mut buf).await.is_err()
        {
            error!("no client");
            return Err(CacheError::Write.into());
        }
    }
    // trace!("(w) client write data");
    if !buf.is_empty() && msg.client_socket.write_all_buf(&mut buf).await.is_err() {
        error!("no client");
        return Err(CacheError::Write.into());
    }

    let mut buf = BytesMut::new();
    command_complete_encode(*cnt, &mut buf);

    if msg.query_type == QueryType::Simple {
        ready_for_query_encode(&mut buf);
    }

    // trace!("(w) client write {:?}", buf);
    if msg.client_socket.write_all_buf(&mut buf).await.is_err() {
        error!("no client");
        return Err(CacheError::Write.into());
    }

    // Record response written time
    msg.timing.response_written_at = Some(Instant::now());

    Ok(())
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn handle_cached_query_binary(
    client: Client,
    return_tx: Sender<Client>,
    msg: &mut WorkerRequest,
) -> CacheResult<()> {
    let conn = ConnectionGuard::new(client, return_tx);

    // Generate SQL query from resolved AST (with schema-qualified table names)
    let mut sql = String::new();
    msg.resolved.deparse(&mut sql);

    let set_gen = format!("SET mem.query_generation = {}", msg.generation);
    conn.simple_query(&set_gen)
        .await
        .map_into_report::<CacheError>()?;

    // Execute query against cache database
    let res = conn
        .query(&sql, &[])
        .await
        .map_into_report::<CacheError>()?;

    // Return connection early - query is done, only writing to client remains
    conn.release().await?;

    // Record query completion time
    msg.timing.query_done_at = Some(Instant::now());

    let mut buf = BytesMut::new();
    for row in &res {
        // Use raw buffer directly to avoid decode/encode overhead
        // The raw buffer contains field data but not the field count
        let raw_data = row.raw_buffer_bytes();
        let field_count = row.len() as u16;

        buf.put_u8(b'D'); // DATA_ROW_TAG
        buf.put_i32(4 + 2 + raw_data.len() as i32); // 4 (length field) + 2 (field count) + data
        buf.put_u16(field_count);
        buf.put_slice(raw_data);

        //send data if more than 64kB have been accumulated
        if buf.len() > BUFFER_SIZE_THRESHOLD {
            if msg.client_socket.write_all_buf(&mut buf).await.is_err() {
                error!("no client");
                return Err(CacheError::Write.into());
            }
            buf.clear();
        }
    }
    // trace!("(w) client write data [{:?}]", buf);
    if !buf.is_empty() && msg.client_socket.write_all_buf(&mut buf).await.is_err() {
        error!("no client");
        return Err(CacheError::Write.into());
    }

    let mut buf = BytesMut::new();
    command_complete_encode(res.len() as u64, &mut buf);

    // trace!("(w) client write {:?}", buf);
    if msg.client_socket.write_all_buf(&mut buf).await.is_err() {
        error!("no client");
        return Err(CacheError::Write.into());
    }

    // Record response written time
    msg.timing.response_written_at = Some(Instant::now());

    Ok(())
}
