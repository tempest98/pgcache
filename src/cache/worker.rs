use std::rc::Rc;

use tokio::io::AsyncWriteExt;
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage};
use tokio_util::bytes::{BufMut, BytesMut};
use tracing::{debug, error, instrument};

use crate::pg::protocol::encode::*;
use crate::query::ast::Deparse;
use crate::settings::Settings;

use super::{
    CacheError, CacheResult, MapIntoReport,
    query_cache::{QueryType, WorkerRequest},
};

const BUFFER_SIZE_THRESHOLD: usize = 64 * 1024;

#[derive(Debug, Clone)]
pub struct CacheWorker {
    db_cache: Rc<Client>,
}

impl CacheWorker {
    pub async fn new(settings: &Settings) -> CacheResult<Self> {
        debug!(
            "CacheWorker: connecting to cache db at {}:{}",
            settings.cache.host, settings.cache.port
        );
        let (cache_client, cache_connection) = Config::new()
            .host(&settings.cache.host)
            .port(settings.cache.port)
            .user(&settings.cache.user)
            .dbname(&settings.cache.database)
            .connect(NoTls)
            .await
            .map_into_report::<CacheError>()?;

        debug!("CacheWorker: connection established");

        //task to process connection to cache pg db
        tokio::spawn(async move {
            debug!("CacheWorker: connection task started");
            if let Err(e) = cache_connection.await {
                error!("CacheWorker connection error: {e}");
            } else {
                debug!("CacheWorker: connection closed normally");
            }
        });

        Ok(Self {
            db_cache: Rc::new(cache_client),
        })
    }

    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn handle_cached_query(&self, msg: &mut WorkerRequest) -> CacheResult<()> {
        // Set generation before query execution (enables row tracking in pgcache_pgrx)
        debug!("message query generation {}", msg.generation);

        let rv = if msg.result_formats.first().is_none_or(|&f| f == 0) {
            self.handle_cached_query_text(msg).await
        } else {
            self.handle_cached_query_binary(msg).await
        };

        debug!("cache hit");
        rv
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn handle_cached_query_text(&self, msg: &mut WorkerRequest) -> CacheResult<()> {
        // Generate SQL query from resolved AST (with schema-qualified table names)
        let mut sql = String::new();
        msg.resolved.deparse(&mut sql);

        let combined_sql = format!("SET mem.query_generation = {}; {};", msg.generation, &sql);

        // Execute query against cache database
        let res = self
            .db_cache
            .simple_query(&combined_sql)
            .await
            .map_into_report::<CacheError>()?;

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

        Ok(())
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn handle_cached_query_binary(&self, msg: &mut WorkerRequest) -> CacheResult<()> {
        // Generate SQL query from resolved AST (with schema-qualified table names)
        let mut sql = String::new();
        msg.resolved.deparse(&mut sql);

        let set_gen = format!("SET mem.query_generation = {}", msg.generation);
        self.db_cache
            .simple_query(&set_gen)
            .await
            .map_into_report::<CacheError>()?;

        // Execute query against cache database
        let res = self
            .db_cache
            .query(&sql, &[])
            .await
            .map_into_report::<CacheError>()?;

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

        Ok(())
    }
}
