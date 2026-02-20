use std::time::Instant;

use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tokio_util::bytes::BytesMut;
use tokio_util::codec::FramedRead;
use tracing::{debug, error, instrument, trace};

use crate::cache::messages::PipelineDescribe;
use crate::pg::cache_connection::CacheConnection;
use crate::pg::protocol::backend::PgBackendMessageType;
use crate::pg::protocol::encode::{
    bind_complete_encode, parse_complete_encode, ready_for_query_encode,
};
use crate::query::ast::Deparse;

use super::{
    CacheError, CacheResult,
    query_cache::{QueryType, WorkerRequest},
};

const BUFFER_SIZE_THRESHOLD: usize = 64 * 1024;

/// Guard that ensures a connection is returned to the pool.
///
/// Returns the connection via async `release()` on success.
/// On error (drop without release), the connection is discarded if poisoned
/// to avoid returning a connection with stale response data in its buffer.
struct ConnectionGuard {
    conn: Option<CacheConnection>,
    return_tx: Sender<CacheConnection>,
    poisoned: bool,
}

impl ConnectionGuard {
    fn new(conn: CacheConnection, return_tx: Sender<CacheConnection>) -> Self {
        Self {
            conn: Some(conn),
            return_tx,
            poisoned: false,
        }
    }

    /// Return the connection to the pool.
    async fn release(mut self) -> CacheResult<()> {
        if let Some(conn) = self.conn.take() {
            self.return_tx
                .send(conn)
                .await
                .map_err(|_| CacheError::NoConnection)?;
        }
        Ok(())
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        if self.poisoned {
            // Discard connection — may have unread response data
            self.conn.take();
            return;
        }
        if let Some(conn) = self.conn.take() {
            // try_send won't block; channel always has capacity since
            // pool size equals channel size
            let _ = self.return_tx.try_send(conn);
        }
    }
}

#[instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub async fn handle_cached_query(
    conn: CacheConnection,
    return_tx: Sender<CacheConnection>,
    msg: &mut WorkerRequest,
) -> CacheResult<()> {
    debug!("message query generation {}", msg.generation);

    let rv = if msg.result_formats.first().is_none_or(|&f| f == 0) {
        handle_cached_query_text(conn, return_tx, msg).await
    } else {
        handle_cached_query_binary(conn, return_tx, msg).await
    };

    debug!("cache hit");
    rv
}

/// Response state machine for the text (simple query) path.
///
/// A combined `SET gen; SELECT ...` produces:
/// CommandComplete (SET) → RowDescription → DataRow* → CommandComplete (SELECT) → ReadyForQuery
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TextResponseState {
    /// Waiting for SET CommandComplete
    SetComplete,
    /// Waiting for RowDescription (or DataRows if no rows)
    RowDescription,
    /// Streaming DataRow messages
    DataRows,
    /// Done — ReadyForQuery received
    Done,
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn handle_cached_query_text(
    conn: CacheConnection,
    return_tx: Sender<CacheConnection>,
    msg: &mut WorkerRequest,
) -> CacheResult<()> {
    let mut guard = ConnectionGuard::new(conn, return_tx);
    let mut conn = guard.conn.take().expect("connection available");

    // Generate SQL query from resolved AST (with schema-qualified table names)
    #[cfg(feature = "hotpath")]
    let _m = hotpath::functions::MeasurementGuard::new("hcqt:deparse", false, false);

    let mut sql = String::new();
    msg.resolved.deparse(&mut sql);
    // Append incoming query's LIMIT/OFFSET (the stored resolved query has no LIMIT)
    if let Some(limit) = &msg.limit {
        limit.deparse(&mut sql);
    }
    let combined_sql = format!("SET mem.query_generation = {}; {};", msg.generation, &sql);

    #[cfg(feature = "hotpath")]
    drop(_m);

    // Send query to cache database
    #[cfg(feature = "hotpath")]
    let _m = hotpath::functions::MeasurementGuard::new("hcqt:send_query", false, false);

    conn.simple_query_send(&combined_sql)
        .await
        .inspect_err(|_| {
            guard.poisoned = true;
        })?;

    #[cfg(feature = "hotpath")]
    drop(_m);

    // Stream results to client
    #[cfg(feature = "hotpath")]
    let _m = hotpath::functions::MeasurementGuard::new("hcqt:stream", false, false);

    let CacheConnection {
        stream,
        read_buf,
        codec,
    } = conn;
    let mut framed = FramedRead::new(stream, codec);
    *framed.read_buffer_mut() = read_buf;

    let query_type = msg.query_type;
    let has_sync = msg.has_sync;
    let has_parse = msg.has_parse;
    let has_bind = msg.has_bind;
    let pipeline_describe = msg.pipeline_describe;
    let parameter_description = msg.parameter_description.take();
    let client_socket = &mut msg.client_socket;

    let mut write_buf = BytesMut::with_capacity(BUFFER_SIZE_THRESHOLD);

    // Prepend ParseComplete / BindComplete for messages the proxy buffered
    if has_parse {
        parse_complete_encode(&mut write_buf);
    }
    if has_bind {
        bind_complete_encode(&mut write_buf);
    }

    let mut state = TextResponseState::SetComplete;

    loop {
        tokio::select! {
            frame = framed.next() => {
                let frame = match frame {
                    Some(Ok(frame)) => frame,
                    Some(Err(_)) | None => {
                        guard.poisoned = true;
                        return Err(CacheError::InvalidMessage.into());
                    }
                };

                #[cfg(feature = "hotpath")]
                let _match = hotpath::functions::MeasurementGuard::new("hcqt:match", false, false);

                match (state, frame.message_type) {
                    (TextResponseState::SetComplete, PgBackendMessageType::CommandComplete) => {
                        // SET response — skip, advance state
                        state = TextResponseState::RowDescription;
                    }
                    (TextResponseState::RowDescription, PgBackendMessageType::RowDescription) => {
                        if query_type == QueryType::Simple {
                            trace!("net: cache→client RowDescription ({} bytes, simple)", frame.data.len());
                            write_buf.extend_from_slice(&frame.data);
                        } else if pipeline_describe != PipelineDescribe::None {
                            // Describe was in pipeline — include Describe response
                            if pipeline_describe == PipelineDescribe::Statement
                            && let Some(param_desc) = &parameter_description
                            {
                                trace!("net: cache→client ParameterDescription ({} bytes, pipeline)", param_desc.len());
                                write_buf.extend_from_slice(param_desc);
                            }
                            trace!("net: cache→client RowDescription ({} bytes, pipeline)", frame.data.len());
                            write_buf.extend_from_slice(&frame.data);
                        } else {
                            trace!("net: cache skip RowDescription ({} bytes, extended — no Describe in pipeline)", frame.data.len());
                        }
                        state = TextResponseState::DataRows;
                    }
                    (TextResponseState::DataRows, PgBackendMessageType::DataRows) => {
                        trace!("net: cache→client DataRow ({} bytes)", frame.data.len());
                        write_buf.extend_from_slice(&frame.data);
                    }
                    (TextResponseState::DataRows, PgBackendMessageType::CommandComplete) => {
                        trace!("net: cache→client CommandComplete ({} bytes)", frame.data.len());
                        write_buf.extend_from_slice(&frame.data);
                        msg.timing.query_done_at = Some(Instant::now());
                    }
                    (_, PgBackendMessageType::ReadyForQuery) => {
                        state = TextResponseState::Done;
                    }
                    _ => {}
                }

                #[cfg(feature = "hotpath")]
                drop(_match);

            }
            result = client_socket.write_buf(&mut write_buf), if !write_buf.is_empty() =>
            {
                match result {
                    Ok(cnt) => {
                        trace!("net: cache→client flush (text, partial write, {} bytes)", cnt);
                    }
                    Err(_) => {
                        guard.poisoned = true;
                        error!("no client");
                        return Err(CacheError::Write.into());
                    }
                }
            }
        }

        if state == TextResponseState::Done {
            break;
        }
    }

    // Cache DB response fully consumed — return connection to pool immediately
    let parts = framed.into_parts();
    guard.conn = Some(CacheConnection {
        stream: parts.io,
        read_buf: parts.read_buf,
        codec: parts.codec,
    });
    guard.release().await?;

    // Append ReadyForQuery for simple queries (always) and extended queries with Sync
    if query_type == QueryType::Simple || has_sync {
        trace!("net: cache→client ReadyForQuery");
        ready_for_query_encode(&mut write_buf);
    }

    // Final flush
    if !write_buf.is_empty() {
        trace!(
            "net: cache→client final flush (text, {} bytes remaining)",
            write_buf.len()
        );
        client_socket
            .write_all_buf(&mut write_buf)
            .await
            .map_err(|_| {
                error!("no client");
                CacheError::Write
            })?;
    }

    msg.timing.response_written_at = Some(Instant::now());

    #[cfg(feature = "hotpath")]
    drop(_m);

    Ok(())
}

/// Response state machine for the binary (pipelined extended query) path.
///
/// Pipelined: SET (simple query) + Parse/Bind/[Describe('P')]/Execute/Sync produces:
/// CommandComplete (SET) → ReadyForQuery → ParseComplete → BindComplete →
/// [RowDescription →] DataRow* → CommandComplete (SELECT) → ReadyForQuery
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinaryResponseState {
    /// Waiting for SET CommandComplete
    SetComplete,
    /// Waiting for ReadyForQuery after SET
    SetReady,
    /// Waiting for ParseComplete
    ParseComplete,
    /// Waiting for BindComplete
    BindComplete,
    /// Waiting for RowDescription (only when include_describe is true)
    DescribeRow,
    /// Streaming DataRow messages
    DataRows,
    /// Done — final ReadyForQuery received
    Done,
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn handle_cached_query_binary(
    conn: CacheConnection,
    return_tx: Sender<CacheConnection>,
    msg: &mut WorkerRequest,
) -> CacheResult<()> {
    let mut guard = ConnectionGuard::new(conn, return_tx);
    let mut conn = guard.conn.take().expect("connection available");

    // Generate SQL query from resolved AST
    let mut sql = String::new();
    msg.resolved.deparse(&mut sql);
    // Append incoming query's LIMIT/OFFSET (the stored resolved query has no LIMIT)
    if let Some(limit) = &msg.limit {
        limit.deparse(&mut sql);
    }
    let set_sql = format!("SET mem.query_generation = {}", msg.generation);

    // Include Describe('P') in cache DB pipeline when the client's pipeline has a Describe
    let include_describe = msg.pipeline_describe != PipelineDescribe::None;

    // Send pipelined: SET (simple query) + Parse/Bind/[Describe('P')]/Execute/Sync (extended query)
    conn.pipelined_binary_query_send(&set_sql, &sql, include_describe)
        .await
        .inspect_err(|_| {
            guard.poisoned = true;
        })?;

    // Stream results to client
    let CacheConnection {
        stream,
        read_buf,
        codec,
    } = conn;
    let mut framed = FramedRead::new(stream, codec);
    *framed.read_buffer_mut() = read_buf;

    let has_sync = msg.has_sync;
    let has_parse = msg.has_parse;
    let has_bind = msg.has_bind;
    let pipeline_describe = msg.pipeline_describe;
    let parameter_description = msg.parameter_description.take();
    let client_socket = &mut msg.client_socket;

    let mut write_buf = BytesMut::with_capacity(BUFFER_SIZE_THRESHOLD);

    // Prepend ParseComplete / BindComplete for messages the proxy buffered
    if has_parse {
        parse_complete_encode(&mut write_buf);
    }
    if has_bind {
        bind_complete_encode(&mut write_buf);
    }

    let mut state = BinaryResponseState::SetComplete;

    loop {
        tokio::select! {
            frame = framed.next() => {
                let frame = match frame {
                    Some(Ok(frame)) => frame,
                    Some(Err(_)) | None => {
                        guard.poisoned = true;
                        return Err(CacheError::InvalidMessage.into());
                    }
                };

                match (state, frame.message_type) {
                    (BinaryResponseState::SetComplete, PgBackendMessageType::CommandComplete) => {
                        state = BinaryResponseState::SetReady;
                    }
                    (BinaryResponseState::SetReady, PgBackendMessageType::ReadyForQuery) => {
                        state = BinaryResponseState::ParseComplete;
                    }
                    (BinaryResponseState::ParseComplete, PgBackendMessageType::ParseComplete) => {
                        state = BinaryResponseState::BindComplete;
                    }
                    (BinaryResponseState::BindComplete, PgBackendMessageType::BindComplete) => {
                        state = if include_describe {
                            BinaryResponseState::DescribeRow
                        } else {
                            BinaryResponseState::DataRows
                        };
                    }
                    (BinaryResponseState::DescribeRow, PgBackendMessageType::RowDescription) => {
                        // Include Describe response from cache DB in client response
                        if pipeline_describe == PipelineDescribe::Statement
                            && let Some(param_desc) = &parameter_description
                        {
                            trace!("net: cache→client ParameterDescription (binary, {} bytes)", param_desc.len());
                            write_buf.extend_from_slice(param_desc);
                        }
                        trace!("net: cache→client RowDescription (binary, {} bytes)", frame.data.len());
                        write_buf.extend_from_slice(&frame.data);
                        state = BinaryResponseState::DataRows;
                    }
                    (BinaryResponseState::DataRows, PgBackendMessageType::DataRows) => {
                        trace!("net: cache→client DataRow (binary, {} bytes)", frame.data.len());
                        write_buf.extend_from_slice(&frame.data);
                    }
                    (BinaryResponseState::DataRows, PgBackendMessageType::CommandComplete) => {
                        trace!("net: cache→client CommandComplete (binary, {} bytes)", frame.data.len());
                        write_buf.extend_from_slice(&frame.data);
                        msg.timing.query_done_at = Some(Instant::now());
                    }
                    (_, PgBackendMessageType::ReadyForQuery)
                        if state != BinaryResponseState::SetComplete
                            && state != BinaryResponseState::SetReady =>
                    {
                        state = BinaryResponseState::Done;
                    }
                    _ => {}
                }
            }
            result = client_socket.write_buf(&mut write_buf), if !write_buf.is_empty() =>
            {
                match result {
                    Ok(cnt) => {
                        trace!("net: cache→client flush (binary, partial write, {} bytes)", cnt);
                    }
                    Err(_) => {
                        guard.poisoned = true;
                        error!("no client");
                        return Err(CacheError::Write.into());
                    }
                }
            }
        }

        if state == BinaryResponseState::Done {
            break;
        }
    }

    // Cache DB response fully consumed — return connection to pool immediately
    let parts = framed.into_parts();
    guard.conn = Some(CacheConnection {
        stream: parts.io,
        read_buf: parts.read_buf,
        codec: parts.codec,
    });
    guard.release().await?;

    // Append ReadyForQuery for extended queries with Sync
    if has_sync {
        trace!("net: cache→client ReadyForQuery (binary pipeline)");
        ready_for_query_encode(&mut write_buf);
    }

    // Final flush
    if !write_buf.is_empty() {
        trace!(
            "net: cache→client final flush (binary, {} bytes remaining)",
            write_buf.len()
        );
        client_socket
            .write_all_buf(&mut write_buf)
            .await
            .map_err(|_| {
                error!("no client");
                CacheError::Write
            })?;
    }

    msg.timing.response_written_at = Some(Instant::now());

    Ok(())
}
