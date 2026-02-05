use std::{
    collections::{HashMap, VecDeque},
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    time::Instant,
};

use rootcause::Report;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpStream, lookup_host},
    runtime::Builder,
    select,
    sync::mpsc::{UnboundedReceiver, channel},
    task::{LocalSet, spawn_local},
};
use tokio_stream::StreamExt;
use tokio_util::{
    bytes::{Buf, BytesMut},
    codec::FramedRead,
};
use tracing::{debug, error, instrument};

use crate::{
    cache::{CacheMessage, CacheReply, ProxyMessage, query::CacheableQuery},
    metrics::names,
    pg::protocol::{
        backend::{
            AUTHENTICATION_SASL, PgBackendMessage, PgBackendMessageCodec, PgBackendMessageType,
            authentication_type, data_row_first_column, parameter_status_parse,
        },
        extended::{
            ParsedBindMessage, ParsedParseMessage, Portal, PreparedStatement, StatementType,
            parse_bind_message, parse_close_message, parse_describe_message, parse_execute_message,
            parse_parameter_description, parse_parse_message,
        },
        frontend::{
            PgFrontendMessage, PgFrontendMessageCodec, PgFrontendMessageType,
            simple_query_message_build, startup_message_parameter,
        },
    },
    settings::{Settings, SslMode},
    timing::{QueryId, QueryTiming, timing_record},
    tls::{self},
};

use super::client_stream::{ClientReadHalf, ClientSocketSource, ClientStream, ClientWriteHalf};
use super::query::{Action, ForwardReason, handle_query};
use super::search_path::SearchPath;
use super::tls_stream::{TlsReadHalf, TlsStream, TlsWriteHalf};
use super::{CacheSender, ConnectionError, ConnectionResult, ProxyMode, ProxyStatus};
use crate::result::{MapIntoReport, ReportExt};

/// Guard that decrements active connections gauge when dropped.
struct ActiveConnectionGuard;

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        metrics::gauge!(names::CONNECTIONS_ACTIVE).decrement(1.0);
    }
}

// ============================================================================
// OriginStream - type aliases using generic TLS stream types
// ============================================================================

/// Origin database connection stream, either plain TCP or TLS-encrypted.
pub type OriginStream = TlsStream<rustls::ClientConnection>;

/// Borrowed read half of an OriginStream.
pub type OriginReadHalf<'a> = TlsReadHalf<'a, rustls::ClientConnection>;

/// Borrowed write half of an OriginStream.
pub type OriginWriteHalf<'a> = TlsWriteHalf<'a, rustls::ClientConnection>;

/// Create an OriginStream from a tokio-rustls TlsStream.
///
/// Decomposes the TlsStream to allow borrowed splits with `.writable()`.
fn origin_stream_from_tls(tls_stream: tokio_rustls::client::TlsStream<TcpStream>) -> OriginStream {
    let (tcp, client_connection) = tls_stream.into_inner();
    TlsStream::Tls {
        tcp,
        tls_state: Arc::new(std::sync::Mutex::new(client_connection)),
    }
}

/// Manages state for a single client connection.
/// Encapsulates transaction state, query fingerprint cache, and protocol state.
pub(super) struct ConnectionState {
    /// data waiting to be written to origin
    origin_write_buf: VecDeque<BytesMut>,

    /// data waiting to be written to client
    client_write_buf: VecDeque<BytesMut>,

    /// Cache of query fingerprints to cacheability decisions
    fingerprint_cache: HashMap<u64, Result<Box<CacheableQuery>, ForwardReason>>,

    /// Whether the connection is currently in a transaction
    in_transaction: bool,

    /// Current proxy mode (reading, writing to client/origin/cache)
    proxy_mode: ProxyMode,

    /// Proxy status (normal or degraded if cache is unavailable)
    proxy_status: ProxyStatus,

    /// Source for creating ClientSocket instances for cache queries.
    /// The connection handler writes directly to the client stream, but when
    /// sending queries to the cache, we create a ClientSocket from this source.
    client_socket_source: ClientSocketSource,

    /// Extended protocol: prepared statements by name
    prepared_statements: HashMap<String, PreparedStatement>,

    /// Extended protocol: portals (bound statements) by name
    portals: HashMap<String, Portal>,

    /// Extended protocol: name of statement most recently described (awaiting ParameterDescription)
    pending_describe_statement: Option<String>,

    /// PostgreSQL session user from startup message
    /// TODO: Track SET ROLE queries to update effective user for permission checks
    session_user: Option<String>,

    /// Parsed search_path for this connection (from ParameterStatus or SHOW query)
    search_path: Option<SearchPath>,

    /// Whether we're waiting for SHOW search_path response (pre-18 fallback)
    search_path_query_pending: bool,

    /// Whether this is the first ReadyForQuery after authentication
    first_ready_for_query: bool,

    /// Start time of the current query (for latency tracking)
    query_start: Option<Instant>,

    /// Start time of origin query (for origin latency tracking)
    origin_query_start: Option<Instant>,

    /// Per-query timing data for the current query
    current_timing: Option<QueryTiming>,
}

impl ConnectionState {
    fn new(client_socket_source: ClientSocketSource) -> Self {
        Self {
            origin_write_buf: VecDeque::new(),
            client_write_buf: VecDeque::new(),
            fingerprint_cache: HashMap::new(),
            in_transaction: false,
            proxy_mode: ProxyMode::Read,
            proxy_status: ProxyStatus::Normal,
            client_socket_source,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            pending_describe_statement: None,
            session_user: None,
            search_path: None,
            search_path_query_pending: false,
            first_ready_for_query: true,
            query_start: None,
            origin_query_start: None,
            current_timing: None,
        }
    }
}

impl ConnectionState {
    /// Handle a message from the client (frontend).
    /// Determines whether to forward to origin, check cache, or take other action.
    #[expect(clippy::wildcard_enum_match_arm)]
    async fn handle_client_message(&mut self, msg: PgFrontendMessage) {
        // debug!("client {:?}", &msg);
        match msg.message_type {
            PgFrontendMessageType::Query => {
                metrics::counter!(names::QUERIES_TOTAL).increment(1);
                metrics::counter!(names::PROTOCOL_SIMPLE_QUERIES).increment(1);
                self.query_start = Some(Instant::now());

                if !self.in_transaction {
                    self.proxy_mode =
                        match handle_query(&msg.data, &mut self.fingerprint_cache).await {
                            Ok(Action::Forward(reason)) => {
                                match reason {
                                    ForwardReason::UnsupportedStatement => {
                                        metrics::counter!(names::QUERIES_UNSUPPORTED).increment(1);
                                    }
                                    ForwardReason::UncacheableSelect => {
                                        metrics::counter!(names::QUERIES_UNCACHEABLE).increment(1);
                                    }
                                    ForwardReason::Invalid => {
                                        metrics::counter!(names::QUERIES_INVALID).increment(1);
                                    }
                                }
                                self.origin_query_start = Some(Instant::now());
                                self.origin_write_buf.push_back(msg.data);
                                ProxyMode::Read
                            }
                            Ok(Action::CacheCheck(ast)) => {
                                // Create timing with fingerprint from the cacheable query
                                use crate::query::ast::query_expr_fingerprint;
                                let fingerprint = query_expr_fingerprint(&ast.query);
                                let query_id = QueryId::new(fingerprint);
                                let received_at = self.query_start.unwrap_or_else(Instant::now);
                                let mut timing = QueryTiming::new(query_id, received_at);
                                timing.parsed_at = Some(Instant::now());
                                self.current_timing = Some(timing);
                                ProxyMode::CacheWrite(CacheMessage::Query(msg.data, ast))
                            }
                            Err(e) => {
                                metrics::counter!(names::QUERIES_UNCACHEABLE).increment(1);
                                metrics::counter!(names::QUERIES_INVALID).increment(1);
                                error!("handle_query {}", e);
                                self.origin_query_start = Some(Instant::now());
                                self.origin_write_buf.push_back(msg.data);
                                ProxyMode::Read
                            }
                        };
                } else {
                    metrics::counter!(names::QUERIES_UNCACHEABLE).increment(1);
                    self.origin_query_start = Some(Instant::now());
                    self.origin_write_buf.push_back(msg.data);
                }
            }
            PgFrontendMessageType::Parse => {
                self.handle_parse_message(msg);
            }
            PgFrontendMessageType::Bind => {
                self.handle_bind_message(msg);
            }
            PgFrontendMessageType::Execute => {
                self.handle_execute_message(msg);
            }
            PgFrontendMessageType::Describe => {
                self.handle_describe_message(msg);
            }
            PgFrontendMessageType::Close => {
                self.handle_close_message(msg);
            }
            PgFrontendMessageType::Sync => {
                self.handle_sync_message(msg);
            }
            PgFrontendMessageType::Flush => {
                // Flush requests the server to send any pending data
                self.origin_write_buf.push_back(msg.data);
            }
            PgFrontendMessageType::Startup => {
                self.session_user = startup_message_parameter(&msg.data, "user").map(String::from);
                self.origin_write_buf.push_back(msg.data);
            }
            PgFrontendMessageType::SslRequest => {
                // SSLRequest should be handled during connection setup before framing begins.
                // If we receive it here, something unexpected happened - log a warning.
                // Respond with 'N' to allow the connection to continue.
                debug!("unexpected SslRequest after TLS negotiation phase, responding 'N'");
                self.client_write_buf.push_back(BytesMut::from(&[b'N'][..]));
            }
            PgFrontendMessageType::PasswordMessageFamily => {
                // Forward password/SASL messages to origin.
                // Note: Channel binding cannot be modified in transit because SCRAM includes
                // the gs2-header in the cryptographic proof. Clients connecting via TLS must
                // use channel_binding=disable in their connection string.
                self.origin_write_buf.push_back(msg.data);
            }
            _ => {
                // All other message types - forward to origin
                self.origin_write_buf.push_back(msg.data);
            }
        }
    }

    /// Handle a message from the origin database (backend).
    /// Updates transaction state, captures parameter OIDs, and forwards to client.
    #[expect(clippy::wildcard_enum_match_arm)]
    fn handle_origin_message(&mut self, mut msg: PgBackendMessage) {
        // debug!("origin {:?}", &msg);

        // Intercept SHOW search_path response (don't forward to client)
        if self.search_path_query_pending {
            match msg.message_type {
                PgBackendMessageType::DataRows => {
                    // Extract search_path value from the DataRow
                    if let Some(value) = data_row_first_column(&msg.data) {
                        debug!("received search_path from SHOW query: {}", value);
                        self.search_path = Some(SearchPath::parse(value));
                    }
                }
                PgBackendMessageType::ReadyForQuery => {
                    // SHOW query complete, resume normal operation
                    debug!("search_path query complete");
                    self.search_path_query_pending = false;
                }
                _ => {
                    // Ignore RowDescription, CommandComplete, etc.
                }
            }
            // Don't forward any of these messages to client
            return;
        }

        match msg.message_type {
            PgBackendMessageType::ParameterStatus => {
                // Check for search_path parameter (PG 18+ sends this during startup)
                if let Some(("search_path", value)) = parameter_status_parse(&msg.data) {
                    debug!("received search_path from ParameterStatus: {}", value);
                    self.search_path = Some(SearchPath::parse(value));
                }
            }
            PgBackendMessageType::ParameterDescription => {
                // Update the pending statement's parameter OIDs from the server response
                if let Some(stmt_name) = self.pending_describe_statement.take()
                    && let Ok(parsed) = parse_parameter_description(&msg.data)
                    && let Some(stmt) = self.prepared_statements.get_mut(&stmt_name)
                {
                    debug!(
                        "updated statement '{}' with parameter OIDs {:?}",
                        stmt_name, parsed.parameter_oids
                    );
                    stmt.parameter_oids = parsed.parameter_oids;
                }
            }
            PgBackendMessageType::Authentication => {
                if authentication_type(&msg.data).is_some_and(|v| v == AUTHENTICATION_SASL) {
                    // Strip SCRAM-SHA-256-PLUS from SASL authentication options.
                    // Channel binding cannot be supported because the proxy terminates TLS.
                    let needle = b"SCRAM-SHA-256-PLUS\0";
                    if let Some(pos) = msg
                        .data
                        .windows(needle.len())
                        .position(|window| window == needle)
                    {
                        // Remove needle in place using split/unsplit
                        let mut tail = msg.data.split_off(pos);
                        let after_needle = tail.split_off(needle.len());
                        msg.data.unsplit(after_needle);

                        // Update the length field (bytes 1-4, big-endian i32, excludes tag byte)
                        // Safety: Message format guarantees at least 5 bytes (1 tag + 4 length)
                        let new_len = (msg.data.len() - 1) as i32;
                        #[expect(
                            clippy::indexing_slicing,
                            reason = "PostgreSQL message format guarantees 5+ bytes"
                        )]
                        msg.data[1..5].copy_from_slice(&new_len.to_be_bytes());
                    }
                }
            }
            PgBackendMessageType::ReadyForQuery => {
                // ReadyForQuery message contains transaction status at byte 5
                // 'I' = idle (not in transaction)
                // 'T' = in transaction block
                // 'E' = in failed transaction block
                self.in_transaction = msg.data.get(5).is_some_and(|&b| b == b'T' || b == b'E');

                // Record origin latency
                if let Some(start) = self.origin_query_start.take() {
                    let duration = start.elapsed();
                    metrics::histogram!(names::ORIGIN_LATENCY_SECONDS)
                        .record(duration.as_secs_f64());
                }

                // Record total query latency
                if let Some(start) = self.query_start.take() {
                    let duration = start.elapsed();
                    metrics::histogram!(names::QUERY_LATENCY_SECONDS)
                        .record(duration.as_secs_f64());
                }

                // Clean up unnamed portals when transaction ends
                if !self.in_transaction {
                    self.portals.retain(|name, _| !name.is_empty());
                }

                // On first ReadyForQuery, check if we need to query search_path (pre-18 fallback)
                if self.first_ready_for_query {
                    self.first_ready_for_query = false;

                    if self.search_path.is_none() && !self.search_path_query_pending {
                        // PG < 18 doesn't send search_path in ParameterStatus
                        // Send SHOW search_path query to origin
                        debug!("search_path not received, sending SHOW search_path query");
                        self.search_path_query_pending = true;
                        let query_msg = simple_query_message_build("SHOW search_path;");
                        self.origin_write_buf.push_back(query_msg);
                    }
                }
            }
            _ => {}
        }

        // debug!("{:?}", &msg.data);
        self.client_write_buf.push_back(msg.data);
    }

    /// Handle a reply from the cache.
    /// If cache indicates error or needs forwarding, send query to origin instead.
    fn handle_cache_reply(&mut self, reply: CacheReply) {
        match reply {
            CacheReply::Data(_) => {
                // Data chunks are written directly to the client by the cache worker
                // via CacheClientWriter, so this should not happen through the channel.
                // No action needed - just wait for Complete.
            }
            CacheReply::Complete(_, timing) => {
                metrics::counter!(names::QUERIES_CACHE_HIT).increment(1);
                // Record query latency for cache hit
                if let Some(start) = self.query_start.take() {
                    let duration = start.elapsed();
                    metrics::histogram!(names::QUERY_LATENCY_SECONDS)
                        .record(duration.as_secs_f64());
                }
                // Record per-stage timing breakdown
                if let Some(timing) = timing {
                    timing_record(&timing);
                }
                self.proxy_mode = ProxyMode::Read;
            }
            CacheReply::Error(buf) => {
                metrics::counter!(names::QUERIES_CACHE_ERROR).increment(1);
                debug!("forwarding to origin");
                self.origin_query_start = Some(Instant::now());
                self.origin_write_buf.push_back(buf);
                self.proxy_mode = ProxyMode::Read;
            }
            CacheReply::Forward(buf) => {
                metrics::counter!(names::QUERIES_CACHE_MISS).increment(1);
                debug!("forwarding to origin");
                self.origin_query_start = Some(Instant::now());
                self.origin_write_buf.push_back(buf);
                self.proxy_mode = ProxyMode::Read;
            }
        }
    }

    /// Handle Parse message - analyze cacheability, store prepared statement, and forward to origin.
    fn handle_parse_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_parse_message(&msg.data) {
            // Analyze SQL type (regardless of transaction state - deferred to Execute)
            let sql_type = match pg_query::parse(&parsed.sql) {
                Ok(ast) => match crate::query::ast::query_expr_convert(&ast) {
                    Ok(query) => match CacheableQuery::try_from(&query) {
                        Ok(cacheable_query) => StatementType::Cacheable(Box::new(cacheable_query)),
                        Err(_) => StatementType::UncacheableSelect,
                    },
                    Err(_) => StatementType::NonSelect,
                },
                Err(_) => StatementType::ParseError,
            };

            self.statement_store(parsed, sql_type);
        }
        self.origin_write_buf.push_back(msg.data);
    }

    /// Handle Bind message - store portal and forward to origin.
    fn handle_bind_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_bind_message(&msg.data) {
            self.portal_store(parsed);
        }
        self.origin_write_buf.push_back(msg.data);
    }

    /// Handle Execute message - check cache for cacheable parameterized queries, otherwise forward to origin.
    fn handle_execute_message(&mut self, msg: PgFrontendMessage) {
        metrics::counter!(names::QUERIES_TOTAL).increment(1);
        metrics::counter!(names::PROTOCOL_EXTENDED_QUERIES).increment(1);
        self.query_start = Some(Instant::now());

        self.proxy_mode = match self.try_cache_execute(&msg) {
            Some(cache_msg) => {
                // Create timing with fingerprint from the cacheable query
                use crate::query::ast::query_expr_fingerprint;
                let fingerprint = match &cache_msg {
                    CacheMessage::Query(_, ast)
                    | CacheMessage::QueryParameterized(_, ast, _, _) => {
                        query_expr_fingerprint(&ast.query)
                    }
                };
                let query_id = QueryId::new(fingerprint);
                let received_at = self.query_start.unwrap_or_else(Instant::now);
                let mut timing = QueryTiming::new(query_id, received_at);
                timing.parsed_at = Some(Instant::now());
                self.current_timing = Some(timing);
                ProxyMode::CacheWrite(cache_msg)
            }
            None => {
                metrics::counter!(names::QUERIES_UNCACHEABLE).increment(1);

                // Track statement type for metrics
                if let Ok(parsed) = parse_execute_message(&msg.data)
                    && let Some(portal) = self.portals.get(&parsed.portal_name)
                    && let Some(stmt) = self.prepared_statements.get(&portal.statement_name)
                {
                    match &stmt.sql_type {
                        StatementType::NonSelect => {
                            metrics::counter!(names::QUERIES_UNSUPPORTED).increment(1);
                        }
                        StatementType::ParseError => {
                            metrics::counter!(names::QUERIES_INVALID).increment(1);
                        }
                        StatementType::Cacheable(_) | StatementType::UncacheableSelect => {}
                    }
                }

                self.origin_query_start = Some(Instant::now());
                self.origin_write_buf.push_back(msg.data);
                ProxyMode::Read
            }
        };
    }

    /// Attempt to create a cache message for Execute, returning None if caching not possible.
    fn try_cache_execute(&self, msg: &PgFrontendMessage) -> Option<CacheMessage> {
        use crate::cache::QueryParameters;

        // Check transaction state first (cheapest check)
        if self.in_transaction {
            return None;
        }

        // Parse Execute message
        let parsed = parse_execute_message(&msg.data).ok()?;
        debug!("parsed execute message {:?}", parsed);

        // Look up portal
        let portal = self.portals.get(&parsed.portal_name)?;

        // Only handle implicit or uniform result formats
        if portal.result_formats.len() > 1
            && portal
                .result_formats
                .windows(2)
                .all(|w| matches!(w, [a, b] if a == b))
        {
            return None;
        }

        // Look up prepared statement
        let stmt = self.prepared_statements.get(&portal.statement_name)?;

        // Check if cacheable - extract CacheableQuery from StatementType
        let cacheable_query = match &stmt.sql_type {
            StatementType::Cacheable(query) => query.clone(),
            StatementType::NonSelect
            | StatementType::UncacheableSelect
            | StatementType::ParseError => return None,
        };

        // All checks passed - use cache
        Some(CacheMessage::QueryParameterized(
            msg.data.clone(),
            cacheable_query,
            QueryParameters {
                values: portal.parameter_values.clone(),
                formats: portal.parameter_formats.clone(),
                oids: stmt.parameter_oids.clone(),
            },
            portal.result_formats.clone(),
        ))
    }

    /// Handle Describe message - track statement name for ParameterDescription and forward to origin.
    fn handle_describe_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_describe_message(&msg.data) {
            // Track statement describes so we can update OIDs when ParameterDescription arrives
            if parsed.describe_type == b'S' {
                self.pending_describe_statement = Some(parsed.name);
            }
        }
        self.origin_write_buf.push_back(msg.data);
    }

    /// Handle Close message - clean up state and forward to origin.
    fn handle_close_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_close_message(&msg.data) {
            match parsed.close_type {
                b'S' => self.statement_close(&parsed.name),
                b'P' => self.portal_close(&parsed.name),
                _ => {}
            }
        }
        self.origin_write_buf.push_back(msg.data);
    }

    /// Handle Sync message - forward to origin.
    fn handle_sync_message(&mut self, msg: PgFrontendMessage) {
        self.origin_write_buf.push_back(msg.data);
    }

    /// Store a prepared statement in connection state.
    fn statement_store(&mut self, parsed: ParsedParseMessage, sql_type: StatementType) {
        let stmt = PreparedStatement {
            name: parsed.statement_name.clone(),
            sql: parsed.sql,
            parameter_oids: parsed.parameter_oids,
            sql_type,
        };
        debug!("parsed statement insert {}", parsed.statement_name);
        // Only increment gauge if this is a new statement (not replacing)
        if !self
            .prepared_statements
            .contains_key(&parsed.statement_name)
        {
            metrics::gauge!(names::PROTOCOL_PREPARED_STATEMENTS).increment(1.0);
        }
        self.prepared_statements
            .entry(parsed.statement_name)
            .or_insert(stmt);
    }

    /// Store a portal in connection state.
    fn portal_store(&mut self, parsed: ParsedBindMessage) {
        let portal = Portal {
            name: parsed.portal_name.clone(),
            statement_name: parsed.statement_name,
            parameter_values: parsed.parameter_values,
            parameter_formats: parsed.parameter_formats,
            result_formats: parsed.result_formats,
        };

        debug!("parsed portal insert {}", parsed.portal_name);
        debug!("parsed portal insert {:?}", portal);
        self.portals.insert(parsed.portal_name, portal);
    }

    /// Remove a prepared statement from connection state.
    fn statement_close(&mut self, name: &str) {
        if self.prepared_statements.remove(name).is_some() {
            metrics::gauge!(names::PROTOCOL_PREPARED_STATEMENTS).decrement(1.0);
        }
    }

    /// Remove a portal from connection state.
    fn portal_close(&mut self, name: &str) {
        self.portals.remove(name);
    }

    /// Clear all prepared statements from connection state.
    #[expect(unused)]
    fn statements_clear(&mut self) {
        self.prepared_statements.clear();
    }

    /// Clear all portals from connection state.
    #[expect(unused)]
    fn portals_clear(&mut self) {
        self.portals.clear();
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "VecDeque access guarded by !is_empty()"
    )]
    async fn connection_select<'a, 'b>(
        &mut self,
        origin_read: &mut Pin<&mut FramedRead<OriginReadHalf<'b>, PgBackendMessageCodec>>,
        client_read: &mut Pin<&mut FramedRead<ClientReadHalf<'a>, PgFrontendMessageCodec>>,
        origin_write: &mut Pin<&mut OriginWriteHalf<'b>>,
        client_write: &mut Pin<&mut ClientWriteHalf<'a>>,
    ) -> ConnectionResult<()> {
        select! {
            res = client_read.next() => {
                match res {
                    Some(Ok(msg)) => {
                        self.handle_client_message(msg).await;
                    }
                    Some(Err(err)) => {
                        debug!("client read error [{}]", err);
                        return Err(ConnectionError::ProtocolError(err).into());
                    }
                    None => {
                        debug!("client stream closed");
                        return Err(ConnectionError::IoError(io::Error::new(
                            io::ErrorKind::ConnectionReset,
                            "client disconnected",
                        )).into());
                    }
                }
            }
            res = origin_read.next() => {
                match res {
                    Some(Ok(msg)) => {
                        self.handle_origin_message(msg);
                    }
                    Some(Err(err)) => {
                        debug!("origin read error [{}]", err);
                        return Err(ConnectionError::ProtocolError(err).into());
                    }
                    None => {
                        debug!("origin stream closed");
                        return Err(ConnectionError::IoError(io::Error::new(
                            io::ErrorKind::ConnectionReset,
                            "origin disconnected",
                        )).into());
                    }
                }
            }
            _ = origin_write.writable(), if !self.origin_write_buf.is_empty() => {
                origin_write.write_buf(&mut self.origin_write_buf[0]).await
                    .map_err(ConnectionError::IoError)?;
                if !self.origin_write_buf[0].has_remaining() {
                    self.origin_write_buf.pop_front();
                }
            }
            _ = client_write.writable(), if !self.client_write_buf.is_empty() => {
                client_write.write_buf(&mut self.client_write_buf[0]).await
                    .map_err(ConnectionError::IoError)?;
                if !self.client_write_buf[0].has_remaining() {
                    self.client_write_buf.pop_front();
                }
            }
        };

        Ok(())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "VecDeque access guarded by !is_empty()"
    )]
    async fn connection_select_with_cache<'a, 'b>(
        &mut self,
        origin_read: &mut Pin<&mut FramedRead<OriginReadHalf<'b>, PgBackendMessageCodec>>,
        origin_write: &mut Pin<&mut OriginWriteHalf<'b>>,
        client_write: &mut Pin<&mut ClientWriteHalf<'a>>,
    ) -> ConnectionResult<()> {
        // Extract cache_rx from self.proxy_mode
        let ProxyMode::CacheRead(ref mut cache_rx) = self.proxy_mode else {
            return Err(ConnectionError::IoError(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected CacheRead mode",
            ))
            .into());
        };

        select! {
            res = origin_read.next() => {
                match res {
                    Some(Ok(msg)) => {
                        self.handle_origin_message(msg);
                    }
                    Some(Err(err)) => {
                        debug!("origin read error [{}]", err);
                        return Err(ConnectionError::ProtocolError(err).into());
                    }
                    None => {
                        debug!("origin stream closed");
                        return Err(ConnectionError::IoError(io::Error::new(
                            io::ErrorKind::ConnectionReset,
                            "origin disconnected",
                        )).into());
                    }
                }
            }
            reply = cache_rx.recv() => {
                match reply {
                    Some(reply) => {
                        self.handle_cache_reply(reply);
                    }
                    None => {
                        debug!("cache channel closed");
                        return Err(ConnectionError::CacheDead.into());
                    }
                }
            }
            _ = origin_write.writable(), if !self.origin_write_buf.is_empty() => {
                origin_write.write_buf(&mut self.origin_write_buf[0]).await
                    .map_err(ConnectionError::IoError)?;
                if !self.origin_write_buf[0].has_remaining() {
                    self.origin_write_buf.pop_front();
                }
            }
            _ = client_write.writable(), if !self.client_write_buf.is_empty() => {
                client_write.write_buf(&mut self.client_write_buf[0]).await
                    .map_err(ConnectionError::IoError)?;
                if !self.client_write_buf[0].has_remaining() {
                    self.client_write_buf.pop_front();
                }
            }
        };

        Ok(())
    }
}

/// Connect to the origin database server.
/// Tries each address in sequence until one succeeds.
/// If ssl_mode is Require, performs PostgreSQL SSL negotiation and TLS handshake.
async fn origin_connect(
    addrs: &[SocketAddr],
    ssl_mode: SslMode,
    server_name: &str,
) -> ConnectionResult<OriginStream> {
    for addr in addrs {
        if let Ok(stream) = TcpStream::connect(addr).await {
            return match ssl_mode {
                SslMode::Disable => Ok(TlsStream::plain(stream)),
                SslMode::Require => {
                    let tls_stream = tls::pg_tls_connect(stream, server_name)
                        .await
                        .map_err(|e| {
                            Report::from(ConnectionError::TlsError(io::Error::other(
                                e.into_current_context(),
                            )))
                        })
                        .attach_loc("establishing TLS connection")?;
                    Ok(origin_stream_from_tls(tls_stream))
                }
            };
        }
    }
    Err(ConnectionError::NoConnection.into())
}

#[instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn handle_connection(
    mut client_stream: ClientStream,
    addrs: Vec<SocketAddr>,
    ssl_mode: SslMode,
    server_name: &str,
    cache_sender: CacheSender,
) -> ConnectionResult<()> {
    // Track active connections - guard ensures decrement on any exit path
    metrics::gauge!(names::CONNECTIONS_ACTIVE).increment(1.0);
    let _connection_guard = ActiveConnectionGuard;

    // Create ClientSocketSource BEFORE splitting (captures raw fd and TLS state)
    let client_socket_source = client_stream.socket_source_create();

    // Connect to origin database (with TLS if required)
    let mut origin_stream = origin_connect(&addrs, ssl_mode, server_name)
        .await
        .attach_loc("connecting to origin")?;

    // Split origin stream (borrowed halves with .writable() support)
    let (origin_read, origin_write) = origin_stream.split();
    let origin_framed_read = FramedRead::new(origin_read, PgBackendMessageCodec::default());

    // Split client stream in place (borrowed halves with .writable() support)
    let (client_read, client_write) = client_stream.split();
    let client_framed_read = FramedRead::new(client_read, PgFrontendMessageCodec::default());

    // Initialize connection state with socket source
    let mut state = ConnectionState::new(client_socket_source);

    tokio::pin!(origin_framed_read);
    tokio::pin!(client_framed_read);
    tokio::pin!(origin_write);
    tokio::pin!(client_write);

    loop {
        match state.proxy_mode {
            ProxyMode::Read => {
                if let Err(err) = state
                    .connection_select(
                        &mut origin_framed_read,
                        &mut client_framed_read,
                        &mut origin_write,
                        &mut client_write,
                    )
                    .await
                {
                    debug!("read error [{}]", err);
                    break;
                }
            }
            ProxyMode::CacheRead(_) => {
                if let Err(err) = state
                    .connection_select_with_cache(
                        &mut origin_framed_read,
                        &mut origin_write,
                        &mut client_write,
                    )
                    .await
                {
                    debug!("read error [{}]", err);
                    // if matches!(err.current_context(), ConnectionError::CacheDead) {
                    //     state.proxy_status = ProxyStatus::Degraded;
                    // }
                    break;
                }
            }
            ProxyMode::CacheWrite(msg) => {
                // Resolve search_path for this connection (expand $user to session_user)
                // If search_path is unknown, forward to origin instead of caching
                let Some(resolved_search_path) = state.search_path.as_ref().map(|sp| {
                    sp.resolve(state.session_user.as_deref())
                        .into_iter()
                        .map(String::from)
                        .collect::<Vec<_>>()
                }) else {
                    debug!("search_path unknown, forwarding to origin");
                    metrics::counter!(names::QUERIES_UNCACHEABLE).increment(1);
                    state.origin_write_buf.push_back(msg.into_data());
                    state.proxy_mode = ProxyMode::Read;
                    continue;
                };

                metrics::counter!(names::QUERIES_CACHEABLE).increment(1);

                if matches!(msg, CacheMessage::QueryParameterized(_, _, _, _)) {
                    //send a flush to orgin
                    let data = BytesMut::from([b'H', 0, 0, 0, 4u8].as_ref());
                    state.origin_write_buf.push_back(data);
                }

                // Create ClientSocket for this query (dupes the fd)
                let client_socket = match state.client_socket_source.socket_create() {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Failed to create client socket: {}", e);
                        state.origin_write_buf.push_back(msg.into_data());
                        state.proxy_mode = ProxyMode::Read;
                        continue;
                    }
                };

                let (reply_tx, reply_rx) = channel(10);

                // Take timing from connection state and set dispatched_at
                let timing = {
                    let mut t = state.current_timing.take().unwrap_or_else(|| {
                        // Fallback if timing wasn't set (shouldn't happen)
                        QueryTiming::new(QueryId::new(0), Instant::now())
                    });
                    t.dispatched_at = Some(Instant::now());
                    t
                };

                let proxy_msg = ProxyMessage {
                    message: msg,
                    client_socket,
                    reply_tx,
                    search_path: resolved_search_path,
                    timing,
                };

                match cache_sender.send(proxy_msg).await {
                    Ok(()) => {
                        state.proxy_mode = ProxyMode::CacheRead(reply_rx);
                    }
                    Err(e) => {
                        // Cache is unavailable, fall back to proxying directly to origin
                        debug!("cache unavailable");
                        state.proxy_status = ProxyStatus::Degraded;
                        state
                            .origin_write_buf
                            .push_back(e.into_message().message.into_data());
                        state.proxy_mode = ProxyMode::Read;
                    }
                }
            }
        }
    }

    // Clean up prepared statements gauge before connection state is dropped
    let remaining_stmts = state.prepared_statements.len();
    if remaining_stmts > 0 {
        metrics::gauge!(names::PROTOCOL_PREPARED_STATEMENTS).decrement(remaining_stmts as f64);
    }

    match state.proxy_status {
        ProxyStatus::Degraded => Err(ConnectionError::CacheDead.into()),
        ProxyStatus::Normal => Ok(()),
    }
}

#[instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub fn connection_run(
    worker_id: usize,
    settings: &Settings,
    mut rx: UnboundedReceiver<TcpStream>,
    cache_sender: CacheSender,
    tls_acceptor: Option<Arc<tls::TlsAcceptor>>,
) -> ConnectionResult<()> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_into_report::<ConnectionError>()
        .attach_loc("creating connection runtime")?;

    // Extract TLS settings for the connection loop
    let ssl_mode = settings.origin.ssl_mode;
    let server_name = settings.origin.host.clone();

    debug!("handle connection start");
    rt.block_on(async {
        let addrs: Vec<SocketAddr> =
            lookup_host((settings.origin.host.as_str(), settings.origin.port))
                .await
                .map_into_report::<ConnectionError>()
                .attach_loc("resolving origin host")?
                .collect();

        LocalSet::new()
            .run_until(async {
                while let Some(socket) = rx.recv().await {
                    metrics::gauge!(names::PROXY_WORKER_QUEUE, "worker" => worker_id.to_string())
                        .set(rx.len() as f64);

                    let addrs = addrs.clone();
                    let server_name = server_name.clone();
                    let cache_sender = cache_sender.clone();
                    let tls_acceptor = tls_acceptor.clone();
                    spawn_local(async move {
                        debug!("task spawn");

                        // Negotiate client TLS if configured
                        let client_stream = match tls::client_tls_negotiate(
                            socket,
                            tls_acceptor.as_deref(),
                        )
                        .await
                        {
                            Ok(tls::ClientTlsResult::Tls {
                                tcp_stream,
                                tls_state,
                            }) => ClientStream::tls(tcp_stream, tls_state),
                            Ok(tls::ClientTlsResult::Plain(stream)) => ClientStream::plain(stream),
                            Err(e) => {
                                metrics::counter!(names::CONNECTIONS_ERRORS).increment(1);
                                error!("TLS negotiation failed: {}", e);
                                return Ok(());
                            }
                        };

                        let res = handle_connection(
                            client_stream,
                            addrs,
                            ssl_mode,
                            &server_name,
                            cache_sender,
                        )
                        .await;

                        if let Err(e) = res {
                            error!("{}", e);
                            metrics::counter!(names::CONNECTIONS_ERRORS).increment(1);
                            if matches!(e.current_context(), ConnectionError::CacheDead) {
                                debug!("connection closed in degraded mode");
                                return Err(io::Error::other("cache dead"));
                            }
                        }

                        debug!("task done");
                        Ok(())
                    });
                }

                Ok(())
            })
            .await
    })
}
