use std::{
    collections::{HashMap, VecDeque},
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    time::Instant,
};

use crate::catalog::FunctionVolatility;

use rootcause::Report;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpStream, lookup_host},
    runtime::Builder,
    select,
    sync::{mpsc::UnboundedReceiver, oneshot},
    task::{LocalSet, spawn_local},
};
use tokio_stream::StreamExt;
use tokio_util::{
    bytes::{Buf, BufMut, BytesMut},
    codec::FramedRead,
};
use tracing::{debug, error, instrument, trace};

use crate::{
    cache::{
        CacheMessage, CacheReply, ProxyMessage, QueryParameters,
        messages::{PipelineContext, PipelineDescribe},
        query::CacheableQuery,
    },
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
    query::ast::query_expr_fingerprint,
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

/// State machine for intercepting origin responses that shouldn't reach the client.
/// Only one intercept can be active at a time.
enum OriginIntercept {
    /// No intercept active — origin messages forwarded normally.
    None,
    /// Intercepting SHOW search_path response (pre-PG18 fallback).
    SearchPath,
    /// Intercepting proactive Parse response for a named statement.
    ProactiveParse { statement_name: String },
    /// ParseComplete (or error) handled; consuming final ReadyForQuery.
    AwaitingReadyForQuery,
}

/// Search path discovery state machine.
///
/// On connection startup, pgcache needs the session's search_path for table
/// resolution. PG 18+ sends it via ParameterStatus during authentication.
/// Older versions require an explicit SHOW query after the first ReadyForQuery.
enum SearchPathState {
    /// Waiting for the first ReadyForQuery to determine if PG sent search_path
    /// via ParameterStatus. If a ParameterStatus arrives with search_path before
    /// the first ReadyForQuery, transitions directly to Resolved.
    AwaitingFirstReady,

    /// search_path has been resolved (either from ParameterStatus or SHOW query)
    Resolved(SearchPath),
}

impl SearchPathState {
    /// Resolve the search_path if available, expanding $user to session_user.
    fn resolve(&self, session_user: Option<&str>) -> Option<Vec<String>> {
        match self {
            Self::AwaitingFirstReady => None,
            Self::Resolved(sp) => Some(
                sp.resolve(session_user)
                    .into_iter()
                    .map(String::from)
                    .collect(),
            ),
        }
    }
}

/// Timing instrumentation for the current query in flight.
/// Tracks timestamps for metrics recording across the origin and cache paths.
struct QueryTelemetry {
    /// When the client message arrived — measures end-to-end latency for both
    /// cache hits (CACHE_QUERY_LATENCY) and origin queries (ORIGIN_QUERY_LATENCY)
    client_received_at: Option<Instant>,

    /// When the query was forwarded to origin — measures origin-only execution
    /// time (ORIGIN_EXECUTION), excluding parse and cacheability-check overhead
    origin_sent_at: Option<Instant>,

    /// Per-stage timing breakdown that travels with the query through the cache
    /// pipeline (coordinator → worker) and back, only set for cache-path queries
    cache_timing: Option<QueryTiming>,
}

impl QueryTelemetry {
    fn new() -> Self {
        Self {
            client_received_at: None,
            origin_sent_at: None,
            cache_timing: None,
        }
    }

    /// Record that a client message was received.
    fn query_receive(&mut self) {
        self.client_received_at = Some(Instant::now());
    }

    /// Record that the query was forwarded to origin.
    fn origin_forward(&mut self) {
        self.origin_sent_at = Some(Instant::now());
    }

    /// Create cache timing for a cacheable query.
    fn cache_timing_start(&mut self, fingerprint: u64) {
        let query_id = QueryId::new(fingerprint);
        let received_at = self.client_received_at.unwrap_or_else(Instant::now);
        let mut timing = QueryTiming::new(query_id, received_at);
        timing.parsed_at = Some(Instant::now());
        self.cache_timing = Some(timing);
    }

    /// Record origin query completion. Takes both timestamps and records
    /// ORIGIN_EXECUTION_SECONDS and ORIGIN_QUERY_LATENCY_SECONDS.
    fn origin_complete(&mut self) {
        if let Some(start) = self.origin_sent_at.take() {
            metrics::histogram!(names::ORIGIN_EXECUTION_SECONDS)
                .record(start.elapsed().as_secs_f64());
        }
        if let Some(start) = self.client_received_at.take() {
            metrics::histogram!(names::ORIGIN_QUERY_LATENCY_SECONDS)
                .record(start.elapsed().as_secs_f64());
        }
    }

    /// Record cache query completion. Records CACHE_QUERY_LATENCY_SECONDS
    /// and per-stage timing breakdown.
    fn cache_complete(&mut self, reply_timing: Option<QueryTiming>) {
        if let Some(start) = self.client_received_at.take() {
            metrics::histogram!(names::CACHE_QUERY_LATENCY_SECONDS)
                .record(start.elapsed().as_secs_f64());
        }
        if let Some(timing) = reply_timing {
            timing_record(&timing);
        }
    }

    /// Take the cache timing for dispatch to the cache pipeline.
    /// Sets dispatched_at before returning.
    fn cache_timing_dispatch(&mut self) -> QueryTiming {
        let mut t = self
            .cache_timing
            .take()
            .unwrap_or_else(|| QueryTiming::new(QueryId::new(0), Instant::now()));
        t.dispatched_at = Some(Instant::now());
        t
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

    /// PostgreSQL session user from startup message
    /// TODO: Track SET ROLE queries to update effective user for permission checks
    session_user: Option<String>,

    /// Intercepts origin responses that shouldn't reach the client (e.g., SHOW
    /// search_path or proactive Parse+Sync). Only one intercept active at a time.
    origin_intercept: OriginIntercept,

    /// Search path discovery state
    search_path_state: SearchPathState,

    /// Query timing instrumentation
    telemetry: QueryTelemetry,

    /// Function volatility map for cacheability checks
    func_volatility: Arc<HashMap<String, FunctionVolatility>>,

    /// Extended query protocol pipeline state
    extended: ExtendedPending,
}

/// Buffered extended protocol messages, accumulated until Sync/Flush.
/// All decision-making (cache vs. forward) is deferred to Sync time.
struct ExtendedBuffer {
    /// Concatenated raw bytes of all buffered messages
    bytes: BytesMut,
    /// Whether a Parse was buffered
    has_parse: bool,
    /// Whether a Bind was buffered
    has_bind: bool,
    /// Whether/what Describe was buffered
    describe: PipelineDescribe,
    /// Portal name from Execute (None if no Execute yet)
    execute_portal: Option<String>,
    /// True if more than one Execute was buffered (forces forward-to-origin)
    multiple_executes: bool,
    /// Statement name from Parse (for pending_parse_statement on forward)
    parse_statement_name: Option<String>,
    /// Statement name from Describe('S') (for pending_describe_statement on forward)
    describe_statement_name: Option<String>,
}

impl Default for ExtendedBuffer {
    fn default() -> Self {
        Self {
            bytes: BytesMut::new(),
            has_parse: false,
            has_bind: false,
            describe: PipelineDescribe::None,
            execute_portal: None,
            multiple_executes: false,
            parse_statement_name: None,
            describe_statement_name: None,
        }
    }
}

/// State for the extended query protocol pipeline.
/// Accumulates messages until Sync/Flush, then tracks pending origin responses
/// and pipeline context for cache dispatch.
struct ExtendedPending {
    /// Statement name whose ParseComplete we're waiting for from origin.
    /// Set when Parse is buffered in the pipeline; consumed when origin responds.
    pending_parse_statement: Option<String>,

    /// Name of statement most recently described (awaiting ParameterDescription)
    pending_describe_statement: Option<String>,

    /// Buffered extended protocol messages accumulated until Sync/Flush.
    /// Decision-making deferred to Sync time.
    buffer: Option<ExtendedBuffer>,

    /// Pipeline context ready for cache dispatch.
    /// Built at Sync time from ExtendedBuffer, consumed by ProxyMessage.
    pipeline_context: Option<PipelineContext>,
}

impl ExtendedPending {
    fn new() -> Self {
        Self {
            pending_parse_statement: None,
            pending_describe_statement: None,
            buffer: None,
            pipeline_context: None,
        }
    }

    /// Get or create the ExtendedBuffer for accumulating messages.
    fn buffer_get_or_create(&mut self) -> &mut ExtendedBuffer {
        self.buffer.get_or_insert_with(ExtendedBuffer::default)
    }

    /// Take the buffer contents. Returns None if no buffer was active.
    fn buffer_take(&mut self) -> Option<ExtendedBuffer> {
        self.buffer.take()
    }

    /// Flush any buffered extended protocol messages.
    /// Extracts pending statement names from buffer metadata.
    /// Returns the buffer's bytes for the caller to push to origin.
    fn buffer_flush(&mut self) -> Option<BytesMut> {
        let buffer = self.buffer.take()?;
        if buffer.has_parse {
            self.pending_parse_statement = buffer.parse_statement_name;
        }
        if buffer.describe_statement_name.is_some() {
            self.pending_describe_statement = buffer.describe_statement_name;
        }
        Some(buffer.bytes)
    }

    /// Forward buffer to origin with trailing bytes (Sync or Flush).
    /// Extracts pending statement names from buffer metadata.
    /// Returns bytes to push to origin.
    fn buffer_forward(&mut self, mut buffer: ExtendedBuffer, trailing_bytes: &[u8]) -> BytesMut {
        if buffer.has_parse {
            self.pending_parse_statement = buffer.parse_statement_name;
        }
        if buffer.describe_statement_name.is_some() {
            self.pending_describe_statement = buffer.describe_statement_name;
        }
        buffer.bytes.extend_from_slice(trailing_bytes);
        buffer.bytes
    }

    /// Handle ParseComplete from origin: mark statement as origin_prepared.
    fn parse_complete(&mut self, prepared_statements: &mut HashMap<String, PreparedStatement>) {
        if let Some(stmt_name) = self.pending_parse_statement.take()
            && let Some(stmt) = prepared_statements.get_mut(&stmt_name)
        {
            stmt.origin_prepared = true;
            trace!("origin_prepared set for statement '{}'", stmt_name);
        }
    }

    /// Handle ParameterDescription from origin: update pending statement's parameter OIDs.
    fn parameter_description_received(
        &mut self,
        msg_data: &BytesMut,
        prepared_statements: &mut HashMap<String, PreparedStatement>,
    ) {
        if let Some(stmt_name) = self.pending_describe_statement.take()
            && let Ok(parsed) = parse_parameter_description(msg_data)
            && let Some(stmt) = prepared_statements.get_mut(&stmt_name)
        {
            debug!(
                "updated statement '{}' with parameter OIDs {:?}",
                stmt_name, parsed.parameter_oids
            );
            stmt.parameter_oids = parsed.parameter_oids;
            stmt.parameter_description = Some(msg_data.clone());
        }
    }

    /// Take pipeline context (for origin fallback or cache dispatch).
    fn pipeline_take(&mut self) -> Option<PipelineContext> {
        self.pipeline_context.take()
    }
}

impl ConnectionState {
    fn new(
        client_socket_source: ClientSocketSource,
        func_volatility: Arc<HashMap<String, FunctionVolatility>>,
    ) -> Self {
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
            session_user: None,
            origin_intercept: OriginIntercept::None,
            search_path_state: SearchPathState::AwaitingFirstReady,
            telemetry: QueryTelemetry::new(),
            func_volatility,
            extended: ExtendedPending::new(),
        }
    }

}

impl ConnectionState {
    /// Handle a message from the client (frontend).
    /// Determines whether to forward to origin, check cache, or take other action.
    #[expect(clippy::wildcard_enum_match_arm)]
    async fn handle_client_message(&mut self, msg: PgFrontendMessage) {
        trace!("net: client→proxy {:?}", msg.message_type);
        match msg.message_type {
            PgFrontendMessageType::Query => {
                metrics::counter!(names::QUERIES_TOTAL).increment(1);
                metrics::counter!(names::PROTOCOL_SIMPLE_QUERIES).increment(1);
                self.telemetry.query_receive();

                if !self.in_transaction {
                    self.proxy_mode = match handle_query(
                        &msg.data,
                        &mut self.fingerprint_cache,
                        &self.func_volatility,
                    )
                    .await
                    {
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
                            self.telemetry.origin_forward();
                            self.origin_write_buf.push_back(msg.data);
                            ProxyMode::Read
                        }
                        Ok(Action::CacheCheck(ast)) => {
                            let fingerprint = query_expr_fingerprint(&ast.query);
                            self.telemetry.cache_timing_start(fingerprint);
                            ProxyMode::CacheWrite(CacheMessage::Query(msg.data, ast))
                        }
                        Err(e) => {
                            metrics::counter!(names::QUERIES_UNCACHEABLE).increment(1);
                            metrics::counter!(names::QUERIES_INVALID).increment(1);
                            error!("handle_query {}", e);
                            self.telemetry.origin_forward();
                            self.origin_write_buf.push_back(msg.data);
                            ProxyMode::Read
                        }
                    };
                } else {
                    metrics::counter!(names::QUERIES_UNCACHEABLE).increment(1);
                    self.telemetry.origin_forward();
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
                self.handle_flush_message(msg);
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

    /// Handle an origin message during an active intercept.
    /// Returns true if the message was consumed (caller should not forward).
    #[expect(clippy::wildcard_enum_match_arm)]
    fn origin_intercept_handle(&mut self, msg: &PgBackendMessage) -> bool {
        match &self.origin_intercept {
            OriginIntercept::None => return false,

            OriginIntercept::SearchPath => match msg.message_type {
                PgBackendMessageType::DataRows => {
                    if let Some(value) = data_row_first_column(&msg.data) {
                        debug!("received search_path from SHOW query: {}", value);
                        self.search_path_state =
                            SearchPathState::Resolved(SearchPath::parse(value));
                    }
                }
                PgBackendMessageType::ReadyForQuery => {
                    debug!("search_path query complete");
                    self.origin_intercept = OriginIntercept::None;
                }
                _ => {}
            },

            OriginIntercept::ProactiveParse { statement_name } => {
                let stmt_name = statement_name.clone();
                match msg.message_type {
                    PgBackendMessageType::ParseComplete => {
                        if let Some(stmt) = self.prepared_statements.get_mut(&stmt_name) {
                            stmt.origin_prepared = true;
                            trace!("origin_prepared set for '{}' (proactive)", stmt_name);
                        }
                        self.origin_intercept = OriginIntercept::AwaitingReadyForQuery;
                    }
                    PgBackendMessageType::ErrorResponse => {
                        error!("proactive Parse failed on origin");
                        self.origin_intercept = OriginIntercept::AwaitingReadyForQuery;
                    }
                    _ => {}
                }
            }

            OriginIntercept::AwaitingReadyForQuery => {
                if matches!(msg.message_type, PgBackendMessageType::ReadyForQuery) {
                    self.origin_intercept = OriginIntercept::None;
                }
            }
        }
        true
    }

    /// Handle a message from the origin database (backend).
    /// Updates transaction state, captures parameter OIDs, and forwards to client.
    #[expect(clippy::wildcard_enum_match_arm)]
    fn handle_origin_message(&mut self, mut msg: PgBackendMessage) {
        trace!("net: origin→proxy {:?}", msg.message_type);

        if self.origin_intercept_handle(&msg) {
            return;
        }

        match msg.message_type {
            PgBackendMessageType::ParameterStatus => {
                // Check for search_path parameter (PG 18+ sends this during startup)
                if let Some(("search_path", value)) = parameter_status_parse(&msg.data) {
                    debug!("received search_path from ParameterStatus: {}", value);
                    self.search_path_state = SearchPathState::Resolved(SearchPath::parse(value));
                }
            }
            PgBackendMessageType::ParameterDescription => {
                self.extended
                    .parameter_description_received(&msg.data, &mut self.prepared_statements);
            }
            PgBackendMessageType::ParseComplete => {
                self.extended.parse_complete(&mut self.prepared_statements);
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

                self.telemetry.origin_complete();

                // Clean up unnamed portals when transaction ends
                if !self.in_transaction {
                    self.portals.retain(|name, _| !name.is_empty());
                }

                // If search_path hasn't been resolved yet (PG < 18 doesn't send
                // ParameterStatus for search_path), send SHOW query to discover it.
                if let SearchPathState::AwaitingFirstReady = self.search_path_state
                    && matches!(self.origin_intercept, OriginIntercept::None)
                {
                    debug!("search_path not received, sending SHOW search_path query");
                    self.origin_intercept = OriginIntercept::SearchPath;
                    let query_msg = simple_query_message_build("SHOW search_path;");
                    self.origin_write_buf.push_back(query_msg);
                }
            }
            _ => {}
        }

        trace!(
            "net: origin→client {:?} ({} bytes)",
            msg.message_type,
            msg.data.len()
        );
        self.client_write_buf.push_back(msg.data);
    }

    /// Flush any buffered extended protocol messages to origin.
    fn extended_buffer_flush_to_origin(&mut self) {
        if let Some(bytes) = self.extended.buffer_flush() {
            self.origin_write_buf.push_back(bytes);
        }
    }

    /// Forward an extended buffer to origin, appending the trailing message bytes (Sync or Flush).
    /// Records metrics for any Execute in the buffer.
    fn extended_buffer_forward_to_origin(&mut self, buffer: ExtendedBuffer, trailing_bytes: &[u8]) {
        // Record non-cacheable metrics for Execute(s) in the buffer
        if buffer.execute_portal.is_some() {
            metrics::counter!(names::QUERIES_UNCACHEABLE).increment(1);

            if let Some(portal_name) = &buffer.execute_portal
                && let Some(portal) = self.portals.get(portal_name)
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
        }

        let bytes = self.extended.buffer_forward(buffer, trailing_bytes);
        self.telemetry.origin_forward();
        self.origin_write_buf.push_back(bytes);
    }

    /// Handle a reply from the cache.
    /// If cache indicates error or needs forwarding, send query to origin instead.
    fn handle_cache_reply(&mut self, reply: CacheReply) {
        trace!(
            "net: cache→proxy reply={}",
            match &reply {
                CacheReply::Complete(_) => "Complete",
                CacheReply::Forward(_) => "Forward",
                CacheReply::Error(_) => "Error",
            }
        );
        match reply {
            CacheReply::Complete(timing) => {
                metrics::counter!(names::QUERIES_CACHE_HIT).increment(1);
                self.telemetry.cache_complete(timing);

                // Proactively forward Parse to origin for named statements not yet
                // origin_prepared. On cache hit, origin never sees Parse — but origin
                // must know about the statement for subsequent Bind-only cycles and
                // transaction paths.
                //
                // Skip unnamed statements: always re-Parsed, can't be reused via
                // Bind-only across Sync boundaries.
                // Skip if another intercept is already active to avoid stacking
                // multiple origin round-trips whose responses would leak to the client.
                let proactive_parse_bytes = self
                    .extended
                    .pending_parse_statement
                    .as_ref()
                    .and_then(|stmt_name| {
                        if stmt_name.is_empty() {
                            return None;
                        }
                        if !matches!(self.origin_intercept, OriginIntercept::None) {
                            return None;
                        }
                        let stmt = self.prepared_statements.get(stmt_name)?;
                        if stmt.origin_prepared {
                            return None;
                        }
                        stmt.parse_bytes.clone()
                    });

                if let Some(parse_bytes) = proactive_parse_bytes {
                    let stmt_name = self
                        .extended
                        .pending_parse_statement
                        .take()
                        .unwrap_or_default();
                    trace!(
                        "proactive Parse sent to origin for statement '{}'",
                        stmt_name
                    );
                    self.origin_write_buf.push_back(parse_bytes);
                    let mut sync_buf = BytesMut::with_capacity(5);
                    sync_buf.put_u8(b'S');
                    sync_buf.put_i32(4);
                    self.origin_write_buf.push_back(sync_buf);
                    self.origin_intercept = OriginIntercept::ProactiveParse {
                        statement_name: stmt_name,
                    };
                } else {
                    self.extended.pending_parse_statement.take();
                }

                self.proxy_mode = ProxyMode::Read;
            }
            CacheReply::Error(buf) => {
                metrics::counter!(names::QUERIES_CACHE_ERROR).increment(1);
                debug!("forwarding to origin");
                self.telemetry.origin_forward();
                self.origin_write_buf.push_back(buf);
                self.proxy_mode = ProxyMode::Read;
            }
            CacheReply::Forward(buf) => {
                metrics::counter!(names::QUERIES_CACHE_MISS).increment(1);
                debug!("forwarding to origin");
                self.telemetry.origin_forward();
                self.origin_write_buf.push_back(buf);
                self.proxy_mode = ProxyMode::Read;
            }
        }
    }

    /// Handle Parse message — analyze cacheability, store statement, buffer bytes.
    fn handle_parse_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_parse_message(&msg.data) {
            let sql_type = match pg_query::parse(&parsed.sql) {
                Ok(ast) => match crate::query::ast::query_expr_convert(&ast) {
                    Ok(query) => match CacheableQuery::try_new(&query, &self.func_volatility) {
                        Ok(cacheable_query) => StatementType::Cacheable(Box::new(cacheable_query)),
                        Err(_) => StatementType::UncacheableSelect,
                    },
                    Err(_) => StatementType::NonSelect,
                },
                Err(_) => StatementType::ParseError,
            };

            let parse_bytes = msg.data.clone();
            let statement_name = parsed.statement_name.clone();
            self.statement_store(parsed, sql_type, parse_bytes);

            let buffer = self.extended.buffer_get_or_create();
            buffer.has_parse = true;
            buffer.parse_statement_name = Some(statement_name);
            buffer.bytes.extend_from_slice(&msg.data);
            trace!("net: Parse buffered");
            return;
        }
        self.origin_write_buf.push_back(msg.data);
    }

    /// Handle Bind message — store portal, buffer bytes.
    fn handle_bind_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_bind_message(&msg.data) {
            self.portal_store(parsed);

            let buffer = self.extended.buffer_get_or_create();
            buffer.has_bind = true;
            buffer.bytes.extend_from_slice(&msg.data);
            trace!("net: Bind buffered");
            return;
        }
        self.origin_write_buf.push_back(msg.data);
    }

    /// Handle Execute message — record metrics, parse portal name, buffer bytes.
    /// Decision-making deferred to Sync.
    fn handle_execute_message(&mut self, msg: PgFrontendMessage) {
        metrics::counter!(names::QUERIES_TOTAL).increment(1);
        metrics::counter!(names::PROTOCOL_EXTENDED_QUERIES).increment(1);
        self.telemetry.query_receive();

        let portal_name = parse_execute_message(&msg.data).ok().map(|p| p.portal_name);

        let buffer = self.extended.buffer_get_or_create();

        if buffer.execute_portal.is_some() {
            buffer.multiple_executes = true;
        } else {
            buffer.execute_portal = portal_name;
        }

        buffer.bytes.extend_from_slice(&msg.data);
        trace!("net: Execute buffered");
    }

    /// Attempt to create a cache message from the extended buffer at Sync time.
    /// Returns None if caching is not possible.
    fn buffer_try_cache(&self, buffer: &ExtendedBuffer) -> Option<CacheMessage> {
        if self.in_transaction {
            return None;
        }
        if self.proxy_status != ProxyStatus::Normal {
            return None;
        }

        let portal_name = buffer.execute_portal.as_ref()?;
        let portal = self.portals.get(portal_name)?;

        // Only handle implicit or uniform result formats
        if portal.result_formats.len() > 1
            && !portal
                .result_formats
                .windows(2)
                .all(|w| matches!(w, [a, b] if a == b))
        {
            trace!("result format is not implicit or uniform");
            return None;
        }

        let stmt = self.prepared_statements.get(&portal.statement_name)?;

        let cacheable_query = match &stmt.sql_type {
            StatementType::Cacheable(query) => query.clone(),
            StatementType::NonSelect
            | StatementType::UncacheableSelect
            | StatementType::ParseError => return None,
        };

        // Bind-only (no Parse in buffer): require origin_prepared
        if !buffer.has_parse && !stmt.origin_prepared {
            return None;
        }

        // Describe('S') in buffer: require cached parameter_description
        if buffer.describe == PipelineDescribe::Statement && stmt.parameter_description.is_none() {
            return None;
        }

        Some(CacheMessage::QueryParameterized(
            // Execute bytes are already in buffer.bytes; pass empty data since
            // the worker uses pipeline context's buffered_bytes for extended queries
            BytesMut::new(),
            cacheable_query,
            QueryParameters {
                values: portal.parameter_values.clone(),
                formats: portal.parameter_formats.clone(),
                oids: stmt.parameter_oids.clone(),
            },
            portal.result_formats.clone(),
        ))
    }

    /// Handle Describe message — buffer bytes and track describe metadata.
    fn handle_describe_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_describe_message(&msg.data) {
            let buffer = self.extended.buffer_get_or_create();

            match parsed.describe_type {
                b'S' => {
                    buffer.describe = PipelineDescribe::Statement;
                    buffer.describe_statement_name = Some(parsed.name);
                }
                b'P' => {
                    buffer.describe = PipelineDescribe::Portal;
                }
                _ => {}
            }

            buffer.bytes.extend_from_slice(&msg.data);
            trace!("net: Describe buffered");
            return;
        }
        self.origin_write_buf.push_back(msg.data);
    }

    /// Handle Close message — flush buffer to origin, clean up state, forward Close.
    fn handle_close_message(&mut self, msg: PgFrontendMessage) {
        self.extended_buffer_flush_to_origin();
        if let Ok(parsed) = parse_close_message(&msg.data) {
            match parsed.close_type {
                b'S' => self.statement_close(&parsed.name),
                b'P' => self.portal_close(&parsed.name),
                _ => {}
            }
        }
        self.origin_write_buf.push_back(msg.data);
    }

    /// Handle Sync message — all cache vs. forward decision-making happens here.
    ///
    /// If the buffer contains exactly one cacheable Execute, dispatch to cache.
    /// Otherwise, forward the whole batch to origin.
    fn handle_sync_message(&mut self, msg: PgFrontendMessage) {
        let Some(mut buffer) = self.extended.buffer_take() else {
            // No buffer — forward bare Sync to origin
            trace!("net: proxy→origin Sync (no buffer)");
            self.origin_write_buf.push_back(msg.data);
            return;
        };

        // Try cache path: single Execute that is cacheable
        if !buffer.multiple_executes
            && buffer.execute_portal.is_some()
            && let Some(cache_msg) = self.buffer_try_cache(&buffer)
        {
            // Build PipelineContext from buffer for the cache/forward path
            let parameter_description = if buffer.describe == PipelineDescribe::Statement {
                buffer
                    .execute_portal
                    .as_ref()
                    .and_then(|p| self.portals.get(p))
                    .and_then(|portal| self.prepared_statements.get(&portal.statement_name))
                    .and_then(|stmt| stmt.parameter_description.clone())
            } else {
                None
            };

            // Append Sync bytes to buffer
            buffer.bytes.extend_from_slice(&msg.data);

            self.extended.pipeline_context = Some(PipelineContext {
                buffered_bytes: buffer.bytes,
                describe: buffer.describe,
                parameter_description,
                has_parse: buffer.has_parse,
                has_bind: buffer.has_bind,
            });

            // Track pending statement names for origin fallback path
            if buffer.has_parse {
                self.extended.pending_parse_statement = buffer.parse_statement_name;
            }
            if buffer.describe_statement_name.is_some() {
                self.extended.pending_describe_statement = buffer.describe_statement_name;
            }

            // Create timing with fingerprint from the cacheable query
            let fingerprint = match &cache_msg {
                CacheMessage::Query(_, ast) | CacheMessage::QueryParameterized(_, ast, _, _) => {
                    query_expr_fingerprint(&ast.query)
                }
            };
            self.telemetry.cache_timing_start(fingerprint);

            trace!("net: Sync → cache dispatch");
            self.proxy_mode = ProxyMode::CacheWrite(cache_msg);
        } else {
            self.extended_buffer_forward_to_origin(buffer, &msg.data);
            trace!("net: Sync → origin (forwarded buffer)");
        }
    }

    /// Handle Flush message — forward buffer to origin, no cache attempt.
    /// Handles JDBC pattern: Parse/Bind/Describe/Flush then Execute/Sync.
    fn handle_flush_message(&mut self, msg: PgFrontendMessage) {
        let Some(buffer) = self.extended.buffer_take() else {
            self.origin_write_buf.push_back(msg.data);
            return;
        };

        self.extended_buffer_forward_to_origin(buffer, &msg.data);
        trace!("net: Flush → origin (forwarded buffer)");
    }

    /// Store a prepared statement in connection state.
    ///
    /// For unnamed statements (empty name), always overwrite — the protocol allows reuse of
    /// the unnamed slot with a new Parse. For named statements, `or_insert` preserves existing
    /// metadata (parameter_description, origin_prepared) accumulated during the cold path.
    fn statement_store(
        &mut self,
        parsed: ParsedParseMessage,
        sql_type: StatementType,
        parse_bytes: BytesMut,
    ) {
        let stmt = PreparedStatement {
            name: parsed.statement_name.clone(),
            sql: parsed.sql,
            parameter_oids: parsed.parameter_oids,
            sql_type,
            parameter_description: None,
            origin_prepared: false,
            parse_bytes: Some(parse_bytes),
        };
        debug!("parsed statement insert {}", parsed.statement_name);

        if parsed.statement_name.is_empty() {
            // Unnamed statement: always overwrite per protocol spec
            self.prepared_statements.insert(parsed.statement_name, stmt);
        } else {
            // Named statement: preserve existing metadata from first cold path
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
            reply = &mut *cache_rx => {
                match reply {
                    Ok(reply) => {
                        self.handle_cache_reply(reply);
                    }
                    Err(_) => {
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

/// Forward a query to origin when cache dispatch isn't possible.
///
/// Sends either the buffered pipeline bytes or the raw message data to origin.
/// Free function (not a method) to allow disjoint field borrows when `proxy_mode`
/// has been partially moved by pattern matching.
fn origin_forward(
    extended: &mut ExtendedPending,
    origin_write_buf: &mut VecDeque<BytesMut>,
    msg: CacheMessage,
) {
    if let Some(pipeline) = extended.pipeline_take() {
        origin_write_buf.push_back(pipeline.buffered_bytes);
    } else {
        origin_write_buf.push_back(msg.into_data());
    }
}

#[instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn handle_connection(
    mut client_stream: ClientStream,
    addrs: Vec<SocketAddr>,
    ssl_mode: SslMode,
    server_name: &str,
    cache_sender: CacheSender,
    func_volatility: Arc<HashMap<String, FunctionVolatility>>,
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
    let mut state = ConnectionState::new(client_socket_source, func_volatility);

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
                let Some(resolved_search_path) = state
                    .search_path_state
                    .resolve(state.session_user.as_deref())
                else {
                    debug!("search_path unknown, forwarding to origin");
                    metrics::counter!(names::QUERIES_UNCACHEABLE).increment(1);
                    origin_forward(&mut state.extended, &mut state.origin_write_buf, msg);
                    state.proxy_mode = ProxyMode::Read;
                    continue;
                };

                metrics::counter!(names::QUERIES_CACHEABLE).increment(1);

                // Create ClientSocket for this query (dupes the fd)
                let client_socket = match state.client_socket_source.socket_create() {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Failed to create client socket: {}", e);
                        origin_forward(&mut state.extended, &mut state.origin_write_buf, msg);
                        state.proxy_mode = ProxyMode::Read;
                        continue;
                    }
                };

                let (reply_tx, reply_rx) = oneshot::channel();

                let timing = state.telemetry.cache_timing_dispatch();

                let proxy_msg = ProxyMessage {
                    message: msg,
                    client_socket,
                    reply_tx,
                    search_path: resolved_search_path,
                    timing,
                    pipeline: state.extended.pipeline_take(),
                };

                match cache_sender.send(proxy_msg).await {
                    Ok(()) => {
                        state.proxy_mode = ProxyMode::CacheRead(reply_rx);
                    }
                    Err(e) => {
                        // Cache is unavailable, fall back to proxying directly to origin.
                        debug!("cache unavailable");
                        state.proxy_status = ProxyStatus::Degraded;
                        let proxy_msg = e.into_message();
                        if let Some(pipeline) = proxy_msg.pipeline {
                            state.origin_write_buf.push_back(pipeline.buffered_bytes);
                        } else {
                            state
                                .origin_write_buf
                                .push_back(proxy_msg.message.into_data());
                        }
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
    func_volatility: Arc<HashMap<String, FunctionVolatility>>,
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
                    let func_volatility = Arc::clone(&func_volatility);
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
                            func_volatility,
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
