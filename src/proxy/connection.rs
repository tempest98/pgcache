use std::{
    collections::{HashMap, VecDeque},
    io::{self, Error},
    net::SocketAddr,
    os::fd::{FromRawFd, IntoRawFd, OwnedFd},
    pin::Pin,
    sync::Arc,
};

use nix::unistd::dup;
use tokio::{
    io::AsyncWriteExt,
    net::{
        TcpStream, lookup_host,
        tcp::{ReadHalf, WriteHalf},
    },
    runtime::Builder,
    select,
    sync::mpsc::{Sender, UnboundedReceiver, channel},
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
    metrics::Metrics,
    pg::protocol::{
        backend::{PgBackendMessage, PgBackendMessageCodec, PgBackendMessageType},
        extended::{
            ParsedBindMessage, ParsedParseMessage, Portal, PreparedStatement, StatementType,
            parse_bind_message, parse_close_message, parse_describe_message, parse_execute_message,
            parse_parameter_description, parse_parse_message,
        },
        frontend::{
            PgFrontendMessage, PgFrontendMessageCodec, PgFrontendMessageType,
            startup_message_parameter,
        },
    },
    settings::Settings,
};

use super::query::{Action, ForwardReason, handle_query};
use super::{ConnectionError, ProxyMode, ProxyStatus, ReadError};

type SenderCacheType = Sender<ProxyMessage>;

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

    /// Duplicated client socket file descriptor for cache worker
    client_fd_dup: OwnedFd,

    /// Extended protocol: prepared statements by name
    prepared_statements: HashMap<String, PreparedStatement>,

    /// Extended protocol: portals (bound statements) by name
    portals: HashMap<String, Portal>,

    /// Extended protocol: name of statement most recently described (awaiting ParameterDescription)
    pending_describe_statement: Option<String>,

    /// Metrics collector for tracking query and cache performance
    metrics: Arc<Metrics>,

    /// PostgreSQL session user from startup message
    /// TODO: Track SET ROLE queries to update effective user for permission checks
    session_user: Option<String>,
}

impl ConnectionState {
    fn new(client_fd_dup: OwnedFd, metrics: Arc<Metrics>) -> Self {
        Self {
            origin_write_buf: VecDeque::new(),
            client_write_buf: VecDeque::new(),
            fingerprint_cache: HashMap::new(),
            in_transaction: false,
            proxy_mode: ProxyMode::Read,
            proxy_status: ProxyStatus::Normal,
            client_fd_dup,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            pending_describe_statement: None,
            metrics,
            session_user: None,
        }
    }

    /// Handle a message from the client (frontend).
    /// Determines whether to forward to origin, check cache, or take other action.
    #[expect(clippy::wildcard_enum_match_arm)]
    async fn handle_client_message(&mut self, msg: PgFrontendMessage) {
        debug!("client {:?}", &msg);
        match msg.message_type {
            PgFrontendMessageType::Query => {
                self.metrics.query_increment();

                if !self.in_transaction {
                    self.proxy_mode =
                        match handle_query(&msg.data, &mut self.fingerprint_cache).await {
                            Ok(Action::Forward(reason)) => {
                                match reason {
                                    ForwardReason::UnsupportedStatement => {
                                        self.metrics.unsupported_increment()
                                    }
                                    ForwardReason::UncacheableSelect => {
                                        self.metrics.uncacheable_increment()
                                    }
                                    ForwardReason::Invalid => self.metrics.invalid_increment(),
                                }
                                self.origin_write_buf.push_back(msg.data);
                                ProxyMode::Read
                            }
                            Ok(Action::CacheCheck(ast)) => {
                                ProxyMode::CacheWrite(CacheMessage::Query(msg.data, ast))
                            }
                            Err(e) => {
                                self.metrics.uncacheable_increment();
                                self.metrics.invalid_increment();
                                error!("handle_query {}", e);
                                self.origin_write_buf.push_back(msg.data);
                                ProxyMode::Read
                            }
                        };
                } else {
                    self.metrics.uncacheable_increment();
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
            _ => {
                // All other message types - forward to origin
                self.origin_write_buf.push_back(msg.data);
            }
        }
    }

    /// Handle a message from the origin database (backend).
    /// Updates transaction state, captures parameter OIDs, and forwards to client.
    #[expect(clippy::wildcard_enum_match_arm)]
    fn handle_origin_message(&mut self, msg: PgBackendMessage) {
        match msg.message_type {
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
            PgBackendMessageType::ReadyForQuery => {
                // ReadyForQuery message contains transaction status at byte 5
                // 'I' = idle (not in transaction)
                // 'T' = in transaction block
                // 'E' = in failed transaction block
                self.in_transaction = msg.data.get(5).is_some_and(|&b| b == b'T' || b == b'E');

                // Clean up unnamed portals when transaction ends
                if !self.in_transaction {
                    self.portals.retain(|name, _| !name.is_empty());
                }
            }
            _ => {}
        }

        debug!("{:?}", &msg.data);
        self.client_write_buf.push_back(msg.data);
    }

    /// Handle a reply from the cache.
    /// If cache indicates error or needs forwarding, send query to origin instead.
    fn handle_cache_reply(&mut self, reply: CacheReply) {
        match reply {
            CacheReply::Complete(_) => {
                self.metrics.cache_hit_increment();
                self.proxy_mode = ProxyMode::Read;
            }
            CacheReply::Error(buf) => {
                self.metrics.cache_error_increment();
                debug!("forwarding to origin");
                self.origin_write_buf.push_back(buf);
                self.proxy_mode = ProxyMode::Read;
            }
            CacheReply::Forward(buf) => {
                self.metrics.cache_miss_increment();
                debug!("forwarding to origin");
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
                Ok(ast) => match crate::query::ast::sql_query_convert(&ast) {
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
        self.metrics.query_increment();

        self.proxy_mode = match self.try_cache_execute(&msg) {
            Some(cache_msg) => ProxyMode::CacheWrite(cache_msg),
            None => {
                self.metrics.uncacheable_increment();

                // Track statement type for metrics
                if let Ok(parsed) = parse_execute_message(&msg.data)
                    && let Some(portal) = self.portals.get(&parsed.portal_name)
                    && let Some(stmt) = self.prepared_statements.get(&portal.statement_name)
                {
                    match &stmt.sql_type {
                        StatementType::NonSelect => self.metrics.unsupported_increment(),
                        StatementType::ParseError => self.metrics.invalid_increment(),
                        StatementType::Cacheable(_) | StatementType::UncacheableSelect => {}
                    }
                }

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
        self.prepared_statements.remove(name);
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
    async fn connection_select(
        &mut self,
        origin_read: &mut Pin<&mut FramedOrigin<'_>>,
        client_read: &mut Pin<&mut FramedClient<'_>>,
        origin_write: &mut Pin<&mut WriteHalf<'_>>,
        client_write: &mut Pin<&mut WriteHalf<'_>>,
    ) -> Result<(), ConnectionError> {
        select! {
            Some(res) = client_read.next() => {
                match res {
                    Ok(msg) => {
                        self.handle_client_message(msg).await;
                    }
                    Err(err) => {
                        debug!("read error [{}]", err);
                        return Err(ConnectionError::ProtocolError(err));
                    }
                }
            }
            Some(res) = origin_read.next() => {
                match res {
                    Ok(msg) => {
                        self.handle_origin_message(msg);
                    }
                    Err(err) => {
                        debug!("read error [{}]", err);
                        return Err(ConnectionError::ProtocolError(err));
                    }
                }
            }
            _ = origin_write.writable(), if !self.origin_write_buf.is_empty() => {
                origin_write.write_buf(&mut self.origin_write_buf[0]).await?;
                if !self.origin_write_buf[0].has_remaining() {
                    self.origin_write_buf.pop_front();
                }
            }
            _ = client_write.writable(), if !self.client_write_buf.is_empty() => {
                client_write.write_buf(&mut self.client_write_buf[0]).await?;
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
    async fn connection_select_with_cache(
        &mut self,
        origin_read: &mut Pin<&mut FramedOrigin<'_>>,
        origin_write: &mut Pin<&mut WriteHalf<'_>>,
        client_write: &mut Pin<&mut WriteHalf<'_>>,
    ) -> Result<(), ConnectionError> {
        // Extract cache_rx from self.proxy_mode
        let ProxyMode::CacheRead(ref mut cache_rx) = self.proxy_mode else {
            return Err(ReadError::IoError(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected CacheRead mode",
            ))
            .into());
        };

        select! {
            Some(res) = origin_read.next() => {
                match res {
                    Ok(msg) => {
                        self.handle_origin_message(msg);
                    }
                    Err(err) => {
                        debug!("read error [{}]", err);
                        return Err(ConnectionError::ProtocolError(err));
                    }
                }
            }
            Some(reply) = cache_rx.recv() => {
                self.handle_cache_reply(reply);
            }
            _ = origin_write.writable(), if !self.origin_write_buf.is_empty() => {
                origin_write.write_buf(&mut self.origin_write_buf[0]).await?;
                if !self.origin_write_buf[0].has_remaining() {
                    self.origin_write_buf.pop_front();
                }
            }
            _ = client_write.writable(), if !self.client_write_buf.is_empty() => {
                client_write.write_buf(&mut self.client_write_buf[0]).await?;
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
async fn origin_connect(addrs: &[SocketAddr]) -> Result<TcpStream, ConnectionError> {
    for addr in addrs {
        if let Ok(stream) = TcpStream::connect(addr).await {
            return Ok(stream);
        }
    }
    Err(ConnectionError::NoConnection)
}

type FramedOrigin<'a> = FramedRead<ReadHalf<'a>, PgBackendMessageCodec>;
type FramedClient<'a> = FramedRead<ReadHalf<'a>, PgFrontendMessageCodec>;

/// Set up the streams for client and origin.
fn streams_setup<'a>(
    client_stream: &'a mut TcpStream,
    origin_stream: &'a mut TcpStream,
) -> (
    FramedOrigin<'a>,
    FramedClient<'a>,
    WriteHalf<'a>,
    WriteHalf<'a>,
) {
    let (client_read, client_write) = client_stream.split();
    let client_framed_read = FramedRead::new(client_read, PgFrontendMessageCodec::default());

    let (origin_read, origin_write) = origin_stream.split();
    let origin_framed_read = FramedRead::new(origin_read, PgBackendMessageCodec::default());

    (
        origin_framed_read,
        client_framed_read,
        origin_write,
        client_write,
    )
}

// SAFETY: fd has to refer to a valid TcpStream
unsafe fn fd_dup_to_stream(fd: OwnedFd) -> Result<TcpStream, ConnectionError> {
    let std_stream = unsafe { std::net::TcpStream::from_raw_fd(fd.into_raw_fd()) };
    std_stream.set_nonblocking(true)?;
    let rv = TcpStream::from_std(std_stream)?;
    Ok(rv)
}

#[instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn handle_connection(
    client_stream: &mut TcpStream,
    addrs: Vec<SocketAddr>,
    cache_tx: SenderCacheType,
    metrics: Arc<Metrics>,
) -> Result<(), ConnectionError> {
    // Connect to origin database
    let mut origin_stream = origin_connect(&addrs).await?;

    // Configure TCP settings
    let _ = client_stream.set_nodelay(true);
    let _ = origin_stream.set_nodelay(true);

    // Initialize connection state
    let client_fd_dup = dup(&client_stream)?;
    let mut state = ConnectionState::new(client_fd_dup, metrics);

    // Set up streams
    let (origin_read, client_read, origin_write, client_write) =
        streams_setup(client_stream, &mut origin_stream);

    tokio::pin!(origin_read);
    tokio::pin!(client_read);
    tokio::pin!(origin_write);
    tokio::pin!(client_write);

    loop {
        match state.proxy_mode {
            ProxyMode::Read => {
                if let Err(err) = state
                    .connection_select(
                        &mut origin_read,
                        &mut client_read,
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
                        &mut origin_read,
                        &mut origin_write,
                        &mut client_write,
                    )
                    .await
                {
                    debug!("read error [{}]", err);
                    break;
                }
            }
            ProxyMode::CacheWrite(msg) => {
                state.metrics.cacheable_increment();

                if matches!(msg, CacheMessage::QueryParameterized(_, _, _, _)) {
                    //send a flush to orgin
                    let data = BytesMut::from([b'H', 0, 0, 0, 4u8].as_ref());
                    state.origin_write_buf.push_back(data);
                }

                let (reply_tx, reply_rx) = channel(10);

                let client_socket_dup = unsafe {
                    // SAFETY: client_fd_dup is created from a valid TcpStream
                    fd_dup_to_stream(state.client_fd_dup.try_clone()?)?
                };

                let proxy_msg = ProxyMessage {
                    message: msg,
                    client_socket: client_socket_dup,
                    reply_tx,
                };

                match cache_tx.send(proxy_msg).await {
                    Ok(()) => {
                        state.proxy_mode = ProxyMode::CacheRead(reply_rx);
                    }
                    Err(e) => {
                        // Cache is unavailable, fall back to proxying directly to origin
                        debug!("cache unavailable, degrading to proxy mode: {}", e);
                        state.proxy_status = ProxyStatus::Degraded;
                        let data = match e.0.message {
                            CacheMessage::Query(data, _) => data,
                            CacheMessage::QueryParameterized(data, _, _, _) => data,
                        };

                        state.origin_write_buf.push_back(data);
                        state.proxy_mode = ProxyMode::Read;
                    }
                }
            }
        }
    }

    match state.proxy_status {
        ProxyStatus::Degraded => Err(ConnectionError::CacheDead),
        ProxyStatus::Normal => Ok(()),
    }
}

#[instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub fn connection_run(
    settings: &Settings,
    mut rx: UnboundedReceiver<TcpStream>,
    cache_tx: SenderCacheType,
    metrics: Arc<Metrics>,
) -> Result<(), ConnectionError> {
    let rt = Builder::new_current_thread().enable_all().build()?;

    debug!("handle connection start");
    rt.block_on(async {
        let addrs: Vec<SocketAddr> =
            lookup_host((settings.origin.host.as_str(), settings.origin.port))
                .await?
                .collect();

        LocalSet::new()
            .run_until(async {
                while let Some(mut socket) = rx.recv().await {
                    let addrs = addrs.clone();
                    let cache_tx = cache_tx.clone();
                    let metrics = Arc::clone(&metrics);
                    spawn_local(async move {
                        debug!("task spawn");
                        match handle_connection(&mut socket, addrs, cache_tx, metrics).await {
                            Err(ConnectionError::CacheDead) => {
                                debug!("connection closed in degraded mode");
                                return Err(Error::other("cache dead"));
                            }
                            Err(e) => {
                                error!("{}", e);
                            }
                            Ok(_) => {}
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
