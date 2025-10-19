use std::{
    collections::{HashMap, VecDeque},
    hash::{DefaultHasher, Hash, Hasher},
    io::{self, Error},
    mem,
    net::SocketAddr,
    os::fd::{FromRawFd, IntoRawFd, OwnedFd},
    pin::Pin,
    thread,
};

use crate::{
    cache::{CacheMessage, CacheReply, ProxyMessage, cache_run, query::CacheableQuery},
    pg::protocol::{
        ProtocolError,
        backend::{PgBackendMessage, PgBackendMessageCodec, PgBackendMessageType},
        extended::{
            ParsedBindMessage, ParsedParseMessage, Portal, PreparedStatement, parse_bind_message,
            parse_close_message, parse_execute_message, parse_parse_message,
        },
        frontend::{PgFrontendMessage, PgFrontendMessageCodec, PgFrontendMessageType},
    },
    query::ast::sql_query_convert,
    settings::Settings,
};

use error_set::error_set;
use nix::{errno::Errno, unistd::dup};
use tokio::{
    io::AsyncWriteExt,
    net::{
        TcpListener, TcpStream, lookup_host,
        tcp::{ReadHalf, WriteHalf},
    },
    runtime::Builder,
    select,
    sync::mpsc::{
        Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel,
    },
    task::{LocalSet, spawn_local},
};
use tokio_stream::StreamExt;
use tokio_util::{
    bytes::{Buf, BytesMut},
    codec::FramedRead,
};
use tracing::{debug, error, instrument, trace};

error_set! {
    ConnectionError := FdError || ConnectError || ReadError || WriteError || DegradedModeExit

    FdError := {
        NixError(Errno),
    }

    ReadError := {
        ProtocolError(ProtocolError),
        IoError(io::Error),
    }

    WriteError := {
        MpscError,
    }

    ConnectError := {
        NoConnection,
    }

    DegradedModeExit := {
        CacheDead,
    }

    ParseError := {
        InvalidUtf8,
        Parse(pg_query::Error)
    }
}

type Worker<'scope> = (
    thread::ScopedJoinHandle<'scope, Result<(), ConnectionError>>,
    UnboundedSender<TcpStream>,
);

type SenderCacheType = Sender<ProxyMessage>;

fn worker_create<'scope, 'env: 'scope, 'settings: 'scope>(
    worker_id: usize,
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    cache_tx: SenderCacheType,
) -> Result<Worker<'scope>, ConnectionError> {
    let (tx, rx) = unbounded_channel::<TcpStream>();
    let join = thread::Builder::new()
        .name(format!("cnxt {worker_id}"))
        .spawn_scoped(scope, || connection_run(settings, rx, cache_tx))?;

    Ok((join, tx))
}

enum WorkerStatus {
    Alive,
    Exited,
    CacheDead,
}

fn worker_ensure_alive<'scope, 'env: 'scope, 'settings: 'scope>(
    workers: &mut [Worker<'scope>],
    worker_index: usize,
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    cache_tx: SenderCacheType,
) -> Result<WorkerStatus, ConnectionError> {
    if workers[worker_index].0.is_finished() {
        let new_worker = worker_create(worker_index, scope, settings, cache_tx)?;
        let old_worker = mem::replace(&mut workers[worker_index], new_worker);
        match old_worker.0.join() {
            Ok(Err(ConnectionError::CacheDead)) => Ok(WorkerStatus::CacheDead),
            _ => Ok(WorkerStatus::Exited),
        }
    } else {
        Ok(WorkerStatus::Alive)
    }
}

type Cache<'scope> = (
    thread::ScopedJoinHandle<'scope, Result<(), crate::cache::CacheError>>,
    SenderCacheType,
);

fn cache_create<'scope, 'env: 'scope, 'settings: 'scope>(
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
) -> Result<Cache<'scope>, Error> {
    const DEFAULT_CHANNEL_SIZE: usize = 100;
    let (cache_tx, cache_rx) = channel(DEFAULT_CHANNEL_SIZE);

    let cache_handle = thread::Builder::new()
        .name("cache".to_owned())
        .spawn_scoped(scope, || cache_run(settings, cache_rx))?;

    Ok((cache_handle, cache_tx))
}

#[instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub fn proxy_run(settings: &Settings) -> Result<(), ConnectionError> {
    thread::scope(|scope| {
        let (mut cache_handle, mut cache_tx) = cache_create(scope, settings)?;

        let mut workers: Vec<_> = (0..settings.num_workers)
            .map(|i| worker_create(i, scope, settings, cache_tx.clone()))
            .collect::<Result<Vec<_>, _>>()?;

        let rt = Builder::new_current_thread().enable_all().build()?;

        debug!("accept loop");
        rt.block_on(async {
            let listener = TcpListener::bind(&settings.listen.socket)
                .await
                .map_err(|e| {
                    ConnectionError::IoError(io::Error::other(format!(
                        "bind error [{}] {e}",
                        &settings.listen.socket
                    )))
                })?;
            debug!("Listening to {}", &settings.listen.socket);

            let mut cur_worker = 0;
            while let Ok((socket, _)) = listener.accept().await {
                debug!("socket accepted");

                let _ = workers[cur_worker].1.send(socket);

                let status = worker_ensure_alive(
                    &mut workers,
                    cur_worker,
                    scope,
                    settings,
                    cache_tx.clone(),
                )?;

                if matches!(status, WorkerStatus::CacheDead) {
                    error!("cache thread detected as dead, restarting...");
                    if cache_handle.is_finished() {
                        let _ = cache_handle.join(); // Clean up old cache thread
                    }
                    (cache_handle, cache_tx) = cache_create(scope, settings)?;
                    debug!("cache thread restarted");
                }

                cur_worker = (cur_worker + 1) % settings.num_workers;
            }

            Ok(())
        })
    })
}

#[instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub fn connection_run(
    settings: &Settings,
    mut rx: UnboundedReceiver<TcpStream>,
    cache_tx: SenderCacheType,
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
                    spawn_local(async move {
                        debug!("task spawn");
                        match handle_connection(&mut socket, addrs, cache_tx).await {
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

#[derive(Debug)]
enum ProxyMode {
    Read,
    CacheRead(Receiver<CacheReply>), //waiting for repsonse from the cache
    // OriginWrite(PgFrontendMessage),
    // ClientWrite(PgBackendMessage),
    CacheWrite(CacheMessage),
}

#[derive(Debug)]
enum ProxyStatus {
    Normal,
    Degraded,
}

/// Manages state for a single client connection.
/// Encapsulates transaction state, query fingerprint cache, and protocol state.
struct ConnectionState {
    /// data waiting to be written to origin
    origin_write_buf: VecDeque<BytesMut>,

    /// data waiting to be written to client
    client_write_buf: VecDeque<BytesMut>,

    /// Cache of query fingerprints to cacheability decisions
    fingerprint_cache: HashMap<u64, Option<Box<CacheableQuery>>>,

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
}

impl ConnectionState {
    fn new(client_fd_dup: OwnedFd) -> Self {
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
        }
    }

    /// Handle a message from the client (frontend).
    /// Determines whether to forward to origin, check cache, or take other action.
    async fn handle_client_message(&mut self, msg: PgFrontendMessage) {
        match msg.message_type {
            PgFrontendMessageType::Query => {
                if !self.in_transaction {
                    self.proxy_mode =
                        match handle_query(&msg.data, &mut self.fingerprint_cache).await {
                            Ok(Action::Forward) => {
                                self.origin_write_buf.push_back(msg.data);
                                ProxyMode::Read
                            }
                            Ok(Action::CacheCheck(ast)) => {
                                ProxyMode::CacheWrite(CacheMessage::Query(msg.data, ast))
                            }
                            Err(e) => {
                                error!("handle_query {}", e);
                                self.origin_write_buf.push_back(msg.data);
                                ProxyMode::Read
                            }
                        };
                } else {
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
            _ => {
                // All other message types - forward to origin
                self.origin_write_buf.push_back(msg.data);
            }
        }
    }

    /// Handle a message from the origin database (backend).
    /// Updates transaction state and forwards to client.
    fn handle_origin_message(&mut self, msg: PgBackendMessage) {
        if msg.message_type == PgBackendMessageType::ReadyForQuery {
            // ReadyForQuery message contains transaction status at byte 5
            // 'I' = idle (not in transaction)
            // 'T' = in transaction block
            // 'E' = in failed transaction block
            self.in_transaction = msg.data[5] == b'T' || msg.data[5] == b'E';

            // Clean up unnamed portals when transaction ends
            if !self.in_transaction {
                self.portals.retain(|name, _| !name.is_empty());
            }
        }

        self.client_write_buf.push_back(msg.data);
    }

    /// Handle a reply from the cache.
    /// If cache indicates error or needs forwarding, send query to origin instead.
    fn handle_cache_reply(&mut self, reply: CacheReply) {
        match reply {
            CacheReply::Complete(_) => self.proxy_mode = ProxyMode::Read,
            CacheReply::Error(buf) | CacheReply::Forward(buf) => {
                debug!("forwarding to origin");
                self.origin_write_buf.push_back(buf);
                self.proxy_mode = ProxyMode::Read;
            }
        }
    }

    /// Handle Parse message - analyze cacheability, store prepared statement, and forward to origin.
    fn handle_parse_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_parse_message(&msg.data) {
            // Analyze cacheability (regardless of transaction state - deferred to Execute)
            let cacheable_query = pg_query::parse(&parsed.sql)
                .ok()
                .and_then(|ast| sql_query_convert(&ast).ok())
                .and_then(|query| CacheableQuery::try_from(&query).ok())
                .map(Box::new);

            self.statement_store(parsed, cacheable_query);
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
        self.proxy_mode = match self.try_cache_execute(&msg) {
            Some(cache_msg) => ProxyMode::CacheWrite(cache_msg),
            None => {
                self.origin_write_buf.push_back(msg.data);
                ProxyMode::Read
            }
        };
    }

    /// Attempt to create a cache message for Execute, returning None if caching not possible.
    fn try_cache_execute(&self, msg: &PgFrontendMessage) -> Option<CacheMessage> {
        // Check transaction state first (cheapest check)
        if self.in_transaction {
            return None;
        }

        // Parse Execute message
        let parsed = parse_execute_message(&msg.data).ok()?;
        debug!("parsed execute message {:?}", parsed);

        // Look up portal
        let portal = self.portals.get(&parsed.portal_name)?;

        // Reject binary format (not supported yet)
        if portal.has_binary_parameters() {
            debug!("has binary parameteres");
            return None;
        }

        // Look up prepared statement
        let stmt = self.prepared_statements.get(&portal.statement_name)?;

        // Check if cacheable
        let cacheable_query = stmt.cacheable_query.as_ref()?.clone();

        // All checks passed - use cache
        Some(CacheMessage::QueryParameterized(
            msg.data.clone(),
            cacheable_query,
            portal.parameter_values.clone(),
        ))
    }

    /// Handle Describe message - forward to origin.
    fn handle_describe_message(&mut self, msg: PgFrontendMessage) {
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
    fn statement_store(
        &mut self,
        parsed: ParsedParseMessage,
        cacheable_query: Option<Box<CacheableQuery>>,
    ) {
        let stmt = PreparedStatement {
            name: parsed.statement_name.clone(),
            sql: parsed.sql,
            parameter_count: parsed.parameter_oids.len(),
            cacheable_query,
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
    fn statements_clear(&mut self) {
        self.prepared_statements.clear();
    }

    /// Clear all portals from connection state.
    fn portals_clear(&mut self) {
        self.portals.clear();
    }

    async fn connection_select(
        &mut self,
        origin_read: &mut Pin<&mut FromedOrigin<'_>>,
        client_read: &mut Pin<&mut FromedClient<'_>>,
        origin_write: &mut Pin<&mut WriteHalf<'_>>,
        client_write: &mut Pin<&mut WriteHalf<'_>>,
    ) -> Result<(), ConnectionError> {
        select! {
            Some(res) = client_read.next() => {
                match res {
                    Ok(msg) => {
                        trace!("client read {:?}", msg.data);
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
                        trace!("origin read {:?}", msg.data);
                        self.handle_origin_message(msg);
                    }
                    Err(err) => {
                        debug!("read error [{}]", err);
                        return Err(ConnectionError::ProtocolError(err));
                    }
                }
            }
            _ = origin_write.writable(), if !self.origin_write_buf.is_empty() => {
                trace!("origin write {:?}", self.origin_write_buf[0]);
                origin_write.write_buf(&mut self.origin_write_buf[0]).await?;
                if !self.origin_write_buf[0].has_remaining() {
                    self.origin_write_buf.pop_front();
                }
            }
            _ = client_write.writable(), if !self.client_write_buf.is_empty() => {
                trace!("client write {:?}", self.client_write_buf[0]);
                client_write.write_buf(&mut self.client_write_buf[0]).await?;
                if !self.client_write_buf[0].has_remaining() {
                    self.client_write_buf.pop_front();
                }
            }
        };

        Ok(())
    }

    async fn connection_select_with_cache(
        &mut self,
        origin_read: &mut Pin<&mut FromedOrigin<'_>>,
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
                        trace!("origin read {:?}", msg.data);
                        self.handle_origin_message(msg);
                    }
                    Err(err) => {
                        debug!("read error [{}]", err);
                        return Err(ConnectionError::ProtocolError(err));
                    }
                }
            }
            Some(reply) = cache_rx.recv() => {
                // trace!("cache read {:?}", reply);
                self.handle_cache_reply(reply);
            }
            _ = origin_write.writable(), if !self.origin_write_buf.is_empty() => {
                trace!("origin write {:?}", self.origin_write_buf[0]);
                origin_write.write_buf(&mut self.origin_write_buf[0]).await?;
                if !self.origin_write_buf[0].has_remaining() {
                    self.origin_write_buf.pop_front();
                }
            }
            _ = client_write.writable(), if !self.client_write_buf.is_empty() => {
                trace!("client write {:?}", self.client_write_buf[0]);
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

type FromedOrigin<'a> = FramedRead<ReadHalf<'a>, PgBackendMessageCodec>;
type FromedClient<'a> = FramedRead<ReadHalf<'a>, PgFrontendMessageCodec>;

/// Set up the streams for client and origin.
fn streams_setup<'a>(
    client_stream: &'a mut TcpStream,
    origin_stream: &'a mut TcpStream,
) -> (
    FromedOrigin<'a>,
    FromedClient<'a>,
    WriteHalf<'a>,
    WriteHalf<'a>,
) {
    let (client_read, client_write) = client_stream.split();
    let client_framed_read = FramedRead::new(client_read, PgFrontendMessageCodec::default());

    let (origin_read, origin_write) = origin_stream.split();
    let origin_framed_read = FramedRead::new(origin_read, PgBackendMessageCodec::default());

    // let client_mapped = client_framed_read.map(|item| item.map(StreamSource::ClientRead));
    // let origin_mapped = origin_framed_read.map(|item| item.map(StreamSource::OriginRead));

    // let streams_read = client_mapped.merge(origin_mapped);

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
) -> Result<(), ConnectionError> {
    // Connect to origin database
    let mut origin_stream = origin_connect(&addrs).await?;

    // Configure TCP settings
    let _ = client_stream.set_nodelay(true);
    let _ = origin_stream.set_nodelay(true);

    // Initialize connection state
    let client_fd_dup = dup(&client_stream)?;
    let mut state = ConnectionState::new(client_fd_dup);

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
                if matches!(msg, CacheMessage::QueryParameterized(_, _, _)) {
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
                            CacheMessage::QueryParameterized(data, _, _) => data,
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

enum Action {
    Forward,
    CacheCheck(Box<CacheableQuery>),
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn handle_query(
    data: &BytesMut,
    fp_cache: &mut HashMap<u64, Option<Box<CacheableQuery>>>,
) -> Result<Action, ParseError> {
    let msg_len = (&data[1..5]).get_u32() as usize;
    let query = str::from_utf8(&data[5..msg_len]).map_err(|_| ParseError::InvalidUtf8)?;

    let mut hasher = DefaultHasher::new();
    query.hash(&mut hasher);
    let fingerprint = hasher.finish();

    match fp_cache.get(&fingerprint) {
        Some(Some(cacheable_query)) => {
            trace!("cache hit: cacheable true");
            Ok(Action::CacheCheck(cacheable_query.clone()))
        }
        Some(None) => {
            trace!("cache hit: cacheable false");
            Ok(Action::Forward)
        }
        None => {
            let ast = pg_query::parse(query)?;

            if let Ok(query) = sql_query_convert(&ast)
                && let Ok(cacheable_query) = CacheableQuery::try_from(&query)
            {
                fp_cache.insert(fingerprint, Some(Box::new(cacheable_query.clone())));
                Ok(Action::CacheCheck(Box::new(cacheable_query)))
            } else {
                fp_cache.insert(fingerprint, None);
                Ok(Action::Forward)
            }
        }
    }
}
