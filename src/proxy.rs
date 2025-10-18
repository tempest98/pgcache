use std::{
    collections::HashMap,
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
        frontend::{PgFrontendMessage, PgFrontendMessageCodec, PgFrontendMessageType},
    },
    query::ast::sql_query_convert,
    settings::Settings,
};

use error_set::error_set;
use nix::{errno::Errno, unistd::dup};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream, lookup_host, tcp::WriteHalf},
    runtime::Builder,
    sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel},
    task::{LocalSet, spawn_local},
};
use tokio_stream::{Stream, StreamExt, StreamMap, wrappers::ReceiverStream};
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
    OriginWrite(PgFrontendMessage),
    ClientWrite(PgBackendMessage),
    CacheWrite(CacheMessage),
}

#[derive(Debug)]
enum ProxyStatus {
    Normal,
    Degraded,
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
enum StreamSource {
    ClientRead(PgFrontendMessage),
    OriginRead(PgBackendMessage),
    CacheRead(CacheReply),
}

type StreamSourceResult = Result<StreamSource, ProtocolError>;

/// Manages state for a single client connection.
/// Encapsulates transaction state, query fingerprint cache, and protocol state.
struct ConnectionState {
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
}

impl ConnectionState {
    fn new(client_fd_dup: OwnedFd) -> Self {
        Self {
            fingerprint_cache: HashMap::new(),
            in_transaction: false,
            proxy_mode: ProxyMode::Read,
            proxy_status: ProxyStatus::Normal,
            client_fd_dup,
        }
    }

    /// Handle a message from the client (frontend).
    /// Determines whether to forward to origin, check cache, or take other action.
    async fn handle_client_message(&mut self, msg: PgFrontendMessage) {
        if !self.in_transaction && matches!(msg.message_type, PgFrontendMessageType::Query) {
            self.proxy_mode = match handle_query(&msg.data, &mut self.fingerprint_cache).await {
                Ok(Action::Forward) => ProxyMode::OriginWrite(msg),
                Ok(Action::CacheCheck(ast)) => {
                    ProxyMode::CacheWrite(CacheMessage::Query(msg.data, ast))
                }
                Err(e) => {
                    error!("handle_query {}", e);
                    ProxyMode::OriginWrite(msg)
                }
            };
        } else {
            // Not a cacheable query message - forward to origin
            self.proxy_mode = ProxyMode::OriginWrite(msg);
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
        }
        self.proxy_mode = ProxyMode::ClientWrite(msg);
    }

    /// Handle a reply from the cache.
    /// If cache indicates error or needs forwarding, send query to origin instead.
    fn handle_cache_reply(&mut self, reply: CacheReply) {
        match reply {
            CacheReply::Error(buf) | CacheReply::Forward(buf) => {
                debug!("forwarding to origin");
                self.proxy_mode = ProxyMode::OriginWrite(PgFrontendMessage {
                    message_type: PgFrontendMessageType::Query,
                    data: buf,
                })
            }
        }
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

type ProxyStreamMap<'a> =
    StreamMap<&'static str, Pin<Box<dyn Stream<Item = StreamSourceResult> + 'a>>>;

/// Set up the stream map for multiplexing client and origin messages.
fn streams_setup<'a>(
    client_socket: &'a mut TcpStream,
    origin_stream: &'a mut TcpStream,
) -> (ProxyStreamMap<'a>, WriteHalf<'a>, WriteHalf<'a>) {
    let (client_read, client_write) = client_socket.split();
    let client_framed_read = FramedRead::new(client_read, PgFrontendMessageCodec::default());

    let (origin_read, origin_write) = origin_stream.split();
    let origin_framed_read = FramedRead::new(origin_read, PgBackendMessageCodec::default());

    let client_mapped = client_framed_read.map(|item| item.map(StreamSource::ClientRead));
    let origin_mapped = origin_framed_read.map(|item| item.map(StreamSource::OriginRead));

    let mut streams_read: StreamMap<&'static str, Pin<Box<dyn Stream<Item = StreamSourceResult>>>> =
        StreamMap::new();

    streams_read.insert("client", Box::pin(client_mapped));
    streams_read.insert("origin", Box::pin(origin_mapped));

    (streams_read, origin_write, client_write)
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
    client_socket: &mut TcpStream,
    addrs: Vec<SocketAddr>,
    cache_tx: SenderCacheType,
) -> Result<(), ConnectionError> {
    // Connect to origin database
    let mut origin_stream = origin_connect(&addrs).await?;

    // Configure TCP settings
    let _ = client_socket.set_nodelay(true);
    let _ = origin_stream.set_nodelay(true);

    // Initialize connection state
    let client_fd_dup = dup(&client_socket)?;
    let mut state = ConnectionState::new(client_fd_dup);

    // Set up stream multiplexing
    let (mut streams_read, mut origin_write, mut client_write) =
        streams_setup(client_socket, &mut origin_stream);

    loop {
        match state.proxy_mode {
            ProxyMode::Read => {
                if let Some((_, res)) = streams_read.next().await {
                    match res {
                        Ok(StreamSource::ClientRead(msg)) => {
                            state.handle_client_message(msg).await;
                        }
                        Ok(StreamSource::OriginRead(msg)) => {
                            state.handle_origin_message(msg);
                        }
                        Ok(StreamSource::CacheRead(reply)) => {
                            state.handle_cache_reply(reply);
                        }
                        Err(err) => {
                            debug!("read error [{}]", err);
                            return Err(ConnectionError::ProtocolError(err));
                        }
                    }
                } else {
                    break;
                }
            }
            ProxyMode::OriginWrite(ref mut msg) => {
                origin_write.write_buf(&mut msg.data).await?;
                if !msg.data.has_remaining() {
                    state.proxy_mode = ProxyMode::Read;
                }
            }
            ProxyMode::ClientWrite(ref mut msg) => {
                client_write.write_buf(&mut msg.data).await?;
                if !msg.data.has_remaining() {
                    state.proxy_mode = ProxyMode::Read;
                }
            }
            ProxyMode::CacheWrite(msg) => {
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
                        // Cache accepted the message, wait for reply via stream
                        let stream_rx = Box::pin(
                            ReceiverStream::new(reply_rx)
                                .map(|item| Ok(StreamSource::CacheRead(item))),
                        );
                        streams_read.insert("cache_reply", stream_rx);
                        state.proxy_mode = ProxyMode::Read;
                    }
                    Err(e) => {
                        // Cache is unavailable, fall back to proxying directly to origin
                        debug!("cache unavailable, degrading to proxy mode: {}", e);
                        state.proxy_status = ProxyStatus::Degraded;
                        let CacheMessage::Query(data, _) = e.0.message;
                        state.proxy_mode = ProxyMode::OriginWrite(PgFrontendMessage {
                            message_type: PgFrontendMessageType::Query,
                            data,
                        });
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
