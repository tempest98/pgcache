use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    io::{self, Error},
    mem,
    net::SocketAddr,
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

use error_set::{ErrContext, error_set};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream, lookup_host},
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
    ConnectionError = ConnectError || ReadError || WriteError || DegradedModeExit;

    ReadError = {
        ProtocolError(ProtocolError),
        IoError(io::Error),
    };

    WriteError = {
        MpscError,
    };

    ConnectError = {
        NoConnection,
    };

    DegradedModeExit = {
        CacheDead,
    };

    ParseError = {
        InvalidUtf8,
        Parse(pg_query::Error)
    };
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
                .with_error_context(|e| format!("bind error [{}] {e}", &settings.listen.socket))?;
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

#[instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn handle_connection(
    client_socket: &mut TcpStream,
    addrs: Vec<SocketAddr>,
    cache_tx: SenderCacheType,
) -> Result<(), ConnectionError> {
    let mut maybe_stream: Option<TcpStream> = None;
    for addr in &addrs {
        maybe_stream = TcpStream::connect(addr).await.ok();
        if maybe_stream.is_some() {
            break;
        }
    }

    let Some(mut origin_stream) = maybe_stream else {
        return Err(ConnectionError::NoConnection);
    };

    let _ = client_socket.set_nodelay(true);
    let _ = origin_stream.set_nodelay(true);

    let mut proxy_mode = ProxyMode::Read;
    let mut proxy_status = ProxyStatus::Normal;

    let (client_read, mut client_write) = client_socket.split();
    let client_framed_read = FramedRead::new(client_read, PgFrontendMessageCodec::default());

    let (origin_read, mut origin_write) = origin_stream.split();
    let origin_framed_read = FramedRead::new(origin_read, PgBackendMessageCodec::default());

    let client_mapped = client_framed_read.map(|item| item.map(StreamSource::ClientRead));
    let origin_mapped = origin_framed_read.map(|item| item.map(StreamSource::OriginRead));

    // let mut framed_read = client_mapped.merge(origin_mapped);

    let mut streams_read: StreamMap<&'static str, Pin<Box<dyn Stream<Item = StreamSourceResult>>>> =
        StreamMap::new();

    let client_mapped_pin = Box::pin(client_mapped); // as Pin<Box<dyn Stream<Item = Result<StreamSource, ProtocolError>>>>;
    let origin_mapped_pin = Box::pin(origin_mapped); // as Pin<Box<dyn Stream<Item = Result<StreamSource, ProtocolError>>>>;

    streams_read.insert("client", client_mapped_pin);
    streams_read.insert("origin", origin_mapped_pin);

    let mut fingerprint_cache: HashMap<u64, Option<Box<CacheableQuery>>> = HashMap::new();

    let mut in_transaction = false;

    loop {
        match proxy_mode {
            ProxyMode::Read => {
                if let Some((_, res)) = streams_read.next().await {
                    match res {
                        Ok(StreamSource::ClientRead(msg)) => {
                            if !in_transaction
                                && matches!(msg.message_type, PgFrontendMessageType::Query)
                            {
                                proxy_mode =
                                    match handle_query(&msg.data, &mut fingerprint_cache).await {
                                        Ok(Action::Forward) => ProxyMode::OriginWrite(msg),
                                        Ok(Action::CacheCheck(ast)) => ProxyMode::CacheWrite(
                                            CacheMessage::Query(msg.data, ast),
                                        ),
                                        Err(e) => {
                                            error!("handle_query {}", e);
                                            ProxyMode::OriginWrite(msg)
                                        }
                                    };
                            } else {
                                proxy_mode = ProxyMode::OriginWrite(msg);
                            }
                        }
                        Ok(StreamSource::OriginRead(msg)) => {
                            if msg.message_type == PgBackendMessageType::ReadyForQuery {
                                in_transaction = msg.data[5] == b'T' || msg.data[5] == b'E';
                            }
                            proxy_mode = ProxyMode::ClientWrite(msg);
                        }
                        Ok(StreamSource::CacheRead(reply)) => match reply {
                            CacheReply::Data(buf, _is_complete) => {
                                proxy_mode = ProxyMode::ClientWrite(PgBackendMessage {
                                    message_type: PgBackendMessageType::Multi,
                                    data: buf,
                                })
                            }
                            CacheReply::Error(buf) | CacheReply::Forward(buf) => {
                                debug!("forwarding to origin");
                                //send query to origin instead
                                proxy_mode = ProxyMode::OriginWrite(PgFrontendMessage {
                                    message_type: PgFrontendMessageType::Query,
                                    data: buf,
                                })
                            }
                        },
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
                    proxy_mode = ProxyMode::Read;
                }
            }
            ProxyMode::ClientWrite(ref mut msg) => {
                client_write.write_buf(&mut msg.data).await?;
                if !msg.data.has_remaining() {
                    proxy_mode = ProxyMode::Read;
                }
            }
            ProxyMode::CacheWrite(msg) => {
                let (resp_tx, resp_rx) = channel(10);

                if let Err(e) = cache_tx.send((msg, resp_tx)).await {
                    // Cache is unavailable, fall back to proxying directly to origin
                    debug!("cache unavailable, degrading to proxy mode: {}", e);
                    proxy_status = ProxyStatus::Degraded;
                    let CacheMessage::Query(data, _) = e.0.0;
                    proxy_mode = ProxyMode::OriginWrite(PgFrontendMessage {
                        message_type: PgFrontendMessageType::Query,
                        data,
                    });
                } else {
                    let stream_rx = Box::pin(
                        ReceiverStream::new(resp_rx).map(|item| Ok(StreamSource::CacheRead(item))),
                    );
                    streams_read.insert("cache_reply", stream_rx);
                    proxy_mode = ProxyMode::Read;
                }
            }
        }
    }

    match proxy_status {
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
