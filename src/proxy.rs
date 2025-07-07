use std::{
    io::{self, Error},
    mem,
    net::SocketAddr,
    pin::Pin,
    thread,
};

use crate::{
    cache::{CacheMessage, CacheReply, is_cacheable},
    pg::protocol::{
        ProtocolError,
        backend::{PgBackendMessage, PgBackendMessageCodec, PgBackendMessageType},
        frontend::{PgFrontendMessage, PgFrontendMessageCodec, PgFrontendMessageType},
    },
    settings::Settings,
    stream_utils::ReceiverStream,
};

use error_set::{ErrContext, error_set};
use pg_query::ParseResult;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream, lookup_host},
    runtime::Builder,
    sync::{
        mpsc::{Sender, UnboundedReceiver, UnboundedSender, unbounded_channel},
        oneshot,
    },
    task::{LocalSet, spawn_local},
};
use tokio_stream::{Stream, StreamExt, StreamMap};
use tokio_util::{
    bytes::{Buf, BufMut, BytesMut},
    codec::FramedRead,
};
use tracing::{debug, error, instrument};

error_set! {
    ConnectionError = ConnectError || ReadError || WriteError;

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

    ParseError = {
        InvalidUtf8,
        Parse(pg_query::Error)
    };
}

type Worker<'scope> = (
    thread::ScopedJoinHandle<'scope, Result<(), Error>>,
    UnboundedSender<TcpStream>,
);

fn worker_create<'scope, 'env: 'scope, 'settings: 'scope>(
    worker_id: usize,
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    cache_tx: Sender<(CacheMessage, oneshot::Sender<CacheReply>)>,
) -> Result<Worker<'scope>, Error> {
    let (tx, rx) = unbounded_channel::<TcpStream>();
    let join = thread::Builder::new()
        .name(format!("cnxt {worker_id}"))
        .spawn_scoped(scope, || connection_run(settings, rx, cache_tx))?;

    Ok((join, tx))
}

fn worker_ensure_alive<'scope, 'env: 'scope, 'settings: 'scope>(
    workers: &mut [Worker<'scope>],
    worker_index: usize,
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    cache_tx: Sender<(CacheMessage, oneshot::Sender<CacheReply>)>,
) -> Result<bool, Error> {
    if workers[worker_index].0.is_finished() {
        let new_worker = worker_create(worker_index, scope, settings, cache_tx)?;
        let old_worker = mem::replace(&mut workers[worker_index], new_worker);
        let _ = old_worker.0.join();
        Ok(true)
    } else {
        Ok(false)
    }
}

#[instrument]
pub fn proxy_run(
    settings: &Settings,
    cache_tx: Sender<(CacheMessage, oneshot::Sender<CacheReply>)>,
) -> Result<(), ConnectionError> {
    thread::scope(|scope| {
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

                let _ = worker_ensure_alive(
                    &mut workers,
                    cur_worker,
                    scope,
                    settings,
                    cache_tx.clone(),
                )?;
                cur_worker = (cur_worker + 1) % settings.num_workers;
            }

            Ok(())
        })
    })
}

#[instrument]
pub fn connection_run(
    settings: &Settings,
    mut rx: UnboundedReceiver<TcpStream>,
    cache_tx: Sender<(CacheMessage, oneshot::Sender<CacheReply>)>,
) -> Result<(), Error> {
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
                        let _ = handle_connection(&mut socket, addrs, cache_tx)
                            .await
                            .inspect_err(|e| error!("{}", e));
                        debug!("task done");
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

#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
enum StreamSource {
    ClientRead(PgFrontendMessage),
    OriginRead(PgBackendMessage),
    CacheRead(CacheReply),
}

#[instrument]
async fn handle_connection(
    client_socket: &mut TcpStream,
    addrs: Vec<SocketAddr>,
    cache_tx: Sender<(CacheMessage, oneshot::Sender<CacheReply>)>,
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

    let (client_read, client_write) = client_socket.split();
    let client_framed_read = FramedRead::new(client_read, PgFrontendMessageCodec::default());
    let mut client_stream_write = client_write;

    let (origin_read, origin_write) = origin_stream.split();
    let origin_framed_read = FramedRead::new(origin_read, PgBackendMessageCodec::default());
    let mut origin_stream_write = origin_write;

    let client_mapped = client_framed_read.map(|item| item.map(StreamSource::ClientRead));
    let origin_mapped = origin_framed_read.map(|item| item.map(StreamSource::OriginRead));

    // let mut framed_read = client_mapped.merge(origin_mapped);

    let mut streams_read: StreamMap<
        &'static str,
        Pin<Box<dyn Stream<Item = Result<StreamSource, ProtocolError>>>>,
    > = StreamMap::new();

    let client_mapped_pin = Box::pin(client_mapped); // as Pin<Box<dyn Stream<Item = Result<StreamSource, ProtocolError>>>>;
    let origin_mapped_pin = Box::pin(origin_mapped); // as Pin<Box<dyn Stream<Item = Result<StreamSource, ProtocolError>>>>;

    streams_read.insert("client", client_mapped_pin);
    streams_read.insert("origin", origin_mapped_pin);

    loop {
        // dbg!(&proxy_mode);
        match proxy_mode {
            ProxyMode::Read => {
                if let Some((_, res)) = streams_read.next().await {
                    match res {
                        Ok(StreamSource::ClientRead(msg)) => {
                            dbg!(&msg);
                            if matches!(msg.message_type, PgFrontendMessageType::Query) {
                                proxy_mode = match handle_query(&msg.data).await {
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
                                proxy_mode = ProxyMode::OriginWrite(msg);
                            }
                        }
                        Ok(StreamSource::OriginRead(msg)) => {
                            proxy_mode = ProxyMode::ClientWrite(msg);
                        }
                        Ok(StreamSource::CacheRead(reply)) => match reply {
                            CacheReply::Data(buf) => {
                                proxy_mode = ProxyMode::ClientWrite(PgBackendMessage {
                                    message_type: PgBackendMessageType::Multi,
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
                origin_stream_write.write_buf(&mut msg.data).await?;
                if !msg.data.has_remaining() {
                    proxy_mode = ProxyMode::Read;
                }
            }
            ProxyMode::ClientWrite(ref mut msg) => {
                dbg!(&msg);
                client_stream_write.write_buf(&mut msg.data).await?;
                if !msg.data.has_remaining() {
                    proxy_mode = ProxyMode::Read;
                }
            }
            ProxyMode::CacheWrite(msg) => {
                let (resp_tx, resp_rx) = oneshot::channel();
                let stream_rx = Box::pin(
                    ReceiverStream::new(resp_rx).map(|item| Ok(StreamSource::CacheRead(item))),
                );
                streams_read.insert("cache_reply", stream_rx);

                cache_tx.send((msg, resp_tx)).await.map_err(|e| {
                    error!("{}", e);
                    WriteError::MpscError
                })?;
                proxy_mode = ProxyMode::Read;
            }
        }
    }

    Ok(())
}

enum Action {
    Forward,
    CacheCheck(ParseResult),
}

async fn handle_query(data: &BytesMut) -> Result<Action, ParseError> {
    let msg_len = (&data[1..5]).get_u32() as usize;
    let query = str::from_utf8(&data[5..msg_len]).map_err(|_| ParseError::InvalidUtf8)?;

    let ast = pg_query::parse(query)?;
    if is_cacheable(&ast) {
        Ok(Action::CacheCheck(ast))
    } else {
        Ok(Action::Forward)
    }
}
