use std::{
    io::{self, Error},
    mem,
    net::SocketAddr,
    thread,
};

use crate::{
    pg::protocol::{
        PgBackendMessage, PgBackendMessageCodec, PgConnectionState, PgFrontendMessage,
        PgFrontendMessageCodec,
    },
    settings::Settings,
};

use futures::StreamExt;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream, lookup_host},
    runtime::Builder,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    task::{LocalSet, spawn_local},
};
use tokio_util::{bytes::Buf, codec::FramedRead};
use tracing::{debug, error, instrument};

type Worker<'scope> = (
    thread::ScopedJoinHandle<'scope, Result<(), Error>>,
    UnboundedSender<TcpStream>,
);

fn worker_create<'scope, 'env: 'scope, 'settings: 'scope>(
    settings: &'settings Settings,
    worker_id: usize,
    scope: &'scope thread::Scope<'scope, 'env>,
) -> Result<Worker<'scope>, Error> {
    let (tx, rx) = unbounded_channel::<TcpStream>();
    let join = thread::Builder::new()
        .name(format!("cnxt {worker_id}"))
        .spawn_scoped(scope, || connection_run(settings, rx))?;

    Ok((join, tx))
}

fn worker_ensure_alive<'scope, 'env: 'scope, 'settings: 'scope>(
    workers: &mut [Worker<'scope>],
    worker_index: usize,
    settings: &'settings Settings,
    scope: &'scope thread::Scope<'scope, 'env>,
) -> Result<bool, Error> {
    if workers[worker_index].0.is_finished() {
        let new_worker = worker_create(settings, worker_index, scope)?;
        let old_worker = mem::replace(&mut workers[worker_index], new_worker);
        let _ = old_worker.0.join();
        Ok(true)
    } else {
        Ok(false)
    }
}

#[instrument]
pub fn handle_listen(settings: &Settings) -> Result<(), Error> {
    thread::scope(|scope| {
        let mut workers: Vec<_> = (0..settings.num_workers)
            .map(|i| worker_create(settings, i, scope))
            .collect::<Result<Vec<_>, _>>()?;

        let rt = Builder::new_current_thread().enable_all().build()?;

        debug!("accept loop");
        rt.block_on(async {
            let listener = TcpListener::bind(&settings.listen.socket).await?;
            debug!("Listening to {}", &settings.listen.socket);

            let mut cur_worker = 0;
            while let Ok((socket, _)) = listener.accept().await {
                debug!("socket accepted");

                let _ = workers[cur_worker].1.send(socket);

                let _ = worker_ensure_alive(&mut workers, cur_worker, settings, scope)?;
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
                    spawn_local(async move {
                        debug!("task spawn");
                        let _ = handle_connection(&mut socket, addrs)
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
    ClientRead,
    OriginWrite(PgFrontendMessage),
    OriginRead,
    ClientWrite(PgBackendMessage),
}

#[instrument]
async fn handle_connection(
    client_socket: &mut TcpStream,
    addrs: Vec<SocketAddr>,
) -> Result<(), Error> {
    let mut maybe_stream: Option<TcpStream> = None;
    for addr in &addrs {
        maybe_stream = TcpStream::connect(addr).await.ok();
        if maybe_stream.is_some() {
            break;
        }
    }

    let Some(mut origin_stream) = maybe_stream else {
        return Err(io::Error::other("no origin connection"));
    };

    let _ = client_socket.set_nodelay(true);
    let _ = origin_stream.set_nodelay(true);

    let mut proxy_mode = ProxyMode::ClientRead;

    let (client_read, client_write) = client_socket.split();
    let mut client_framed_read = FramedRead::new(client_read, PgFrontendMessageCodec::default());
    let mut client_stream_write = client_write;

    let (origin_read, origin_write) = origin_stream.split();
    let mut origin_framed_read = FramedRead::new(origin_read, PgBackendMessageCodec::default());
    let mut origin_stream_write = origin_write;

    loop {
        dbg!(&proxy_mode);
        match proxy_mode {
            ProxyMode::ClientRead => {
                if let Some(res) = client_framed_read.next().await {
                    match res {
                        Ok(msg) => {
                            dbg!(&msg);
                            proxy_mode = ProxyMode::OriginWrite(msg);
                        }
                        Err(err) => {
                            error!("client read [{}]", err);
                        }
                    }
                } else {
                    break;
                }
            }
            ProxyMode::OriginWrite(ref mut msg) => {
                let mut b = msg.get_buf();
                origin_stream_write.write_buf(&mut b).await?;
                if !b.has_remaining() {
                    proxy_mode = ProxyMode::OriginRead;
                }
            }
            ProxyMode::OriginRead => {
                if let Some(res) = origin_framed_read.next().await {
                    match res {
                        Ok(msg) => {
                            dbg!(&msg);
                            match &msg {
                                PgBackendMessage::SslRequestResponse(response) => {
                                    //server will return "N" (no ssl support) or "S" (supported)
                                    //we will pretend the response is always N for now
                                    client_framed_read.decoder_mut().state =
                                        PgConnectionState::AwaitingStartup;
                                    origin_framed_read.decoder_mut().state =
                                        PgConnectionState::AwaitingStartup;
                                }
                                _ => {}
                            }
                            proxy_mode = ProxyMode::ClientWrite(msg);
                        }
                        Err(err) => {
                            error!("origin read [{}]", err);
                        }
                    }
                } else {
                    break;
                }
            }
            ProxyMode::ClientWrite(ref mut msg) => {
                let mut b = msg.get_buf();
                client_stream_write.write_buf(&mut b).await?;
                if !b.has_remaining() {
                    proxy_mode = ProxyMode::ClientRead;
                }
            }
        }
    }

    Ok(())
}
