use std::{
    io::{self, Error},
    mem,
    net::SocketAddr,
    thread,
};

use crate::settings::Settings;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    join,
    net::{
        TcpListener, TcpStream, lookup_host,
        tcp::{ReadHalf, WriteHalf},
    },
    runtime::Builder,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    task::{LocalSet, spawn_local},
};
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
        .name(format!("cnxt {}", worker_id))
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

    let (client_read, client_write) = client_socket.split();
    let (origin_read, origin_write) = origin_stream.split();

    let results = join!(
        pump(client_read, origin_write),
        pump(origin_read, client_write)
    );

    Ok(())
}

async fn pump<'r, 'w>(
    mut stream_from: ReadHalf<'r>,
    mut stream_to: WriteHalf<'w>,
) -> Result<(), Error> {
    enum Mode {
        Read,
        Write,
    }

    let mut mode = Mode::Read;
    let mut buf = [0u8; 32768];
    let mut count_buf = 0;
    let mut count_written = 0;

    loop {
        match mode {
            Mode::Read => {
                let Ok(count) = stream_from.read(&mut buf).await else {
                    return Err(io::Error::other("read error"));
                };
                if count > 0 {
                    mode = Mode::Write;
                    debug!(
                        "read {count} [{}]",
                        String::from_utf8_lossy(&buf[0..count].escape_ascii().collect::<Vec<u8>>())
                    );
                    count_buf = count;
                } else {
                    break;
                }
            }
            Mode::Write => {
                let Ok(count) = stream_to.write(&buf[count_written..count_buf]).await else {
                    return Err(io::Error::other("write error"));
                };
                if count > 0 {
                    debug!(
                        "write {count} [{}]",
                        String::from_utf8_lossy(
                            &buf[count_written..(count_written + count)]
                                .escape_ascii()
                                .collect::<Vec<u8>>()
                        )
                    );
                    count_written += count;
                    if count_written == count_buf {
                        count_written = 0;
                        mode = Mode::Read;
                    }
                } else {
                    break;
                }
            }
        }
    }

    Ok(())
}
