use std::{io::Error, mem, thread};

use crate::settings::Settings;
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    runtime::Builder,
    sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    task::{LocalSet, spawn_local},
};

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
        .name(format!("connection {}", worker_id))
        .spawn_scoped(scope, || handle_connection(settings, rx))?;

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

pub fn handle_connection(
    settings: &Settings,
    mut rx: UnboundedReceiver<TcpStream>,
) -> Result<(), Error> {
    let rt = Builder::new_current_thread().enable_all().build()?;

    println!("handle connection start {:?}", thread::current().name());
    rt.block_on(async {
        let local = LocalSet::new();
        local
            .run_until(async {
                while let Some(mut socket) = rx.recv().await {
                    spawn_local(async move {
                        let mut buf = [0u8; 1024];

                        println!("spawn task {:?}", thread::current().name());

                        let _ = socket.set_nodelay(true);
                        while let Ok(count) = socket.read(&mut buf).await {
                            if count == 0 {
                                break;
                            }
                            println!("read {count} [{:?}]", &buf[0..count]);
                        }
                    });
                }

                Ok(())
            })
            .await
    })
}

pub fn handle_listen(settings: &Settings) -> Result<(), Error> {
    thread::scope(|scope| {
        let mut workers: Vec<_> = (0..settings.num_workers)
            .map(|i| worker_create(settings, i, scope))
            .collect::<Result<Vec<_>, _>>()?;

        let rt = Builder::new_current_thread().enable_all().build()?;

        println!("accept loop {:?}", thread::current().name());
        rt.block_on(async {
            let listener = TcpListener::bind(&settings.listen.socket).await?;
            println!("Listening to {}", &settings.listen.socket);

            let mut cur_worker = 0;
            while let Ok((socket, _)) = listener.accept().await {
                println!("socket accepted");

                let _ = workers[cur_worker].1.send(socket);

                let _ = worker_ensure_alive(&mut workers, cur_worker, settings, scope)?;
                cur_worker = (cur_worker + 1) % settings.num_workers;
            }

            Ok(())
        })
    })
}
