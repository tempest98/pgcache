use std::{io::Error, mem, sync::Arc, thread};

use tokio::{
    net::{TcpListener, TcpStream},
    runtime::Builder,
    sync::mpsc::{Sender, UnboundedSender, channel, unbounded_channel},
};
use tracing::{debug, error};

use crate::{
    cache::{CacheError, ProxyMessage, cache_run},
    metrics::Metrics,
    settings::Settings,
};

use super::{ConnectionError, connection_run};

type SenderCacheType = Sender<ProxyMessage>;

type Worker<'scope> = (
    thread::ScopedJoinHandle<'scope, Result<(), ConnectionError>>,
    UnboundedSender<TcpStream>,
);

enum WorkerStatus {
    Alive,
    Exited,
    CacheDead,
}

fn worker_create<'scope, 'env: 'scope, 'settings: 'scope>(
    worker_id: usize,
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    cache_tx: SenderCacheType,
    metrics: Arc<Metrics>,
) -> Result<Worker<'scope>, ConnectionError> {
    let (tx, rx) = unbounded_channel::<TcpStream>();
    let join = thread::Builder::new()
        .name(format!("cnxt {worker_id}"))
        .spawn_scoped(scope, || connection_run(settings, rx, cache_tx, metrics))?;

    Ok((join, tx))
}

fn worker_ensure_alive<'scope, 'env: 'scope, 'settings: 'scope>(
    workers: &mut [Worker<'scope>],
    worker_index: usize,
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    cache_tx: SenderCacheType,
    metrics: Arc<Metrics>,
) -> Result<WorkerStatus, ConnectionError> {
    if workers[worker_index].0.is_finished() {
        let new_worker = worker_create(worker_index, scope, settings, cache_tx, metrics)?;
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
    thread::ScopedJoinHandle<'scope, Result<(), CacheError>>,
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

#[tracing::instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub fn proxy_run(settings: &Settings, metrics: Arc<Metrics>) -> Result<(), ConnectionError> {
    thread::scope(|scope| {
        let (mut cache_handle, mut cache_tx) = cache_create(scope, settings)?;

        let mut workers: Vec<_> = (0..settings.num_workers)
            .map(|i| worker_create(i, scope, settings, cache_tx.clone(), metrics.clone()))
            .collect::<Result<Vec<_>, _>>()?;

        let rt = Builder::new_current_thread().enable_all().build()?;

        debug!("accept loop");
        rt.block_on(async {
            let listener = TcpListener::bind(&settings.listen.socket)
                .await
                .map_err(|e| {
                    ConnectionError::IoError(std::io::Error::other(format!(
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
                    metrics.clone(),
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
