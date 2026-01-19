use std::{io::Error, mem, sync::Arc, thread, time::Duration};

use tokio::{
    net::{TcpListener, TcpStream},
    runtime::Builder,
    sync::mpsc::{Sender, UnboundedSender, channel, unbounded_channel},
};
use tracing::{debug, error};

use crate::{
    cache::{CacheError, ProxyMessage, cache_run},
    metrics::Metrics,
    pg::cdc::{replication_cleanup, replication_provision},
    settings::Settings,
    tls,
};

use super::{ConnectionError, connection_run};

type SenderCacheType = Sender<ProxyMessage>;

type Worker<'scope> = (
    thread::ScopedJoinHandle<'scope, Result<(), ConnectionError>>,
    UnboundedSender<TcpStream>,
);

fn worker_create<'scope, 'env: 'scope, 'settings: 'scope>(
    worker_id: usize,
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    cache_tx: SenderCacheType,
    metrics: Arc<Metrics>,
    tls_acceptor: Option<Arc<tls::TlsAcceptor>>,
) -> Result<Worker<'scope>, ConnectionError> {
    let (tx, rx) = unbounded_channel::<TcpStream>();
    let join = thread::Builder::new()
        .name(format!("cnxt {worker_id}"))
        .spawn_scoped(scope, || {
            connection_run(settings, rx, cache_tx, metrics, tls_acceptor)
        })?;

    Ok((join, tx))
}

fn workers_create_all<'scope, 'env: 'scope, 'settings: 'scope>(
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    cache_tx: SenderCacheType,
    metrics: Arc<Metrics>,
    tls_acceptor: Option<Arc<tls::TlsAcceptor>>,
) -> Result<Vec<Worker<'scope>>, ConnectionError> {
    (0..settings.num_workers)
        .map(|i| {
            worker_create(
                i,
                scope,
                settings,
                cache_tx.clone(),
                Arc::clone(&metrics),
                tls_acceptor.clone(),
            )
        })
        .collect()
}

fn worker_ensure_alive<'scope, 'env: 'scope, 'settings: 'scope>(
    workers: &mut [Worker<'scope>],
    worker_index: usize,
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    cache_tx: SenderCacheType,
    metrics: Arc<Metrics>,
    tls_acceptor: Option<Arc<tls::TlsAcceptor>>,
) -> Result<(), ConnectionError> {
    let Some(worker) = workers.get_mut(worker_index) else {
        return Err(ConnectionError::IoError(std::io::Error::other(
            "worker index out of bounds",
        )));
    };

    if worker.0.is_finished() {
        let new_worker = worker_create(
            worker_index,
            scope,
            settings,
            cache_tx,
            metrics,
            tls_acceptor,
        )?;
        let old_worker = mem::replace(worker, new_worker);
        let _ = old_worker.0.join();
    }

    Ok(())
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

fn tls_config_load(settings: &Settings) -> Result<Option<Arc<tls::TlsAcceptor>>, ConnectionError> {
    match (&settings.tls_cert, &settings.tls_key) {
        (Some(cert_path), Some(key_path)) => {
            debug!(
                "Loading TLS certificates from {:?} and {:?}",
                cert_path, key_path
            );
            let config = tls::server_tls_config_build(cert_path, key_path).map_err(|e| {
                ConnectionError::IoError(std::io::Error::other(format!(
                    "Failed to load TLS certificates: {e}"
                )))
            })?;
            Ok(Some(Arc::new(tls::TlsAcceptor::from(config))))
        }
        (None, None) => {
            debug!("TLS not configured, accepting plaintext connections only");
            Ok(None)
        }
        _ => Err(ConnectionError::IoError(std::io::Error::other(
            "Both tls_cert and tls_key must be specified together",
        ))),
    }
}

#[tracing::instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub fn proxy_run(settings: &Settings, metrics: Arc<Metrics>) -> Result<(), ConnectionError> {
    // Load TLS config if certificates are provided
    let tls_acceptor = tls_config_load(settings)?;

    thread::scope(|scope| {
        let rt = Builder::new_current_thread().enable_all().build()?;

        let _ = rt.block_on(async { replication_provision(settings).await });

        let (mut cache_handle, mut cache_tx) = cache_create(scope, settings)?;

        let mut workers = workers_create_all(
            scope,
            settings,
            cache_tx.clone(),
            Arc::clone(&metrics),
            tls_acceptor.clone(),
        )?;

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
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        let (socket, _) = result.map_err(|e| {
                            ConnectionError::IoError(std::io::Error::other(format!(
                                "accept error: {e}"
                            )))
                        })?;
                        debug!("socket accepted");

                        if let Some(worker) = workers.get(cur_worker) {
                            let _ = worker.1.send(socket);
                        }

                        worker_ensure_alive(
                            &mut workers,
                            cur_worker,
                            scope,
                            settings,
                            cache_tx.clone(),
                            Arc::clone(&metrics),
                            tls_acceptor.clone(),
                        )?;

                        cur_worker = (cur_worker + 1) % settings.num_workers;
                    }
                    _ = cache_tx.closed() => {
                        error!("cache thread exited, restarting...");
                        let _ = cache_handle.join();
                        debug!("cache handle joined");
                        replication_provision(settings).await?;
                        debug!("replication_provision");
                        (cache_handle, cache_tx) = cache_create(scope, settings)?;
                        debug!("cache_create");
                        workers = workers_create_all(
                            scope,
                            settings,
                            cache_tx.clone(),
                            Arc::clone(&metrics),
                            tls_acceptor.clone(),
                        )?;
                        debug!("cache thread restarted");
                    }
                }
            }

            #[allow(unreachable_code)]
            {
                replication_cleanup(settings).await?;
                Ok(())
            }
        })
    })
}
