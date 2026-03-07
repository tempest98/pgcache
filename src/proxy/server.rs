use std::{collections::HashMap, mem, sync::Arc, thread, time::Duration};

use rootcause::Report;

use crate::result::{MapIntoReport, ReportExt};
use tokio::{
    net::{TcpListener, TcpStream},
    runtime::Builder,
    sync::mpsc::{UnboundedSender, channel, unbounded_channel},
    time::{Sleep, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace};

use crate::metrics::names;

/// Initial backoff delay for cache restart attempts
const INITIAL_BACKOFF_MS: u64 = 100;
/// Maximum backoff delay (steady state) for cache restart attempts
const STEADY_STATE_BACKOFF_MS: u64 = 60_000;

struct ProxyCacheState<'scope> {
    handle: Option<thread::ScopedJoinHandle<'scope, CacheResult<()>>>,
    updater: CacheSenderUpdater,
    /// Current sender for detecting when the cache thread exits via `.closed()`.
    current_tx: CacheSenderInner,
    alive: bool,
    attempts: u32,
    backoff: Option<std::pin::Pin<Box<Sleep>>>,
    backoff_ms: u64,
}

impl<'scope> ProxyCacheState<'scope> {
    fn new(
        handle: thread::ScopedJoinHandle<'scope, CacheResult<()>>,
        updater: CacheSenderUpdater,
        current_tx: CacheSenderInner,
    ) -> Self {
        Self {
            handle: Some(handle),
            updater,
            current_tx,
            alive: true,
            attempts: 0,
            backoff: None,
            backoff_ms: INITIAL_BACKOFF_MS,
        }
    }

    fn handle_exit(&mut self) {
        self.alive = false;
        self.attempts += 1;

        // Mark cache as unavailable immediately so connections fall back to origin
        self.updater.sender_clear();

        let exit_result = self.handle.take().map(|h| h.join());
        error!(
            "cache thread exited (attempt {}): {:?}",
            self.attempts, exit_result
        );

        self.backoff_schedule();
    }

    async fn handle_backoff_expired<'env: 'scope, 'settings: 'scope>(
        &mut self,
        scope: &'scope thread::Scope<'scope, 'env>,
        settings: &'settings Settings,
        pinned: &'settings [PinnedQuery],
        cancel: CancellationToken,
    ) {
        self.backoff = None;
        self.attempts += 1;

        debug!(
            "backoff expired, attempting restart (attempt {})",
            self.attempts
        );

        if let Some((new_handle, new_tx)) =
            cache_restart_attempt(scope, settings, pinned, cancel).await
        {
            self.handle = Some(new_handle);
            // Update all subscribers with the new cache sender
            self.updater.sender_update(new_tx.clone());
            self.current_tx = new_tx;
            self.restart_reset();
            debug!("cache thread restarted successfully");
        } else {
            self.backoff_increase();
            debug!(
                "restart failed, backing off {}ms before next attempt",
                self.backoff_ms
            );
            self.backoff_schedule();
        }
    }

    fn restart_reset(&mut self) {
        self.alive = true;
        self.attempts = 0;
        self.backoff_ms = INITIAL_BACKOFF_MS;
    }

    fn backoff_schedule(&mut self) {
        self.backoff = Some(Box::pin(sleep(Duration::from_millis(self.backoff_ms))));
    }

    fn backoff_increase(&mut self) {
        self.backoff_ms = (self.backoff_ms * 2).min(STEADY_STATE_BACKOFF_MS);
    }

    async fn backoff_wait(backoff: &mut Option<std::pin::Pin<Box<Sleep>>>) {
        match backoff {
            Some(sleep) => sleep.as_mut().await,
            None => std::future::pending().await,
        }
    }
}

use crate::{
    cache::query::CacheableQuery,
    cache::{CacheResult, PinnedQuery, cache_run},
    catalog::{FunctionVolatility, function_volatility_map_load},
    pg::{
        cdc::{replication_cleanup, replication_provision},
        connect,
    },
    query::ast::{query_expr_convert, query_expr_fingerprint},
    settings::Settings,
    tls,
};

use super::cache_sender::CacheSenderInner;
use super::{CacheSender, CacheSenderUpdater, ConnectionError, ConnectionResult, connection_run};

type Worker<'scope> = (
    thread::ScopedJoinHandle<'scope, ConnectionResult<()>>,
    UnboundedSender<TcpStream>,
);

/// Shared resources passed to worker threads.
#[derive(Clone)]
struct WorkerResources {
    cache_sender: CacheSender,
    tls_acceptor: Option<Arc<tls::TlsAcceptor>>,
    func_volatility: Arc<HashMap<String, FunctionVolatility>>,
}

fn worker_create<'scope, 'env: 'scope, 'settings: 'scope>(
    worker_id: usize,
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    resources: WorkerResources,
) -> ConnectionResult<Worker<'scope>> {
    let (tx, rx) = unbounded_channel::<TcpStream>();
    let join = thread::Builder::new()
        .name(format!("cnxt {worker_id}"))
        .spawn_scoped(scope, move || {
            connection_run(
                worker_id,
                settings,
                rx,
                resources.cache_sender,
                resources.tls_acceptor,
                resources.func_volatility,
            )
        })
        .map_into_report::<ConnectionError>()?;

    Ok((join, tx))
}

fn workers_create_all<'scope, 'env: 'scope, 'settings: 'scope>(
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    resources: WorkerResources,
) -> ConnectionResult<Vec<Worker<'scope>>> {
    (0..settings.num_workers)
        .map(|i| worker_create(i, scope, settings, resources.clone()))
        .collect()
}

fn worker_ensure_alive<'scope, 'env: 'scope, 'settings: 'scope>(
    workers: &mut [Worker<'scope>],
    worker_index: usize,
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    resources: WorkerResources,
) -> ConnectionResult<()> {
    let Some(worker) = workers.get_mut(worker_index) else {
        return Err(
            ConnectionError::IoError(std::io::Error::other("worker index out of bounds")).into(),
        );
    };

    if worker.0.is_finished() {
        let new_worker = worker_create(worker_index, scope, settings, resources)?;
        let old_worker = mem::replace(worker, new_worker);
        let _ = old_worker.0.join();
    }

    Ok(())
}

type Cache<'scope> = (
    thread::ScopedJoinHandle<'scope, CacheResult<()>>,
    CacheSenderInner,
);

fn cache_create<'scope, 'env: 'scope, 'settings: 'scope>(
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    pinned: &'settings [PinnedQuery],
    cancel: CancellationToken,
) -> Result<Cache<'scope>, std::io::Error> {
    const DEFAULT_CHANNEL_SIZE: usize = 100;
    let (cache_tx, cache_rx) = channel(DEFAULT_CHANNEL_SIZE);

    let cache_handle = thread::Builder::new()
        .name("cache".to_owned())
        .spawn_scoped(scope, || cache_run(settings, cache_rx, pinned, cancel))?;

    Ok((cache_handle, cache_tx))
}

/// Attempts to restart the cache thread.
/// On success, returns Some with the new cache handle and sender.
/// On failure, returns None.
async fn cache_restart_attempt<'scope, 'env: 'scope, 'settings: 'scope>(
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    pinned: &'settings [PinnedQuery],
    cancel: CancellationToken,
) -> Option<Cache<'scope>> {
    if let Err(e) = replication_provision(settings).await {
        error!("replication provision failed: {:?}", e);
        return None;
    }

    match cache_create(scope, settings, pinned, cancel) {
        Ok(cache) => Some(cache),
        Err(e) => {
            error!("cache creation failed: {:?}", e);
            None
        }
    }
}

fn tls_config_load(settings: &Settings) -> ConnectionResult<Option<Arc<tls::TlsAcceptor>>> {
    match (&settings.tls_cert, &settings.tls_key) {
        (Some(cert_path), Some(key_path)) => {
            debug!(
                "Loading TLS certificates from {:?} and {:?}",
                cert_path, key_path
            );
            let config = tls::server_tls_config_build(cert_path, key_path).map_err(|e| {
                Report::from(ConnectionError::IoError(std::io::Error::other(format!(
                    "Failed to load TLS certificates: {e}"
                ))))
            })?;
            Ok(Some(Arc::new(tls::TlsAcceptor::from(config))))
        }
        (None, None) => {
            debug!("TLS not configured, accepting plaintext connections only");
            Ok(None)
        }
        _ => Err(ConnectionError::IoError(std::io::Error::other(
            "Both tls_cert and tls_key must be specified together",
        ))
        .into()),
    }
}

/// Load function volatilities from origin database.
///
/// Opens a temporary connection to origin, queries pg_proc for all scalar
/// function volatilities, and returns an immutable shared map.
async fn function_volatility_load(
    settings: &Settings,
) -> ConnectionResult<Arc<HashMap<String, FunctionVolatility>>> {
    let client = connect(&settings.origin, "volatility-load")
        .await
        .map_err(|e| {
            Report::from(ConnectionError::IoError(std::io::Error::other(format!(
                "volatility load connection: {e}"
            ))))
        })?;
    let map = function_volatility_map_load(&client).await.map_err(|e| {
        Report::from(ConnectionError::IoError(std::io::Error::other(format!(
            "volatility map load: {e}"
        ))))
    })?;
    let immutable_count = map
        .iter()
        .filter(|(_, v)| matches!(v, FunctionVolatility::Immutable))
        .count();
    info!(
        "loaded {} function volatilities ({immutable_count} immutable)",
        map.len()
    );

    if tracing::enabled!(tracing::Level::TRACE) {
        let mut names: Vec<&str> = map
            .iter()
            .filter(|(_, v)| matches!(v, FunctionVolatility::Immutable))
            .map(|(k, _)| k.as_str())
            .collect();
        names.sort_unstable();
        trace!("immutable functions: {}", names.join(", "));
    }

    Ok(Arc::new(map))
}

/// Parse and validate pinned queries at startup, returning only those that are cacheable.
fn pinned_queries_validate(
    settings: &Settings,
    func_volatility: &HashMap<String, FunctionVolatility>,
) -> Vec<PinnedQuery> {
    let Some(queries) = &settings.pinned_queries else {
        return Vec::new();
    };

    queries
        .iter()
        .filter_map(|sql| {
            let ast = match pg_query::parse(sql) {
                Ok(ast) => ast,
                Err(e) => {
                    tracing::warn!("pinned query not parseable, skipping: {sql} ({e})");
                    return None;
                }
            };
            let query_expr = match query_expr_convert(&ast) {
                Ok(q) => q,
                Err(e) => {
                    tracing::warn!("pinned query not convertible, skipping: {sql} ({e})");
                    return None;
                }
            };
            let cacheable_query = match CacheableQuery::try_new(&query_expr, func_volatility) {
                Ok(cq) => cq,
                Err(e) => {
                    tracing::warn!("pinned query not cacheable, skipping: {sql} ({e})");
                    return None;
                }
            };
            let fingerprint = query_expr_fingerprint(&query_expr);
            info!("pinned query validated: {sql} (fingerprint: {fingerprint})");
            Some(PinnedQuery {
                fingerprint,
                cacheable_query: Arc::new(cacheable_query),
            })
        })
        .collect()
}

#[tracing::instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub fn proxy_run(settings: &Settings, cancel: CancellationToken) -> ConnectionResult<()> {
    // Load TLS config if certificates are provided
    let tls_acceptor = tls_config_load(settings)?;

    // Pre-scope setup: load function volatilities and validate pinned queries.
    // These must outlive thread::scope so spawned threads can borrow them.
    let pre_rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_into_report::<ConnectionError>()?;
    let _ = pre_rt.block_on(async { replication_provision(settings).await });
    let func_volatility = pre_rt.block_on(function_volatility_load(settings))?;
    let pinned = pinned_queries_validate(settings, &func_volatility);
    drop(pre_rt);

    thread::scope(|scope| {
        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_into_report::<ConnectionError>()?;

        let cache_cancel = cancel.child_token();
        let (cache_handle, cache_tx) = cache_create(scope, settings, &pinned, cache_cancel.clone())
            .map_into_report::<ConnectionError>()
            .attach_loc("creating cache thread")?;
        let (updater, cache_sender) = CacheSenderUpdater::new(cache_tx.clone());
        let mut cache_state = ProxyCacheState::new(cache_handle, updater, cache_tx);

        let resources = WorkerResources {
            cache_sender,
            tls_acceptor,
            func_volatility,
        };

        let mut workers = workers_create_all(scope, settings, resources.clone())?;

        debug!("accept loop");
        rt.block_on(async {
            let listener = TcpListener::bind(&settings.listen.socket)
                .await
                .map_err(|e| {
                    Report::from(ConnectionError::IoError(std::io::Error::other(format!(
                        "bind error [{}] {e}",
                        &settings.listen.socket
                    ))))
                })?;
            info!("Listening to {}", &settings.listen.socket);

            let mut cur_worker = 0;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        info!("proxy shutdown signal received");
                        break;
                    }
                    // Branch 1: Accept new connections
                    result = listener.accept() => {
                        let (socket, _) = result.map_err(|e| {
                            Report::from(ConnectionError::IoError(std::io::Error::other(format!(
                                "accept error: {e}"
                            ))))
                        })?;
                        metrics::counter!(names::CONNECTIONS_TOTAL).increment(1);
                        debug!("socket accepted");

                        if let Some(worker) = workers.get(cur_worker) {
                            let _ = worker.1.send(socket);
                        }

                        worker_ensure_alive(
                            &mut workers,
                            cur_worker,
                            scope,
                            settings,
                            WorkerResources {
                                cache_sender: cache_state.updater.sender_subscribe(),
                                ..resources.clone()
                            },
                        )?;

                        cur_worker = (cur_worker + 1) % settings.num_workers;
                    }

                    // Branch 2: Cache thread exited - initiate restart
                    _ = cache_state.current_tx.closed(), if cache_state.alive => {
                        cache_state.handle_exit();
                    }

                    // Branch 3: Backoff timer expired - retry restart
                    _ = ProxyCacheState::backoff_wait(&mut cache_state.backoff) => {
                        cache_state
                            .handle_backoff_expired(scope, settings, &pinned, cache_cancel.child_token())
                            .await;
                    }
                }
            }

            replication_cleanup(settings)
                .await
                .map_err(|r| r.context_transform(ConnectionError::CdcError))
                .attach_loc("cleaning up replication")?;
            Ok(())
        })
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing)]

    use std::collections::HashMap;

    use crate::catalog::FunctionVolatility;
    use crate::settings::{
        CachePolicy, CdcSettings, ListenSettings, PgSettings, Settings, SslMode,
    };

    use super::pinned_queries_validate;

    fn test_settings(pinned_queries: Option<Vec<String>>) -> Settings {
        let pg = PgSettings {
            host: "localhost".to_owned(),
            port: 5432,
            user: "test".to_owned(),
            password: None,
            database: "test".to_owned(),
            ssl_mode: SslMode::Disable,
        };
        Settings {
            origin: pg.clone(),
            replication: pg.clone(),
            cache: pg,
            cdc: CdcSettings {
                publication_name: "pub".to_owned(),
                slot_name: "slot".to_owned(),
            },
            listen: ListenSettings {
                socket: "127.0.0.1:5432".parse().expect("valid socket"),
            },
            num_workers: 1,
            cache_size: None,
            tls_cert: None,
            tls_key: None,
            metrics: None,
            log_level: None,
            cache_policy: CachePolicy::Clock,
            admission_threshold: 2,
            allowed_tables: None,
            pinned_queries,
        }
    }

    #[test]
    fn pinned_queries_validate_none_returns_empty() {
        let settings = test_settings(None);
        let result = pinned_queries_validate(&settings, &HashMap::new());
        assert!(result.is_empty());
    }

    #[test]
    fn pinned_queries_validate_valid_query_accepted() {
        let settings = test_settings(Some(vec![
            "SELECT id, name FROM users WHERE active = true".to_owned(),
        ]));
        let result = pinned_queries_validate(&settings, &HashMap::new());
        assert_eq!(result.len(), 1);
        assert_ne!(result[0].fingerprint, 0);
    }

    #[test]
    fn pinned_queries_validate_multiple_queries() {
        let settings = test_settings(Some(vec![
            "SELECT * FROM users".to_owned(),
            "SELECT * FROM orders".to_owned(),
        ]));
        let result = pinned_queries_validate(&settings, &HashMap::new());
        assert_eq!(result.len(), 2);
        // Different queries should have different fingerprints
        assert_ne!(result[0].fingerprint, result[1].fingerprint);
    }

    #[test]
    fn pinned_queries_validate_unparseable_skipped() {
        let settings = test_settings(Some(vec!["NOT VALID SQL !!!".to_owned()]));
        let result = pinned_queries_validate(&settings, &HashMap::new());
        assert!(result.is_empty());
    }

    #[test]
    fn pinned_queries_validate_non_cacheable_skipped() {
        // INSERT is not cacheable
        let settings = test_settings(Some(vec!["INSERT INTO users (id) VALUES (1)".to_owned()]));
        let result = pinned_queries_validate(&settings, &HashMap::new());
        assert!(result.is_empty());
    }

    #[test]
    fn pinned_queries_validate_mixed_valid_and_invalid() {
        let settings = test_settings(Some(vec![
            "SELECT * FROM users".to_owned(),
            "NOT VALID SQL".to_owned(),
            "SELECT * FROM orders".to_owned(),
        ]));
        let result = pinned_queries_validate(&settings, &HashMap::new());
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn pinned_queries_validate_non_cacheable_function_in_where_rejected() {
        let mut fv = HashMap::new();
        fv.insert("random".to_owned(), FunctionVolatility::Volatile);

        // Volatile function in WHERE clause makes query non-cacheable
        let settings = test_settings(Some(vec![
            "SELECT id FROM users WHERE random() > 0.5".to_owned(),
        ]));
        let result = pinned_queries_validate(&settings, &fv);
        assert!(result.is_empty());
    }
}
