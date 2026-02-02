use std::rc::Rc;
use std::sync::{Arc, RwLock};

use tokio::runtime::Builder;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::{LocalSet, spawn_local};
use tokio_postgres::{Client, Config, NoTls};
use tracing::{debug, error, trace};

use crate::catalog::TableMetadata;
use crate::metrics::names;
use crate::pg;
use crate::query::resolved::ResolvedSelectNode;
use crate::settings::Settings;

use super::super::{
    CacheError, CacheResult, MapIntoReport, ReportExt,
    messages::WriterCommand,
    types::{Cache, CacheStateView, CachedQueryState, CachedQueryView},
};
use super::population::population_worker;

/// Number of persistent population workers.
pub const POPULATE_POOL_SIZE: usize = 3;

/// Number of connections in the cache pool for concurrent CDC updates.
const CACHE_POOL_SIZE: usize = 4;

/// Work item for population worker pool.
pub struct PopulationWork {
    pub fingerprint: u64,
    pub generation: u64,
    pub relation_oids: Vec<u32>,
    pub table_metadata: Vec<TableMetadata>,
    pub resolved: ResolvedSelectNode,
}

/// Cache writer that owns the Cache and serializes all mutations.
/// This ensures no race conditions between query registration and purging.
pub struct CacheWriter {
    pub(super) cache: Cache,
    pub(super) db_cache: Client,
    pub(super) db_origin: Rc<Client>,
    pub(super) state_view: Arc<RwLock<CacheStateView>>,
    /// Channels to persistent population workers (round-robin dispatch).
    pub(super) populate_txs: Vec<UnboundedSender<PopulationWork>>,
    /// Index for round-robin dispatch to population workers.
    pub(super) populate_next: usize,
    /// Pool of cache connections for concurrent CDC update execution.
    pub(super) cache_pool: Vec<Rc<Client>>,
}

impl CacheWriter {
    pub async fn new(
        settings: &Settings,
        state_view: Arc<RwLock<CacheStateView>>,
        writer_tx: UnboundedSender<WriterCommand>,
    ) -> CacheResult<Self> {
        let (cache_client, cache_connection) = Config::new()
            .host(&settings.cache.host)
            .port(settings.cache.port)
            .user(&settings.cache.user)
            .dbname(&settings.cache.database)
            .connect(NoTls)
            .await
            .map_into_report::<CacheError>()?;

        tokio::spawn(async move {
            if let Err(e) = cache_connection.await {
                error!("writer cache connection error: {e}");
            }
        });

        let origin_client = pg::connect(&settings.origin, "writer origin")
            .await
            .map_into_report::<CacheError>()
            .attach_loc("connecting to origin database")?;

        let db_origin = Rc::new(origin_client);

        // Spawn persistent population workers (each with its own cache connection)
        let mut populate_txs = Vec::with_capacity(POPULATE_POOL_SIZE);

        for i in 0..POPULATE_POOL_SIZE {
            let (cache_conn, cache_conn_task) = Config::new()
                .host(&settings.cache.host)
                .port(settings.cache.port)
                .user(&settings.cache.user)
                .dbname(&settings.cache.database)
                .connect(NoTls)
                .await
                .map_into_report::<CacheError>()?;

            tokio::spawn(async move {
                if let Err(e) = cache_conn_task.await {
                    error!("population worker {i} connection error: {e}");
                }
            });

            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            populate_txs.push(tx);

            let worker_db_origin = Rc::clone(&db_origin);
            let worker_writer_tx = writer_tx.clone();

            spawn_local(async move {
                population_worker(i, rx, worker_db_origin, cache_conn, worker_writer_tx).await;
            });
        }

        // Create cache connection pool for concurrent CDC updates
        let mut cache_pool = Vec::with_capacity(CACHE_POOL_SIZE);
        for i in 0..CACHE_POOL_SIZE {
            let (pool_conn, pool_conn_task) = Config::new()
                .host(&settings.cache.host)
                .port(settings.cache.port)
                .user(&settings.cache.user)
                .dbname(&settings.cache.database)
                .connect(NoTls)
                .await
                .map_into_report::<CacheError>()?;

            tokio::spawn(async move {
                if let Err(e) = pool_conn_task.await {
                    error!("cache pool connection {i} error: {e}");
                }
            });

            cache_pool.push(Rc::new(pool_conn));
        }

        Ok(Self {
            cache: Cache::new(settings),
            db_cache: cache_client,
            db_origin,
            state_view,
            populate_txs,
            populate_next: 0,
            cache_pool,
        })
    }

    /// Handle a writer command, dispatching to the appropriate method.
    pub async fn command_handle(&mut self, cmd: WriterCommand) -> CacheResult<()> {
        match cmd {
            WriterCommand::QueryRegister {
                fingerprint,
                cacheable_query,
                search_path,
                started_at,
            } => {
                let search_path_refs: Vec<&str> = search_path.iter().map(String::as_str).collect();
                if let Err(e) = self
                    .query_register(fingerprint, &cacheable_query, &search_path_refs, started_at)
                    .await
                {
                    error!("query register failed: {e}");
                }
                self.state_gauges_update();
            }
            WriterCommand::QueryReady {
                fingerprint,
                cached_bytes,
            } => {
                self.query_ready_mark(fingerprint, cached_bytes);
                self.cache.current_size = self.cache_size_load().await?;

                while self
                    .cache
                    .cache_size
                    .is_some_and(|s| self.cache.current_size > s)
                {
                    if let Some(min_gen) = self.cache.generations.first()
                        && let Some(query) = self.cache.cached_queries.get2(min_gen)
                    {
                        trace!("exceeded cache size, evicting query");
                        metrics::counter!(names::CACHE_EVICTIONS).increment(1);
                        self.cache_query_invalidate(query.fingerprint).await?;
                    }
                }
                self.state_gauges_update();
            }
            WriterCommand::QueryFailed { fingerprint } => {
                self.query_failed_cleanup(fingerprint);
            }
            WriterCommand::TableRegister(table_metadata) => {
                if let Err(e) = self.cache_table_register(table_metadata).await {
                    error!("table register failed: {e}");
                }
                self.state_gauges_update();
            }
            WriterCommand::CdcInsert {
                relation_oid,
                row_data,
            } => {
                if let Err(e) = self.handle_insert(relation_oid, row_data).await {
                    error!("cdc insert failed: {e}");
                }
            }
            WriterCommand::CdcUpdate {
                relation_oid,
                key_data,
                row_data,
            } => {
                if let Err(e) = self.handle_update(relation_oid, key_data, row_data).await {
                    error!("cdc update failed: {e}");
                }
            }
            WriterCommand::CdcDelete {
                relation_oid,
                row_data,
            } => {
                if let Err(e) = self.handle_delete(relation_oid, row_data).await {
                    error!("cdc delete failed: {e}");
                }
            }
            WriterCommand::CdcTruncate { relation_oids } => {
                if let Err(e) = self.handle_truncate(&relation_oids).await {
                    error!("cdc truncate failed: {e}");
                }
            }
            WriterCommand::RelationCheck {
                relation_oid,
                response_tx,
            } => {
                let exists = self.cached_queries_exist(relation_oid);
                let _ = response_tx.send(exists);
            }
        }
        Ok(())
    }

    /// Check if there are any cached queries for a specific table.
    pub fn cached_queries_exist(&self, relation_oid: u32) -> bool {
        self.cache
            .cached_queries
            .iter()
            .any(|query| query.relation_oids.contains(&relation_oid))
    }

    // Helper methods

    pub(super) fn state_view_update(
        &self,
        fingerprint: u64,
        state: CachedQueryState,
        generation: u64,
        resolved: &ResolvedSelectNode,
    ) {
        if let Ok(mut view) = self.state_view.write() {
            view.cached_queries.insert(
                fingerprint,
                CachedQueryView {
                    state,
                    generation,
                    resolved: Some(resolved.clone()),
                },
            );
        }
    }

    /// Update cache state gauges with current values.
    pub(super) fn state_gauges_update(&self) {
        metrics::gauge!(names::CACHE_QUERIES_REGISTERED)
            .set(self.cache.cached_queries.len() as f64);
        let loading_count = self
            .cache
            .cached_queries
            .iter()
            .filter(|q| q.state == CachedQueryState::Loading)
            .count();
        metrics::gauge!(names::CACHE_QUERIES_LOADING).set(loading_count as f64);
        metrics::gauge!(names::CACHE_SIZE_BYTES).set(self.cache.current_size as f64);
        if let Some(limit) = self.cache.cache_size {
            metrics::gauge!(names::CACHE_SIZE_LIMIT_BYTES).set(limit as f64);
        }
        metrics::gauge!(names::CACHE_GENERATION).set(self.cache.generation_counter as f64);
        metrics::gauge!(names::CACHE_TABLES_TRACKED).set(self.cache.tables.len() as f64);
    }

    /// Utility function to get the size of the currently cached data
    pub(super) async fn cache_size_load(&mut self) -> CacheResult<usize> {
        let size: i64 = self
            .db_cache
            .query_one("SELECT pgcache_total_size()", &[])
            .await
            .map_into_report::<CacheError>()?
            .get(0);

        Ok(size as usize)
    }

    /// Purge rows with generation <= threshold.
    pub(super) async fn generation_purge(&mut self, threshold: u64) -> CacheResult<i64> {
        if threshold > 0 {
            let deleted: i64 = self
                .db_cache
                .query_one("SELECT pgcache_purge_rows($1)", &[&(threshold as i64)])
                .await
                .map_into_report::<CacheError>()?
                .get(0);
            trace!("purged generations <= {threshold}: rows removed [{deleted}]");
            Ok(deleted)
        } else {
            Ok(0)
        }
    }
}

const BATCH_SIZE_MAX: usize = 100;

/// Main writer runtime - processes writer commands with LocalSet for spawned tasks.
pub fn writer_run(
    settings: &Settings,
    mut writer_rx: UnboundedReceiver<WriterCommand>,
    state_view: Arc<RwLock<CacheStateView>>,
) -> CacheResult<()> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_into_report::<CacheError>()?;

    debug!("writer loop");
    rt.block_on(async {
        // Create writer channel for spawned tasks to send commands back
        let (writer_tx, mut internal_rx) = tokio::sync::mpsc::unbounded_channel();

        LocalSet::new()
            .run_until(async move {
                // Create writer inside LocalSet so spawn_local works for population workers
                let mut writer = CacheWriter::new(settings, state_view, writer_tx).await?;

                loop {
                    tokio::select! {
                        // Handle commands from coordinator/CDC
                        msg = writer_rx.recv() => {
                            match msg {
                                Some(cmd) => {
                                    if let Err(e) = writer.command_handle(cmd).await {
                                        error!("writer command failed: {e}");
                                    }
                                }
                                None => {
                                    debug!("writer channel closed, shutting down");
                                    break;
                                }
                            }
                        }
                        // Handle commands from spawned population tasks
                        msg = internal_rx.recv() => {
                            match msg {
                                Some(cmd) => {
                                    if let Err(e) = writer.command_handle(cmd).await {
                                        error!("writer internal command failed: {e}");
                                    }
                                }
                                None => {
                                    debug!("writer internal channel closed, shutting down");
                                    break;
                                }
                            }
                        }
                    }

                    for _ in 0..writer_rx.len().max(BATCH_SIZE_MAX) {
                        if let Ok(cmd) = writer_rx.try_recv() {
                            if let Err(e) = writer.command_handle(cmd).await {
                                error!("writer command failed: {e}");
                            }
                        } else {
                            break;
                        }
                        metrics::gauge!(names::CACHE_WRITER_QUEUE).set(writer_rx.len() as f64);
                    }

                    for _ in 0..internal_rx.len().max(BATCH_SIZE_MAX) {
                        if let Ok(cmd) = internal_rx.try_recv() {
                            if let Err(e) = writer.command_handle(cmd).await {
                                error!("writer internal command failed: {e}");
                            }
                        } else {
                            break;
                        }
                        metrics::gauge!(names::CACHE_WRITER_INTERNAL_QUEUE)
                            .set(internal_rx.len() as f64);
                    }
                }
                Ok(())
            })
            .await
    })
}
