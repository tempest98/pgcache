use std::collections::HashSet;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

use tokio::runtime::Builder;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::{LocalSet, spawn_local};
use tokio_postgres::{Client, Config, NoTls};
use tracing::{debug, error, trace};

use crate::catalog::{TableMetadata, aggregate_functions_load};
use crate::metrics::names;
use crate::pg;
use crate::query::resolved::{ResolvedQueryExpr, ResolvedSelectNode};
use crate::settings::{CachePolicy, Settings};

use super::super::{
    CacheError, CacheResult, MapIntoReport, ReportExt,
    messages::{CdcCommand, QueryCommand},
    types::{ActiveRelations, Cache, CacheStateView, CachedQueryState, CachedQueryView},
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
    pub table_metadata: Vec<TableMetadata>,
    /// SELECT branches extracted from the query at registration time.
    /// For simple SELECT queries, this contains one branch.
    /// For set operations (UNION/INTERSECT/EXCEPT), contains all branches.
    pub branches: Vec<ResolvedSelectNode>,
    /// Maximum rows to fetch during population. `None` = fetch all rows.
    pub max_limit: Option<u64>,
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
    /// Shared set of relation OIDs with active cached queries (read by CDC processor).
    active_relations: ActiveRelations,
    /// Aggregate function names from pg_proc, used for scalar subquery decorrelation.
    pub(super) aggregate_functions: HashSet<String>,
}

impl CacheWriter {
    pub async fn new(
        settings: &Settings,
        state_view: Arc<RwLock<CacheStateView>>,
        active_relations: ActiveRelations,
        query_tx: UnboundedSender<QueryCommand>,
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

        let aggregate_functions = aggregate_functions_load(&db_origin)
            .await
            .map_into_report::<CacheError>()
            .attach_loc("loading aggregate functions")?;

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
            let worker_query_tx = query_tx.clone();

            spawn_local(async move {
                population_worker(i, rx, worker_db_origin, cache_conn, worker_query_tx).await;
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
            active_relations,
            aggregate_functions,
        })
    }

    /// Handle a query command, dispatching to the appropriate method.
    pub async fn query_command_handle(&mut self, cmd: QueryCommand) -> CacheResult<()> {
        match cmd {
            QueryCommand::Register {
                fingerprint,
                cacheable_query,
                search_path,
                started_at,
            } => {
                trace!("command query register {fingerprint}");
                let search_path_refs: Vec<&str> = search_path.iter().map(String::as_str).collect();
                if let Err(e) = self
                    .query_register(fingerprint, &cacheable_query, &search_path_refs, started_at)
                    .await
                {
                    error!("query register failed: {e} {fingerprint}");
                }
            }
            QueryCommand::Ready {
                fingerprint,
                cached_bytes,
            } => {
                self.query_ready_mark(fingerprint, cached_bytes);
                self.cache.current_size = self.cache_size_load().await?;
                self.eviction_run().await?;
            }
            QueryCommand::Failed { fingerprint } => {
                self.query_failed_cleanup(fingerprint);
            }
            QueryCommand::LimitBump {
                fingerprint,
                max_limit,
            } => {
                trace!("command limit bump {fingerprint} max_limit={max_limit:?}");
                if let Err(e) = self.limit_bump_handle(fingerprint, max_limit).await {
                    error!("limit bump failed: {e} {fingerprint}");
                }
            }
        }
        self.state_gauges_update();
        Ok(())
    }

    /// Handle a CDC command, dispatching to the appropriate method.
    pub async fn cdc_command_handle(&mut self, cmd: CdcCommand) -> CacheResult<()> {
        match cmd {
            CdcCommand::TableRegister(table_metadata) => {
                if let Err(e) = self.cache_table_register(table_metadata).await {
                    error!("table register failed: {e}");
                }
            }
            CdcCommand::Insert {
                relation_oid,
                row_data,
            } => {
                if let Err(e) = self.handle_insert(relation_oid, row_data).await {
                    error!("cdc insert failed: {e}");
                }
            }
            CdcCommand::Update {
                relation_oid,
                key_data,
                row_data,
            } => {
                if let Err(e) = self.handle_update(relation_oid, key_data, row_data).await {
                    error!("cdc update failed: {e}");
                }
            }
            CdcCommand::Delete {
                relation_oid,
                row_data,
            } => {
                if let Err(e) = self.handle_delete(relation_oid, row_data).await {
                    error!("cdc delete failed: {e}");
                }
            }
            CdcCommand::Truncate { relation_oids } => {
                if let Err(e) = self.handle_truncate(&relation_oids).await {
                    error!("cdc truncate failed: {e}");
                }
            }
        }
        self.state_gauges_update();
        Ok(())
    }

    /// Rebuild the shared active relations set from current cached queries.
    pub(super) fn active_relations_rebuild(&self) {
        let oids: HashSet<u32> = self
            .cache
            .cached_queries
            .iter()
            .flat_map(|q| q.relation_oids.iter().copied())
            .collect();

        if let Ok(mut set) = self.active_relations.write() {
            *set = oids;
        }
    }

    // Helper methods

    pub(super) fn state_view_update(
        &self,
        fingerprint: u64,
        state: CachedQueryState,
        generation: u64,
        resolved: &ResolvedQueryExpr,
        max_limit: Option<u64>,
    ) {
        if let Ok(mut view) = self.state_view.write() {
            view.cached_queries.insert(
                fingerprint,
                CachedQueryView {
                    state,
                    generation,
                    resolved: Some(resolved.clone()),
                    max_limit,
                    referenced: false,
                },
            );
        }
    }

    /// Update cache state gauges with current values.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) fn state_gauges_update(&self) {
        metrics::gauge!(names::CACHE_QUERIES_REGISTERED)
            .set(self.cache.cached_queries.len() as f64);

        if let Ok(view) = self.state_view.read() {
            let mut loading_count = 0;
            let mut pending_count = 0;
            let mut invalidated_count = 0;

            for q in view.cached_queries.values() {
                match q.state {
                    CachedQueryState::Loading => loading_count += 1,
                    CachedQueryState::Pending(_) => pending_count += 1,
                    CachedQueryState::Invalidated => invalidated_count += 1,
                    CachedQueryState::Ready => {}
                }
            }

            metrics::gauge!(names::CACHE_QUERIES_LOADING).set(loading_count as f64);
            metrics::gauge!(names::CACHE_QUERIES_PENDING).set(pending_count as f64);
            metrics::gauge!(names::CACHE_QUERIES_INVALIDATED).set(invalidated_count as f64);
        }

        metrics::gauge!(names::CACHE_SIZE_BYTES).set(self.cache.current_size as f64);
        if let Some(limit) = self.cache.cache_size {
            metrics::gauge!(names::CACHE_SIZE_LIMIT_BYTES).set(limit as f64);
        }
        metrics::gauge!(names::CACHE_GENERATION).set(self.cache.generation_counter as f64);
        metrics::gauge!(names::CACHE_TABLES_TRACKED).set(self.cache.tables.len() as f64);
    }

    /// Utility function to get the size of the currently cached data
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn cache_size_load(&mut self) -> CacheResult<usize> {
        let size: i64 = self
            .db_cache
            .query_one("SELECT pgcache_total_size()", &[])
            .await
            .map_into_report::<CacheError>()?
            .get(0);

        Ok(size as usize)
    }

    /// Run eviction loop. For CLOCK policy, uses second-chance algorithm with reference bit.
    /// For FIFO policy, evicts the oldest-registered query.
    async fn eviction_run(&mut self) -> CacheResult<()> {
        /// Maximum number of generation bumps (second chances) per eviction round.
        /// Bounds re-stamping work and prevents pathological case where all queries are referenced.
        const MAX_BUMPS: usize = 5;
        let mut bumps = 0;

        while self
            .cache
            .cache_size
            .is_some_and(|s| self.cache.current_size > s)
        {
            let Some(&min_gen) = self.cache.generations.first() else {
                break;
            };
            let Some(query) = self.cache.cached_queries.get2(&min_gen) else {
                break;
            };
            let fingerprint = query.fingerprint;

            // For CLOCK policy, check reference bit before evicting
            if self.cache.cache_policy == CachePolicy::Clock && bumps < MAX_BUMPS {
                let referenced = self
                    .state_view
                    .read()
                    .ok()
                    .and_then(|view| view.cached_queries.get(&fingerprint).map(|e| e.referenced))
                    .unwrap_or(false);

                if referenced {
                    // Second chance: bump generation to move to back of eviction order
                    trace!("clock bump {fingerprint}");
                    metrics::counter!(names::CACHE_EVICTIONS, "result" => "bump").increment(1);
                    self.cache_query_generation_bump(fingerprint).await?;
                    bumps += 1;
                    continue;
                }
            }

            // Evict (full removal)
            trace!("evicting query {fingerprint}");
            metrics::counter!(names::CACHE_EVICTIONS).increment(1);
            self.cache_query_evict(fingerprint).await?;
            bumps = 0;
        }

        self.stale_entries_cleanup();
        Ok(())
    }

    /// Bump a cached query's generation to give it a second chance in CLOCK eviction.
    /// Re-executes the query against cache DB so the CustomScan tracker re-stamps
    /// dshash entries from old_gen to new_gen.
    async fn cache_query_generation_bump(&mut self, fingerprint: u64) -> CacheResult<()> {
        let Some(query) = self.cache.cached_queries.get1(&fingerprint) else {
            return Ok(());
        };

        let old_generation = query.generation;
        let resolved = query.resolved.clone();

        // 1. Assign new generation (insert before removing old — keeps old gen valid for re-stamp)
        self.cache.generation_counter += 1;
        let new_generation = self.cache.generation_counter;
        self.cache.generations.insert(new_generation);

        // 2. Set query generation on cache DB connection for row tracking
        let set_gen_sql = format!("SET mem.query_generation = {new_generation}");
        self.db_cache
            .execute(&set_gen_sql, &[])
            .await
            .map_into_report::<CacheError>()?;

        // 3. Re-execute query against cache DB (discard results).
        //    The CustomScan tracker side-effect updates dshash from old_gen to new_gen.
        let mut sql = String::with_capacity(512);
        crate::query::ast::Deparse::deparse(&resolved, &mut sql);
        let _ = self
            .db_cache
            .query(&sql, &[])
            .await
            .map_into_report::<CacheError>()?;

        // 4. Reset query generation
        self.db_cache
            .execute("SET mem.query_generation = 0", &[])
            .await
            .map_into_report::<CacheError>()?;

        // 5. Now safe to remove old generation (rows are re-stamped)
        self.cache.generations.remove(&old_generation);

        // 6. Update CachedQuery in BiHashMap (generation is key2, must remove/reinsert)
        if let Some(mut cached) = self.cache.cached_queries.remove1(&fingerprint) {
            cached.generation = new_generation;
            self.cache.cached_queries.insert_overwrite(cached);
        }

        // 7. Clear reference bit and update generation in state_view
        if let Ok(mut view) = self.state_view.write()
            && let Some(entry) = view.cached_queries.get_mut(&fingerprint)
        {
            entry.referenced = false;
            entry.generation = new_generation;
        }

        Ok(())
    }

    /// Clean up stale Pending and Invalidated entries from state_view.
    /// Pending entries that haven't been promoted and Invalidated entries whose
    /// generation is below the cleanup threshold are removed.
    fn stale_entries_cleanup(&mut self) {
        let cleanup_threshold = self.cache.generation_purge_threshold();

        // Remove invalidated entries from cached_queries that are below threshold
        let stale_fingerprints: Vec<u64> = self
            .cache
            .cached_queries
            .iter()
            .filter(|q| q.invalidated && q.generation < cleanup_threshold)
            .map(|q| q.fingerprint)
            .collect();

        for fp in &stale_fingerprints {
            if let Some(query) = self.cache.cached_queries.remove1(fp) {
                // Remove update queries
                for oid in &query.relation_oids {
                    if let Some(mut queries) = self.cache.update_queries.get_mut(oid) {
                        queries.queries.retain(|q| q.fingerprint != *fp);
                    }
                }
            }
        }

        if !stale_fingerprints.is_empty() {
            self.active_relations_rebuild();
        }

        // Remove stale Pending and Invalidated entries from state_view
        if let Ok(mut view) = self.state_view.write() {
            view.cached_queries.retain(|_fp, entry| {
                !matches!(
                    entry.state,
                    CachedQueryState::Pending(_) | CachedQueryState::Invalidated
                ) || entry.generation >= cleanup_threshold
            });
        }
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

/// Main writer runtime - processes query and CDC commands with LocalSet for spawned tasks.
pub fn writer_run(
    settings: &Settings,
    mut query_rx: UnboundedReceiver<QueryCommand>,
    mut cdc_rx: UnboundedReceiver<CdcCommand>,
    state_view: Arc<RwLock<CacheStateView>>,
    active_relations: ActiveRelations,
) -> CacheResult<()> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_into_report::<CacheError>()?;

    debug!("writer loop");
    rt.block_on(async {
        // Create internal channel for population workers to send query commands back
        let (query_tx, mut internal_rx) = tokio::sync::mpsc::unbounded_channel();

        LocalSet::new()
            .run_until(async move {
                // Create writer inside LocalSet so spawn_local works for population workers
                let mut writer =
                    CacheWriter::new(settings, state_view, active_relations, query_tx).await?;

                loop {
                    tokio::select! {
                        // Handle query commands from coordinator
                        msg = query_rx.recv() => {
                            match msg {
                                Some(cmd) => {
                                    if let Err(e) = writer.query_command_handle(cmd).await {
                                        error!("writer query command failed: {e}");
                                    }
                                }
                                None => {
                                    debug!("writer query channel closed, shutting down");
                                    break;
                                }
                            }
                        }
                        // Handle CDC commands from coordinator
                        msg = cdc_rx.recv() => {
                            match msg {
                                Some(cmd) => {
                                    if let Err(e) = writer.cdc_command_handle(cmd).await {
                                        error!("writer cdc command failed: {e}");
                                    }
                                }
                                None => {
                                    debug!("writer cdc channel closed, shutting down");
                                    break;
                                }
                            }
                        }
                        // Handle commands from spawned population tasks
                        msg = internal_rx.recv() => {
                            match msg {
                                Some(cmd) => {
                                    if let Err(e) = writer.query_command_handle(cmd).await {
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

                    metrics::gauge!(names::CACHE_WRITER_QUERY_QUEUE).set(query_rx.len() as f64);
                    metrics::gauge!(names::CACHE_WRITER_CDC_QUEUE).set(cdc_rx.len() as f64);
                    metrics::gauge!(names::CACHE_WRITER_INTERNAL_QUEUE)
                        .set(internal_rx.len() as f64);
                }

                Ok(())
            })
            .await
    })
}
