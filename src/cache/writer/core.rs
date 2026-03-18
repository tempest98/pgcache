use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use tokio::runtime::Builder;
use tokio::sync::mpsc::{Receiver, UnboundedReceiver, UnboundedSender};
use tokio::task::{LocalSet, spawn_local, yield_now};
use tokio_postgres::{Client, Config, NoTls};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, trace};

use crate::cache::status::{
    CacheStatusData, CdcStatusData, LatencyStats, QueryStatusData, StatusRequest, StatusResponse,
};
use crate::catalog::{TableMetadata, aggregate_functions_load};
use crate::metrics::names;
use crate::pg;
use crate::query::ast::Deparse;
use crate::query::resolved::ResolvedSelectNode;
use crate::settings::{CachePolicy, Settings};

use super::super::{
    CacheError, CacheResult, MapIntoReport, ReportExt,
    messages::{CdcCommand, QueryCommand},
    types::{
        ActiveRelations, Cache, CacheStateView, CachedQueryState, CachedQueryView, SharedResolved,
    },
};
use super::population::population_worker;

/// Minimum number of persistent population workers.
const MIN_POPULATE_POOL_SIZE: usize = 2;

/// Minimum number of connections in the cache pool for concurrent CDC updates.
const MIN_CACHE_POOL_SIZE: usize = 2;

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
    pub(super) state_view: Arc<CacheStateView>,
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
    /// Publication name for dynamic table management.
    publication_name: String,
    /// OIDs currently in the publication (mirrors the origin-side state).
    publication_oids: HashSet<u32>,
    /// Set when a removal path changes active relations; drained by command handlers.
    pub(super) relations_dirty: bool,
    /// Writer's own command channel for sending deferred commands (e.g., pinned readmit from CDC).
    pub(super) query_tx: UnboundedSender<QueryCommand>,
}

impl CacheWriter {
    pub async fn new(
        settings: &Settings,
        state_view: Arc<CacheStateView>,
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
        let populate_pool_size = settings.num_workers.max(MIN_POPULATE_POOL_SIZE);
        let mut populate_txs = Vec::with_capacity(populate_pool_size);

        for i in 0..populate_pool_size {
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
        let cache_pool_size = (settings.num_workers / 2).max(MIN_CACHE_POOL_SIZE);
        let mut cache_pool = Vec::with_capacity(cache_pool_size);
        for i in 0..cache_pool_size {
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
            publication_name: settings.cdc.publication_name.clone(),
            publication_oids: HashSet::new(),
            relations_dirty: false,
            query_tx,
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
                subsumption_tx,
                admit_action,
                pinned,
            } => {
                trace!("command query register {fingerprint}");
                let search_path_refs: Vec<&str> = search_path.iter().map(String::as_str).collect();
                if let Err(e) = self
                    .query_register(
                        fingerprint,
                        &cacheable_query,
                        &search_path_refs,
                        started_at,
                        subsumption_tx,
                        admit_action,
                        pinned,
                    )
                    .await
                {
                    error!("query register failed: {e} {fingerprint}");
                }
            }
            QueryCommand::Ready {
                fingerprint,
                cached_bytes,
                row_count,
            } => {
                self.query_ready_mark(fingerprint, cached_bytes, row_count);
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
            QueryCommand::Readmit { fingerprint } => {
                trace!("command readmit {fingerprint}");
                if let Err(e) = self.query_readmit(fingerprint, Instant::now()).await {
                    error!("pinned readmit failed: {e} {fingerprint}");
                }
            }
        }
        self.publication_dirty_drain().await?;
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
        self.publication_dirty_drain().await?;
        self.state_gauges_update();
        Ok(())
    }

    /// Rebuild the shared active relations set from current cached queries.
    /// Returns `true` if the set changed.
    pub(super) fn active_relations_rebuild(&self) -> bool {
        let oids: HashSet<u32> = self
            .cache
            .cached_queries
            .iter()
            .flat_map(|q| q.relation_oids.iter().copied())
            .collect();

        let current = self.active_relations.load();
        if **current == oids {
            return false;
        }
        self.active_relations.store(Arc::new(oids));
        true
    }

    /// Synchronize the origin publication's table list with active relations.
    /// Compares `publication_oids` (current publication state) with the shared
    /// active relations set and issues ALTER PUBLICATION as needed.
    pub(super) async fn publication_update(&mut self) -> CacheResult<()> {
        let new_oids: HashSet<u32> = (**self.active_relations.load()).clone();

        if new_oids == self.publication_oids {
            return Ok(());
        }

        let sql = if new_oids.is_empty() {
            // Drop all tables from the publication
            let table_list =
                self.oids_to_table_list(&self.publication_oids.iter().copied().collect::<Vec<_>>());
            format!(
                "ALTER PUBLICATION {} DROP TABLE {}",
                self.publication_name, table_list
            )
        } else {
            let table_list = self.oids_to_table_list(&new_oids.iter().copied().collect::<Vec<_>>());
            format!(
                "ALTER PUBLICATION {} SET TABLE {}",
                self.publication_name, table_list
            )
        };

        debug!("publication update: {sql}");
        self.db_origin
            .execute(&sql, &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("updating publication table list")?;
        self.publication_oids = new_oids;
        Ok(())
    }

    /// Resolve a list of OIDs to a comma-separated `schema.table` string.
    fn oids_to_table_list(&self, oids: &[u32]) -> String {
        oids.iter()
            .filter_map(|oid| {
                self.cache
                    .tables
                    .get1(oid)
                    .map(|t| format!("{}.{}", t.schema, t.name))
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Drain the dirty flag: rebuild active relations and update the publication if changed.
    async fn publication_dirty_drain(&mut self) -> CacheResult<()> {
        if !self.relations_dirty {
            return Ok(());
        }
        self.relations_dirty = false;
        if self.active_relations_rebuild() {
            self.publication_update().await?;
        }
        Ok(())
    }

    // Helper methods

    pub(super) fn state_view_update(
        &self,
        fingerprint: u64,
        state: CachedQueryState,
        generation: u64,
        resolved: &SharedResolved,
        max_limit: Option<u64>,
    ) {
        self.state_view.cached_queries.insert(
            fingerprint,
            CachedQueryView {
                state,
                generation,
                resolved: Some(Arc::clone(resolved)),
                max_limit,
                referenced: false,
            },
        );
    }

    /// Update cache state gauges with current values.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) fn state_gauges_update(&self) {
        metrics::gauge!(names::CACHE_QUERIES_REGISTERED)
            .set(self.cache.cached_queries.len() as f64);

        {
            let mut loading_count = 0;
            let mut pending_count = 0;
            let mut invalidated_count = 0;

            for entry in self.state_view.cached_queries.iter() {
                match entry.value().state {
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
        let mut pinned_skips = 0;

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
            let query_pinned = query.pinned;

            // Pinned queries are never evicted — always bump to move past them.
            // Unlike CLOCK bumps, pinned bumps are not bounded by MAX_BUMPS.
            if query_pinned {
                trace!("pinned bump {fingerprint}");
                metrics::counter!(names::CACHE_EVICTIONS, "result" => "pinned_bump").increment(1);
                self.cache_query_generation_bump(fingerprint).await?;
                pinned_skips += 1;
                if pinned_skips >= self.cache.cached_queries.len() {
                    break; // all remaining candidates are pinned
                }
                continue;
            }

            // CLOCK second-chance: referenced queries get bumped (bounded by MAX_BUMPS)
            if self.cache.cache_policy == CachePolicy::Clock && bumps < MAX_BUMPS {
                let referenced = self
                    .state_view
                    .cached_queries
                    .get(&fingerprint)
                    .map(|e| e.referenced)
                    .unwrap_or(false);

                if referenced {
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
            pinned_skips = 0;
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
        let resolved = Arc::clone(&query.resolved);

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
        crate::query::ast::Deparse::deparse(&*resolved, &mut sql);
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
        if let Some(mut entry) = self.state_view.cached_queries.get_mut(&fingerprint) {
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
            .filter(|q| q.invalidated && !q.pinned && q.generation < cleanup_threshold)
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
            self.relations_dirty = true;
        }

        // Remove stale Pending and Invalidated entries from state_view
        self.state_view.cached_queries.retain(|_fp, entry| {
            !matches!(
                entry.state,
                CachedQueryState::Pending(_) | CachedQueryState::Invalidated
            ) || entry.generation >= cleanup_threshold
        });

        // Remove metrics for fingerprints no longer in either map
        self.state_view.metrics.retain(|fp, _| {
            self.cache.cached_queries.contains_key1(fp)
                || self.state_view.cached_queries.contains_key(fp)
        });
    }

    /// Promote generation-0 entries to `generation_counter + 1` so they become
    /// purgeable in future cycles. Only bumps the counter if entries were promoted.
    async fn generation_zero_promote(&mut self) -> CacheResult<()> {
        let new_gen = self.cache.generation_counter + 1;
        let promoted: i64 = self
            .db_cache
            .query_one(
                "SELECT pgcache_generation_zero_promote($1)",
                &[&(new_gen as i64)],
            )
            .await
            .map_into_report::<CacheError>()?
            .get(0);

        if promoted > 0 {
            self.cache.generation_counter = new_gen;
            debug!("promoted {promoted} gen-0 entries to generation {new_gen}");
        }

        Ok(())
    }

    /// Build and send a status response for an admin `/status` request.
    async fn status_respond(&self, req: StatusRequest) {
        let cache = &self.cache;

        let cache_status = CacheStatusData {
            size_bytes: cache.current_size,
            size_limit_bytes: cache.cache_size,
            generation: cache.generation_counter,
            tables_tracked: cache.tables.len(),
            policy: format!("{:?}", cache.cache_policy),
        };

        let mut queries: Vec<QueryStatusData> = Vec::with_capacity(cache.cached_queries.len());
        for q in &cache.cached_queries {
            let mut sql_preview = String::with_capacity(128);
            Deparse::deparse(&*q.resolved, &mut sql_preview);
            sql_preview.truncate(200);

            let tables: Vec<String> = q
                .relation_oids
                .iter()
                .filter_map(|oid| {
                    cache
                        .tables
                        .get1(oid)
                        .map(|t| format!("{}.{}", t.schema, t.name))
                })
                .collect();

            let state = self
                .state_view
                .cached_queries
                .get(&q.fingerprint)
                .map(|entry| format!("{:?}", entry.value().state))
                .unwrap_or_else(|| "Unknown".to_owned());

            // Look up per-query metrics (shared read access)
            let metrics = self.state_view.metrics.get(&q.fingerprint);
            let now_ns = self.state_view.started_at.elapsed().as_nanos() as u64;
            let (
                hit_count,
                miss_count,
                idle_duration_ms,
                registered_duration_ms,
                cached_duration_ms,
                invalidation_count,
                readmission_count,
                eviction_count,
                subsumption_count,
                population_count,
                last_population_duration_ms,
                total_bytes_served,
                population_row_count,
                cache_hit_latency,
            ) = match &metrics {
                Some(m) => {
                    let latency_stats = if !m.cache_hit_latency.is_empty() {
                        Some(LatencyStats {
                            count: m.cache_hit_latency.len(),
                            mean_us: m.cache_hit_latency.mean(),
                            p50_us: m.cache_hit_latency.value_at_quantile(0.5),
                            p95_us: m.cache_hit_latency.value_at_quantile(0.95),
                            p99_us: m.cache_hit_latency.value_at_quantile(0.99),
                            min_us: m.cache_hit_latency.min(),
                            max_us: m.cache_hit_latency.max(),
                        })
                    } else {
                        None
                    };

                    (
                        m.hit_count,
                        m.miss_count,
                        m.last_hit_at_ns
                            .map(|ns| now_ns.saturating_sub(ns.get()) / 1_000_000),
                        m.registered_at_ns
                            .map(|ns| now_ns.saturating_sub(ns.get()) / 1_000_000),
                        m.cached_since_ns
                            .map(|ns| now_ns.saturating_sub(ns.get()) / 1_000_000),
                        m.invalidation_count,
                        m.readmission_count,
                        m.eviction_count,
                        m.subsumption_count,
                        m.population_count,
                        m.last_population_duration_us.map(|us| us.get() / 1_000),
                        m.total_bytes_served,
                        m.population_row_count,
                        latency_stats,
                    )
                }
                None => (0, 0, None, None, None, 0, 0, 0, 0, 0, None, 0, 0, None),
            };

            queries.push(QueryStatusData {
                fingerprint: q.fingerprint,
                sql_preview,
                tables,
                state,
                cached_bytes: q.cached_bytes,
                max_limit: q.max_limit,
                pinned: q.pinned,
                hit_count,
                miss_count,
                idle_duration_ms,
                registered_duration_ms,
                cached_duration_ms,
                invalidation_count,
                readmission_count,
                eviction_count,
                subsumption_count,
                population_count,
                last_population_duration_ms,
                total_bytes_served,
                population_row_count,
                cache_hit_latency,
            });

            yield_now().await;
        }

        let response = StatusResponse {
            cache: cache_status,
            cdc: CdcStatusData::default(),
            queries,
        };

        let _ = req.reply_tx.send(response);
    }

    /// Purge rows with generation <= threshold.
    /// First promotes any gen-0 entries so they become purgeable in future cycles.
    pub(super) async fn generation_purge(&mut self, threshold: u64) -> CacheResult<i64> {
        self.generation_zero_promote().await?;

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
    state_view: Arc<CacheStateView>,
    active_relations: ActiveRelations,
    cancel: CancellationToken,
    mut status_rx: Receiver<StatusRequest>,
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
                        _ = cancel.cancelled() => {
                            debug!("writer shutdown signal received");
                            break;
                        }
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
                        // Handle status requests from admin HTTP server
                        msg = status_rx.recv() => {
                            if let Some(req) = msg {
                                writer.status_respond(req).await;
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
