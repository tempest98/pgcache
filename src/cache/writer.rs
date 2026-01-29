use std::rc::Rc;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use iddqd::BiHashMap;
use postgres_protocol::escape;
use rootcause::Report;
use tokio::runtime::Builder;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::{LocalSet, spawn_local};
use tokio_postgres::Row;
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage, types::Type};
use tracing::{debug, error, info, instrument, trace};

use crate::catalog::{ColumnMetadata, IndexMetadata, TableMetadata, cache_type_name_resolve};
use crate::metrics::names;
use crate::pg;
use crate::query::ast::{Deparse, TableNode};
use crate::query::constraints::analyze_query_constraints;
use crate::query::resolved::{
    ResolvedSelectStatement, ResolvedTableNode, select_statement_resolve,
};
use crate::query::transform::{query_table_update_queries, resolved_table_replace_with_values};
use crate::settings::Settings;

use super::{
    CacheError, CacheResult, MapIntoReport, ReportExt,
    messages::WriterCommand,
    query::CacheableQuery,
    types::{
        Cache, CacheStateView, CachedQuery, CachedQueryState, CachedQueryView, UpdateQueries,
        UpdateQuery,
    },
};

/// Number of persistent population workers.
const POPULATE_POOL_SIZE: usize = 3;

/// Work item for population worker pool.
struct PopulationWork {
    fingerprint: u64,
    generation: u64,
    relation_oids: Vec<u32>,
    table_metadata: Vec<TableMetadata>,
    resolved: ResolvedSelectStatement,
}

/// Cache writer that owns the Cache and serializes all mutations.
/// This ensures no race conditions between query registration and purging.
pub struct CacheWriter {
    cache: Cache,
    db_cache: Client,
    db_origin: Rc<Client>,
    state_view: Arc<RwLock<CacheStateView>>,
    /// Channels to persistent population workers (round-robin dispatch).
    populate_txs: Vec<UnboundedSender<PopulationWork>>,
    /// Index for round-robin dispatch to population workers.
    populate_next: usize,
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

        Ok(Self {
            cache: Cache::new(settings),
            db_cache: cache_client,
            db_origin,
            state_view,
            populate_txs,
            populate_next: 0,
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

    /// Registers a query in the cache and spawns background population.
    /// Registration is synchronous (updates Cache state), population is async.
    #[instrument(skip_all)]
    pub async fn query_register(
        &mut self,
        fingerprint: u64,
        cacheable_query: &CacheableQuery,
        search_path: &[&str],
        started_at: Instant,
    ) -> CacheResult<()> {
        let select_statement = cacheable_query.statement();
        let mut relation_oids = Vec::new();

        // Ensure all tables are registered in the cache
        for table_node in select_statement.nodes::<TableNode>() {
            let table_name = table_node.name.as_str();

            let schema = if let Some(schema) = table_node.schema.as_deref() {
                schema.to_owned()
            } else {
                self.schema_for_table_find(table_name, search_path).await?
            };

            if !self
                .cache
                .tables
                .contains_key2(&(schema.as_str(), table_name))
            {
                let table = self.cache_table_create(Some(&schema), table_name).await?;
                self.cache.tables.insert_overwrite(table);
            }
        }

        // Resolve the query using catalog metadata
        let resolved = select_statement_resolve(select_statement, &self.cache.tables, search_path)
            .map_err(|e| Report::from(CacheError::from(e.into_current_context())))
            .attach_loc("resolving select statement")?;

        // Analyze constraints from the resolved query
        let query_constraints = analyze_query_constraints(&resolved);

        for (table_node, update_select) in query_table_update_queries(cacheable_query) {
            let schema = if let Some(schema) = table_node.schema.as_deref() {
                schema.to_owned()
            } else {
                self.schema_for_table_find(table_node.name.as_str(), search_path)
                    .await?
            };
            let relation_oid = self
                .cache
                .tables
                .get2(&(schema.as_str(), table_node.name.as_str()))
                .ok_or(CacheError::UnknownTable {
                    oid: None,
                    name: Some(table_node.name.clone()),
                })?
                .relation_oid;

            let update_resolved =
                select_statement_resolve(&update_select, &self.cache.tables, search_path)
                    .map_err(|e| Report::from(CacheError::from(e.into_current_context())))
                    .attach_loc("resolving update select statement")?;

            let update_query = UpdateQuery {
                fingerprint,
                resolved: update_resolved,
            };

            self.cache
                .update_queries
                .entry(relation_oid)
                .and_modify(|mut queries| queries.queries.push(update_query.clone()))
                .or_insert_with(|| UpdateQueries {
                    relation_oid,
                    queries: vec![update_query],
                });

            relation_oids.push(relation_oid);
        }

        // Assign generation number synchronously (before any await)
        self.cache.generation_counter += 1;
        let generation = self.cache.generation_counter;
        self.cache.generations.insert(generation);

        // Create CachedQuery entry with Loading state
        let cached_query = CachedQuery {
            state: CachedQueryState::Loading,
            fingerprint,
            generation,
            relation_oids: relation_oids.clone(),
            select_statement: select_statement.clone(),
            resolved: resolved.clone(),
            constraints: query_constraints,
            cached_bytes: 0,
            registration_started_at: Some(started_at),
        };

        self.cache.cached_queries.insert_overwrite(cached_query);
        trace!("cached query loading");

        // Update shared state view with Loading state
        self.state_view_update(
            fingerprint,
            CachedQueryState::Loading,
            generation,
            &resolved,
        );

        // Collect table metadata needed for population
        let table_metadata: Vec<TableMetadata> = relation_oids
            .iter()
            .filter_map(|oid| self.cache.tables.get1(oid).cloned())
            .collect();

        // Create work item for population worker
        let work = PopulationWork {
            fingerprint,
            generation,
            relation_oids,
            table_metadata,
            resolved,
        };

        // Round-robin dispatch to population workers
        let idx = self.populate_next;
        self.populate_next = (self.populate_next + 1) % POPULATE_POOL_SIZE;

        if self.populate_txs[idx].send(work).is_err() {
            error!("population worker {idx} channel closed");
        }

        trace!("population work queued for query {fingerprint} to worker {idx}");
        Ok(())
    }

    /// Mark a query as ready after successful population.
    fn query_ready_mark(&mut self, fingerprint: u64, cached_bytes: usize) {
        let update_info = if let Some(mut query) = self.cache.cached_queries.get1_mut(&fingerprint)
        {
            query.state = CachedQueryState::Ready;
            query.cached_bytes = cached_bytes;
            let started_at = query.registration_started_at.take();
            Some((query.generation, query.resolved.clone(), started_at))
        } else {
            None
        };

        if let Some((generation, resolved, started_at)) = update_info {
            // Record registration latency metric
            if let Some(started) = started_at {
                let latency = started.elapsed();
                metrics::histogram!(names::QUERY_REGISTRATION_LATENCY_SECONDS)
                    .record(latency.as_secs_f64());
            }

            // Update shared state view
            self.state_view_update(fingerprint, CachedQueryState::Ready, generation, &resolved);
            trace!("cached query ready, cached_bytes={cached_bytes}");
        }
    }

    /// Clean up after a failed population.
    fn query_failed_cleanup(&mut self, fingerprint: u64) {
        if let Some(query) = self.cache.cached_queries.remove1(&fingerprint) {
            // Remove generation from active set
            self.cache.generations.remove(&query.generation);

            // Remove from update_queries
            for oid in &query.relation_oids {
                if let Some(mut update_queries) = self.cache.update_queries.get_mut(oid) {
                    update_queries
                        .queries
                        .retain(|q| q.fingerprint != fingerprint);
                }
            }

            // Remove from state view
            if let Ok(mut view) = self.state_view.write() {
                view.cached_queries.remove(&fingerprint);
            }

            debug!("cleaned up failed query {fingerprint}");
        }
    }

    /// Check if there are any cached queries for a specific table.
    pub fn cached_queries_exist(&self, relation_oid: u32) -> bool {
        self.cache
            .cached_queries
            .iter()
            .any(|query| query.relation_oids.contains(&relation_oid))
    }

    /// Register table metadata from CDC processing.
    #[instrument(skip_all)]
    pub async fn cache_table_register(
        &mut self,
        mut table_metadata: TableMetadata,
    ) -> CacheResult<()> {
        let relation_oid = table_metadata.relation_oid;

        let table_exists = self.cache.tables.contains_key1(&relation_oid);
        if table_exists {
            if let Some(current_table) = self.cache.tables.get1(&relation_oid)
                && current_table.schema_eq(&table_metadata)
            {
                return Ok(());
            }

            info!(
                "Table {} (OID: {}) recreating table, invalidating queries",
                table_metadata.name, relation_oid
            );

            self.cache_table_invalidate(relation_oid).await?;
        }

        if table_metadata.indexes.is_empty() {
            table_metadata.indexes = self.query_table_indexes_get(relation_oid).await?;
        }

        self.cache_table_create_from_metadata(&table_metadata)
            .await?;

        self.cache.tables.insert_overwrite(table_metadata);

        Ok(())
    }

    /// Invalidate all cached queries that reference a table.
    async fn cache_table_invalidate(&mut self, relation_oid: u32) -> CacheResult<()> {
        let fingerprints: Vec<u64> = self
            .cache
            .cached_queries
            .iter()
            .filter(|q| q.relation_oids.contains(&relation_oid))
            .map(|q| q.fingerprint)
            .collect();

        for fp in fingerprints {
            self.cache_query_invalidate(fp).await?;
        }
        Ok(())
    }

    /// Invalidate a specific cached query and purge its generation if safe.
    async fn cache_query_invalidate(&mut self, fingerprint: u64) -> CacheResult<()> {
        let Some(query) = self.cache.cached_queries.remove1(&fingerprint) else {
            return Ok(());
        };

        trace!("invalidating query {fingerprint}");

        let prev_generation_threshold = self.cache.generation_purge_threshold();

        // Remove generation from tracking
        self.cache.generations.remove(&query.generation);

        // Remove from state view
        if let Ok(mut view) = self.state_view.write() {
            view.cached_queries.remove(&fingerprint);
        }

        // Remove update queries
        for oid in &query.relation_oids {
            if let Some(mut queries) = self.cache.update_queries.get_mut(oid) {
                queries.queries.retain(|q| q.fingerprint != fingerprint);
            }
        }

        // Purge generations based on new threshold
        // add in cache size check when supported here
        let new_threshold = self.cache.generation_purge_threshold();
        if new_threshold > prev_generation_threshold {
            let mut current_size = self.cache_size_load().await?;

            if self.cache.cache_size.is_some_and(|s| current_size > s) {
                self.generation_purge(new_threshold).await?;
                current_size = self.cache_size_load().await?;
            }

            self.cache.current_size = current_size as usize;
        }

        Ok(())
    }

    /// Utility function to get the size of the currently cached data
    async fn cache_size_load(&mut self) -> CacheResult<usize> {
        let size: i64 = self
            .db_cache
            .query_one("SELECT pgcache_total_size()", &[])
            .await
            .map_into_report::<CacheError>()?
            .get(0);

        Ok(size as usize)
    }

    /// Purge rows with generation <= threshold.
    async fn generation_purge(&mut self, threshold: u64) -> CacheResult<i64> {
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

    /// Handle INSERT operation.
    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn handle_insert(
        &mut self,
        relation_oid: u32,
        row_data: Vec<Option<String>>,
    ) -> CacheResult<()> {
        let start = Instant::now();
        metrics::counter!(names::CACHE_HANDLE_INSERTS).increment(1);

        let fp_list = self
            .update_queries_check_invalidate(relation_oid, &None, &row_data, None)
            .attach_loc("checking for query invalidations")?;

        let invalidation_count = fp_list.len() as u64;
        for fp in fp_list {
            self.cache_query_invalidate(fp)
                .await
                .attach_loc("invalidating query")?;
        }
        if invalidation_count > 0 {
            metrics::counter!(names::CACHE_INVALIDATIONS).increment(invalidation_count);
            self.state_gauges_update();
        }

        let sql_list = self
            .update_queries_sql_list(relation_oid, &row_data)
            .attach_loc("building update SQL")?;

        for sql in sql_list {
            let modified_cnt = self
                .db_cache
                .execute(sql.as_str(), &[])
                .await
                .map_into_report::<CacheError>()?;
            if modified_cnt == 1 {
                break;
            } else if modified_cnt > 1 {
                return Err(CacheError::TooManyModifiedRows.into());
            }
        }

        metrics::histogram!(names::CACHE_HANDLE_INSERT_SECONDS)
            .record(start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Handle UPDATE operation.
    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn handle_update(
        &mut self,
        relation_oid: u32,
        key_data: Vec<Option<String>>,
        new_row_data: Vec<Option<String>>,
    ) -> CacheResult<()> {
        let start = Instant::now();
        metrics::counter!(names::CACHE_HANDLE_UPDATES).increment(1);

        let row_changes = self.query_row_changes(relation_oid, &new_row_data).await?;
        trace!("row_changes {:?}", row_changes);

        let fp_list = self.update_queries_check_invalidate(
            relation_oid,
            &row_changes.first(),
            &new_row_data,
            Some(&key_data),
        )?;
        let invalidation_count = fp_list.len() as u64;
        trace!("invalidation_count {}", invalidation_count);

        for fp in fp_list {
            self.cache_query_invalidate(fp).await?;
        }
        if invalidation_count > 0 {
            metrics::counter!(names::CACHE_INVALIDATIONS).increment(invalidation_count);
            self.state_gauges_update();
        }

        let sql_list = self.update_queries_sql_list(relation_oid, &new_row_data)?;

        let mut matched = false;
        for sql in sql_list {
            let modified_cnt = self
                .db_cache
                .execute(sql.as_str(), &[])
                .await
                .map_into_report::<CacheError>()?;
            if modified_cnt == 1 {
                matched = true;
                break;
            } else if modified_cnt > 1 {
                return Err(CacheError::TooManyModifiedRows.into());
            }
        }

        if !matched {
            let Some(table_metadata) = self.cache.tables.get1(&relation_oid) else {
                error!("No table metadata found for relation_oid: {}", relation_oid);
                return Err(CacheError::UnknownTable {
                    oid: Some(relation_oid),
                    name: None,
                }
                .into());
            };

            let delete_sql = self.cache_delete_sql(table_metadata, &new_row_data)?;
            self.db_cache
                .execute(delete_sql.as_str(), &[])
                .await
                .map_into_report::<CacheError>()?;
        }

        if !key_data.is_empty() {
            let Some(table_metadata) = self.cache.tables.get1(&relation_oid) else {
                error!("No table metadata found for relation_oid: {}", relation_oid);
                return Err(CacheError::UnknownTable {
                    oid: Some(relation_oid),
                    name: None,
                }
                .into());
            };

            let delete_sql = self.cache_delete_sql(table_metadata, &key_data)?;
            self.db_cache
                .execute(delete_sql.as_str(), &[])
                .await
                .map_into_report::<CacheError>()?;
        }

        metrics::histogram!(names::CACHE_HANDLE_UPDATE_SECONDS)
            .record(start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Handle DELETE operation.
    #[instrument(skip_all)]
    pub async fn handle_delete(
        &self,
        relation_oid: u32,
        row_data: Vec<Option<String>>,
    ) -> CacheResult<()> {
        let start = Instant::now();
        metrics::counter!(names::CACHE_HANDLE_DELETES).increment(1);

        let table_metadata = match self.cache.tables.get1(&relation_oid) {
            Some(metadata) => metadata,
            None => {
                error!("No table metadata found for relation_oid: {}", relation_oid);
                metrics::histogram!(names::CACHE_HANDLE_DELETE_SECONDS)
                    .record(start.elapsed().as_secs_f64());
                return Ok(());
            }
        };

        let delete_sql = self.cache_delete_sql(table_metadata, &row_data)?;
        self.db_cache
            .execute(delete_sql.as_str(), &[])
            .await
            .map_into_report::<CacheError>()?;

        metrics::histogram!(names::CACHE_HANDLE_DELETE_SECONDS)
            .record(start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Handle TRUNCATE operation.
    #[instrument(skip_all)]
    pub async fn handle_truncate(&self, relation_oids: &[u32]) -> CacheResult<()> {
        let mut table_names: Vec<String> = Vec::new();

        for oid in relation_oids {
            if let Some(table_metadata) = self.cache.tables.get1(oid) {
                table_names.push(format!("{}.{}", table_metadata.schema, table_metadata.name));
            }
        }

        let truncate_sql = format!("TRUNCATE {}", table_names.join(", "));
        self.db_cache
            .execute(truncate_sql.as_str(), &[])
            .await
            .map_into_report::<CacheError>()?;

        Ok(())
    }

    // Helper methods

    fn state_view_update(
        &self,
        fingerprint: u64,
        state: CachedQueryState,
        generation: u64,
        resolved: &ResolvedSelectStatement,
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
    fn state_gauges_update(&self) {
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

    #[instrument(skip_all)]
    async fn cache_table_create(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> CacheResult<TableMetadata> {
        let table = self.query_table_metadata(schema, table).await?;
        self.cache_table_create_from_metadata(&table).await?;
        Ok(table)
    }

    #[instrument(skip_all)]
    async fn query_table_metadata(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> CacheResult<TableMetadata> {
        let rows = self.query_table_columns_get(schema, table).await?;

        let mut primary_key_columns: Vec<String> = Vec::new();
        let mut columns: BiHashMap<ColumnMetadata> = BiHashMap::with_capacity(rows.len());
        let mut relation_oid: Option<u32> = None;
        let mut schema: Option<&str> = schema;

        for row in &rows {
            if relation_oid.is_none() {
                relation_oid = Some(row.get("relation_oid"));
            }

            if schema.is_none() {
                schema = Some(row.get("table_schema"));
            }

            let type_oid: u32 = row.get("type_oid");

            // Try built-in types first (fast path), then discover custom types from origin
            let data_type = match Type::from_oid(type_oid) {
                Some(t) => t,
                None => self
                    .db_origin
                    .get_type(type_oid)
                    .await
                    .map_into_report::<CacheError>()?,
            };

            let type_name = data_type.name().to_owned();
            let cache_type_name = cache_type_name_resolve(&data_type)?;
            let pg_position: i64 = row.get("position");

            let column = ColumnMetadata {
                name: row.get("column_name"),
                position: pg_position as i16,
                type_oid,
                data_type,
                type_name,
                cache_type_name,
                is_primary_key: row.get("is_primary_key"),
            };

            if column.is_primary_key {
                primary_key_columns.push(column.name.clone());
            }

            columns.insert_overwrite(column);
        }

        let Some(relation_oid) = relation_oid else {
            return Err(CacheError::UnknownTable {
                oid: relation_oid,
                name: None,
            }
            .into());
        };

        let Some(schema) = schema else {
            return Err(CacheError::UnknownSchema.into());
        };

        let indexes = self.query_table_indexes_get(relation_oid).await?;

        let table = TableMetadata {
            name: table.to_owned(),
            schema: schema.to_owned(),
            relation_oid,
            primary_key_columns,
            columns,
            indexes,
        };

        Ok(table)
    }

    #[instrument(skip_all)]
    async fn query_table_columns_get(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> CacheResult<Vec<Row>> {
        let rows = if let Some(schema) = schema {
            let sql = r"
                SELECT
                    c.oid AS relation_oid,
                    n.nspname AS table_schema,
                    c.relname AS table_name,
                    a.attname AS column_name,
                    RANK() OVER (order by a.attnum) AS position,
                    a.atttypid AS type_oid,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) as type_name,
                    a.attnum = any(pgc.conkey) as is_primary_key
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid
                JOIN pg_type t ON t.oid = a.atttypid
                JOIN pg_constraint pgc ON pgc.conrelid = c.oid
                WHERE c.relname = $1
                AND n.nspname = $2
                AND a.attnum > 0
                AND pgc.contype = 'p'
                AND NOT a.attisdropped
                ORDER BY a.attnum;
            ";

            self.db_origin
                .query(sql, &[&table, &schema])
                .await
                .map_into_report::<CacheError>()?
        } else {
            let sql = r"
                SELECT
                    c.oid AS relation_oid,
                    n.nspname AS table_schema,
                    c.relname AS table_name,
                    a.attname AS column_name,
                    RANK() OVER (order by a.attnum) AS position,
                    a.atttypid AS type_oid,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) as type_name,
                    a.attnum = any(pgc.conkey) as is_primary_key
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid
                JOIN pg_type t ON t.oid = a.atttypid
                JOIN pg_constraint pgc ON pgc.conrelid = c.oid
                WHERE c.relname = $1
                AND c.oid = (
                    SELECT c2.oid
                    FROM pg_class c2
                    JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
                    WHERE c2.relname = $1
                    AND c2.relkind IN ('r', 'p')
                    AND n2.nspname = any(current_schemas(false))
                    ORDER BY array_position(current_schemas(false), n2.nspname)
                    LIMIT 1
                )
                AND a.attnum > 0
                AND pgc.contype = 'p'
                AND NOT a.attisdropped
                ORDER BY a.attnum;
            ";

            self.db_origin
                .query(sql, &[&table])
                .await
                .map_into_report::<CacheError>()?
        };

        Ok(rows)
    }

    /// Find one child partition of a partitioned table, if any.
    /// Returns None for regular tables or partitioned tables with no children.
    #[instrument(skip_all)]
    async fn partition_child_find(&self, relation_oid: u32) -> CacheResult<Option<u32>> {
        let sql = r"
            SELECT inhrelid::oid
            FROM pg_inherits
            WHERE inhparent = $1
            LIMIT 1
        ";

        let row = self
            .db_origin
            .query_opt(sql, &[&relation_oid])
            .await
            .map_into_report::<CacheError>()?;

        Ok(row.map(|r| r.get::<_, u32>(0)))
    }

    #[instrument(skip_all)]
    async fn query_table_indexes_get(&self, relation_oid: u32) -> CacheResult<Vec<IndexMetadata>> {
        // For partitioned tables, query indexes from a child partition.
        // Falls back to parent (works for regular tables and partitioned tables with no children).
        let child_oid = self.partition_child_find(relation_oid).await?;
        let target_oid = child_oid.unwrap_or(relation_oid);

        let sql = r"
            SELECT
                i.relname AS index_name,
                ix.indisunique AS is_unique,
                am.amname AS method,
                array_agg(a.attname ORDER BY array_position(ix.indkey::int[], a.attnum::int)) AS columns
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_am am ON am.oid = i.relam
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE t.oid = $1
              AND NOT ix.indisprimary
              AND ix.indexprs IS NULL
              AND ix.indpred IS NULL
            GROUP BY i.relname, ix.indisunique, am.amname, ix.indkey
            ORDER BY i.relname;
        ";

        let rows = self
            .db_origin
            .query(sql, &[&target_oid])
            .await
            .map_into_report::<CacheError>()?;

        let indexes = rows
            .iter()
            .map(|row| {
                let columns: Vec<String> = row.get("columns");
                IndexMetadata {
                    name: row.get("index_name"),
                    is_unique: row.get("is_unique"),
                    method: row.get("method"),
                    columns,
                }
            })
            .collect();

        Ok(indexes)
    }

    #[instrument(skip_all)]
    async fn schema_for_table_find(
        &self,
        table_name: &str,
        search_path: &[&str],
    ) -> CacheResult<String> {
        for schema in search_path {
            if self.cache.tables.get2(&(*schema, table_name)).is_some() {
                return Ok((*schema).to_owned());
            }
        }

        let sql = r"
            SELECT n.nspname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = $1
            AND c.relkind IN ('r', 'p')
            AND n.nspname = any($2)
            ORDER BY array_position($2::text[], n.nspname::text)
            LIMIT 1;
        ";

        let rows = self
            .db_origin
            .query(sql, &[&table_name, &search_path])
            .await
            .map_into_report::<CacheError>()?;

        rows.first()
            .map(|row| row.get::<_, String>(0))
            .ok_or_else(|| {
                CacheError::UnknownTable {
                    oid: None,
                    name: Some(table_name.to_owned()),
                }
                .into()
            })
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    async fn query_row_changes(
        &self,
        relation_oid: u32,
        row_data: &[Option<String>],
    ) -> CacheResult<Vec<Row>> {
        let table_metadata =
            self.cache
                .tables
                .get1(&relation_oid)
                .ok_or(CacheError::UnknownTable {
                    oid: Some(relation_oid),
                    name: None,
                })?;

        let mut where_conditions = Vec::new();
        for pk_column in &table_metadata.primary_key_columns {
            if let Some(column_meta) = table_metadata.columns.get1(pk_column.as_str()) {
                let position = column_meta.position as usize - 1;
                if let Some(row_value) = row_data.get(position) {
                    let value = row_value
                        .as_deref()
                        .map_or_else(|| "NULL".to_owned(), escape::escape_literal);
                    where_conditions.push(format!("{pk_column} = {value}"));
                }
            }
        }

        if where_conditions.is_empty() {
            return Err(CacheError::NoPrimaryKey.into());
        }

        let mut comparison_columns = Vec::new();
        for column_meta in &table_metadata.columns {
            let position = column_meta.position as usize - 1;
            if let Some(row_value) = row_data.get(position) {
                let value = row_value
                    .as_deref()
                    .map_or_else(|| "NULL".to_owned(), escape::escape_literal);
                comparison_columns.push(format!(
                    "{} IS DISTINCT FROM {} AS {}",
                    column_meta.name, value, column_meta.name
                ));
            }
        }

        let sql = format!(
            "SELECT {} FROM {}.{} WHERE {}",
            comparison_columns.join(", "),
            table_metadata.schema,
            table_metadata.name,
            where_conditions.join(" AND ")
        );

        self.db_cache
            .query(&sql, &[])
            .await
            .map_into_report::<CacheError>()
    }

    #[instrument(skip_all)]
    async fn cache_table_create_from_metadata(
        &self,
        table_metadata: &TableMetadata,
    ) -> CacheResult<()> {
        let schema = &table_metadata.schema;
        let table = &table_metadata.name;

        let column_defs: Vec<String> = table_metadata
            .columns
            .iter()
            .map(|c| format!("    \"{}\" {}", c.name, c.cache_type_name))
            .collect();
        let column_defs = column_defs.join(",\n");

        let primary_key = table_metadata
            .primary_key_columns
            .iter()
            .map(|c| format!("\"{c}\""))
            .collect::<Vec<_>>()
            .join(", ");

        let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\"");
        let drop_sql = format!("DROP TABLE IF EXISTS \"{schema}\".\"{table}\"");
        let create_sql = format!(
            "CREATE UNLOGGED TABLE \"{schema}\".\"{table}\" (\n{column_defs},\n\tPRIMARY KEY({primary_key})\n)"
        );

        self.db_cache
            .execute(&create_schema_sql, &[])
            .await
            .map_into_report::<CacheError>()?;
        self.db_cache
            .execute(&drop_sql, &[])
            .await
            .map_into_report::<CacheError>()?;
        self.db_cache
            .execute(&create_sql, &[])
            .await
            .map_into_report::<CacheError>()?;

        for index in &table_metadata.indexes {
            let unique = if index.is_unique { "UNIQUE " } else { "" };
            let method = &index.method;
            let columns = index.columns.join(", ");
            let index_sql = format!(
                "CREATE {unique}INDEX ON \"{schema}\".\"{table}\" USING {method} ({columns})"
            );
            self.db_cache
                .execute(&index_sql, &[])
                .await
                .map_into_report::<CacheError>()?;
        }

        // Enable generation tracking triggers on the table
        let enable_tracking_sql =
            format!("SELECT pgcache_enable_tracking('\"{schema}\".\"{table}\"'::regclass::oid)");
        self.db_cache
            .execute(&enable_tracking_sql, &[])
            .await
            .map_into_report::<CacheError>()?;

        Ok(())
    }

    fn cache_upsert_with_predicate_sql(
        &self,
        resolved: &ResolvedSelectStatement,
        table_metadata: &TableMetadata,
        row_data: &[Option<String>],
    ) -> CacheResult<String> {
        let mut column_names = Vec::new();
        let mut values = Vec::new();

        for column_meta in &table_metadata.columns {
            let position = column_meta.position as usize - 1;
            if let Some(row_value) = row_data.get(position) {
                let value = row_value
                    .as_deref()
                    .map_or_else(|| "NULL".to_owned(), escape::escape_literal);

                column_names.push(column_meta.name.as_str());
                values.push(value);
            }
        }

        let value_select = resolved_table_replace_with_values(resolved, table_metadata, row_data)
            .map_err(|e| CacheError::from(e.into_current_context()))?;
        let mut select = String::with_capacity(1024);
        value_select.deparse(&mut select);

        let schema = &table_metadata.schema;
        let table = &table_metadata.name;
        let column_list = column_names.join(", ");
        let value_list = values.join(", ");
        let pk_column_list = table_metadata.primary_key_columns.join(", ");
        let update_list = column_names
            .iter()
            .filter(|&col| {
                !table_metadata
                    .primary_key_columns
                    .contains(&col.to_string())
            })
            .map(|col| format!("{col} = EXCLUDED.{col}"))
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "INSERT INTO {schema}.{table} ({column_list}) \
            SELECT {value_list} WHERE EXISTS ({select}) \
            ON CONFLICT ({pk_column_list}) \
            DO UPDATE SET {update_list}"
        );

        Ok(sql)
    }

    fn update_queries_sql_list(
        &self,
        relation_oid: u32,
        row_data: &[Option<String>],
    ) -> CacheResult<Vec<String>> {
        let update_queries =
            self.cache
                .update_queries
                .get(&relation_oid)
                .ok_or(CacheError::UnknownTable {
                    oid: Some(relation_oid),
                    name: None,
                })?;

        let Some(table_metadata) = self.cache.tables.get1(&relation_oid) else {
            error!("No table metadata found for relation_oid: {}", relation_oid);
            return Err(CacheError::UnknownTable {
                oid: Some(relation_oid),
                name: None,
            }
            .into());
        };

        let mut sql_list = Vec::new();
        for update_query in &update_queries.queries {
            sql_list.push(
                self.cache_upsert_with_predicate_sql(
                    &update_query.resolved,
                    table_metadata,
                    row_data,
                )
                .attach_loc(format!(
                    "building update SQL for query {}",
                    update_query.fingerprint
                ))?,
            );
        }

        Ok(sql_list)
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    fn update_queries_check_invalidate(
        &self,
        relation_oid: u32,
        row_changes: &Option<&Row>,
        row_data: &[Option<String>],
        key_data: Option<&[Option<String>]>,
    ) -> CacheResult<Vec<u64>> {
        let update_queries =
            self.cache
                .update_queries
                .get(&relation_oid)
                .ok_or(CacheError::UnknownTable {
                    oid: Some(relation_oid),
                    name: None,
                })?;

        let Some(table_metadata) = self.cache.tables.get1(&relation_oid) else {
            error!("No table metadata found for relation_oid: {}", relation_oid);
            return Err(CacheError::UnknownTable {
                oid: Some(relation_oid),
                name: None,
            }
            .into());
        };

        let mut fp_list = Vec::new();
        for update_query in &update_queries.queries {
            let cached_query = self
                .cache
                .cached_queries
                .get1(&update_query.fingerprint)
                .ok_or_else(|| {
                    error!(
                        "Cached query not found for fingerprint: {}",
                        update_query.fingerprint
                    );
                    CacheError::Other
                })?;

            if row_changes.is_none() {
                // Row not in cache - potentially entering result set.
                // We need to determine if this update could affect the query result.

                if update_query.resolved.is_single_table() {
                    continue;
                }

                let has_table_constraints = cached_query
                    .constraints
                    .table_constraints
                    .contains_key(&table_metadata.name);

                // If key_data is empty, PK didn't change. If all join columns are PK columns
                // and there are no WHERE constraints for this table, the row's membership
                // in the result set is unchanged - skip invalidation.
                if !has_table_constraints {
                    if let Some(key) = key_data
                        && key.is_empty()
                    {
                        let join_columns: Vec<&str> = cached_query
                            .constraints
                            .table_join_columns(&table_metadata.name)
                            .collect();

                        let all_join_cols_are_pk = !join_columns.is_empty()
                            && join_columns.iter().all(|col| {
                                table_metadata
                                    .primary_key_columns
                                    .iter()
                                    .any(|pk| pk == col)
                            });

                        if all_join_cols_are_pk {
                            // PK didn't change, all join columns are PK, no WHERE constraints
                            // Row membership is stable - skip invalidation
                            continue;
                        }
                    }

                    // No constraints and couldn't prove stability - must invalidate
                    fp_list.push(update_query.fingerprint);
                    continue;
                }

                // Check if row matches table constraints
                if let Some(constraints) = cached_query
                    .constraints
                    .table_constraints
                    .get(&table_metadata.name)
                {
                    let mut all_match = true;
                    for (column_name, constraint_value) in constraints {
                        if let Some(column_meta) = table_metadata.columns.get1(column_name.as_str())
                        {
                            let position = column_meta.position as usize - 1;
                            if let Some(row_value) = row_data.get(position)
                                && !constraint_value.matches(row_value)
                            {
                                all_match = false;
                                break;
                            }
                        }
                    }

                    if !all_match {
                        continue;
                    }
                }

                fp_list.push(update_query.fingerprint);
                continue;
            }

            let mut needs_invalidation = false;

            for column in cached_query
                .constraints
                .table_join_columns(&table_metadata.name)
            {
                let column_changed = row_changes.is_some_and(|row| row.get::<&str, bool>(column));

                if !column_changed {
                    continue;
                }

                if let Some(constraints) = cached_query
                    .constraints
                    .table_constraints
                    .get(&table_metadata.name)
                {
                    let mut all_constraints_match = true;
                    for (constraint_column, constraint_value) in constraints {
                        if let Some(column_meta) =
                            table_metadata.columns.get1(constraint_column.as_str())
                        {
                            let position = column_meta.position as usize - 1;
                            if let Some(row_value) = row_data.get(position)
                                && !constraint_value.matches(row_value)
                            {
                                all_constraints_match = false;
                                break;
                            }
                        }
                    }

                    if !all_constraints_match {
                        continue;
                    }
                }

                needs_invalidation = true;
                break;
            }

            if needs_invalidation {
                fp_list.push(update_query.fingerprint);
            }
        }

        Ok(fp_list)
    }

    #[instrument(skip_all)]
    fn cache_delete_sql(
        &self,
        table_metadata: &TableMetadata,
        row_data: &[Option<String>],
    ) -> CacheResult<String> {
        let mut where_conditions = Vec::new();

        for pk_column in &table_metadata.primary_key_columns {
            if let Some(column_meta) = table_metadata.columns.get1(pk_column.as_str()) {
                let position = column_meta.position as usize - 1;
                if let Some(row_value) = row_data.get(position) {
                    let value = row_value
                        .as_deref()
                        .map_or_else(|| "NULL".to_owned(), escape::escape_literal);
                    where_conditions.push(format!("{pk_column} = {value}"));
                }
            }
        }

        if where_conditions.is_empty() {
            error!("Cannot build DELETE WHERE clause: no primary key values found");
            return Err(CacheError::NoPrimaryKey.into());
        }

        let sql = format!(
            "DELETE FROM {}.{} WHERE {}",
            table_metadata.schema,
            table_metadata.name,
            where_conditions.join(" AND ")
        );

        Ok(sql)
    }
}

/// Persistent population worker that processes work items from a channel.
/// Each worker owns its own cache database connection.
async fn population_worker(
    id: usize,
    mut rx: UnboundedReceiver<PopulationWork>,
    db_origin: Rc<Client>,
    db_cache: Client,
    writer_tx: UnboundedSender<WriterCommand>,
) {
    debug!("population worker {id} started");

    while let Some(work) = rx.recv().await {
        let result = population_task(
            work.fingerprint,
            work.generation,
            &work.relation_oids,
            &work.table_metadata,
            &work.resolved,
            Rc::clone(&db_origin),
            &db_cache,
        )
        .await;

        match result {
            Ok(cached_bytes) => {
                if writer_tx
                    .send(WriterCommand::QueryReady {
                        fingerprint: work.fingerprint,
                        cached_bytes,
                    })
                    .is_err()
                {
                    error!("population worker {id}: failed to send QueryReady");
                }
            }
            Err(e) => {
                error!(
                    "population worker {id}: population failed for query {}: {e}",
                    work.fingerprint
                );
                if writer_tx
                    .send(WriterCommand::QueryFailed {
                        fingerprint: work.fingerprint,
                    })
                    .is_err()
                {
                    error!("population worker {id}: failed to send QueryFailed");
                }
            }
        }
    }

    debug!("population worker {id} shutting down");
}

/// Background task for populating cache with query results.
/// Runs on a dedicated pool connection to avoid session variable conflicts.
#[allow(clippy::too_many_arguments)]
async fn population_task(
    fingerprint: u64,
    generation: u64,
    relation_oids: &[u32],
    table_metadata: &[TableMetadata],
    resolved: &ResolvedSelectStatement,
    db_origin: Rc<Client>,
    db_cache: &Client,
) -> CacheResult<usize> {
    // Set generation for tracking triggers
    let set_generation_sql = format!("SET mem.query_generation = {generation}");
    db_cache
        .execute(&set_generation_sql, &[])
        .await
        .map_into_report::<CacheError>()?;

    let mut total_bytes: usize = 0;
    let task_start = Instant::now();

    // Fetch and populate for each table
    for &table_oid in relation_oids {
        let table = table_metadata
            .iter()
            .find(|t| t.relation_oid == table_oid)
            .ok_or(CacheError::UnknownTable {
                oid: Some(table_oid),
                name: None,
            })?;

        // Fetch from origin
        let fetch_start = Instant::now();
        let rows = population_fetch(&db_origin, table_oid, table, resolved).await?;
        let fetch_elapsed = fetch_start.elapsed();

        // Populate cache
        let insert_start = Instant::now();
        let bytes = population_insert(db_cache, table, &rows).await?;
        let insert_elapsed = insert_start.elapsed();

        total_bytes += bytes;

        trace!(
            "population table {}.{} fetch={:?} insert={:?} bytes={bytes}",
            table.schema, table.name, fetch_elapsed, insert_elapsed
        );
    }

    let task_elapsed = task_start.elapsed();

    // Reset generation
    db_cache
        .execute("SET mem.query_generation = 0", &[])
        .await
        .map_into_report::<CacheError>()?;

    trace!(
        "population complete for query {fingerprint}, total_time={:?} bytes={total_bytes}",
        task_elapsed
    );
    Ok(total_bytes)
}

/// Fetch data from origin database for a single table.
async fn population_fetch(
    db_origin: &Client,
    relation_oid: u32,
    table: &TableMetadata,
    resolved: &ResolvedSelectStatement,
) -> CacheResult<Vec<SimpleQueryMessage>> {
    let maybe_alias = resolved
        .nodes::<ResolvedTableNode>()
        .find(|tn| tn.relation_oid == relation_oid)
        .and_then(|t| t.alias.as_deref());

    // Use table metadata to get column list
    let select_columns = table.resolved_select_columns(maybe_alias);

    // Build query with table columns
    use crate::query::transform::resolved_select_replace;
    let new_ast = resolved_select_replace(resolved, select_columns);
    let mut buf = String::with_capacity(1024);
    new_ast.deparse(&mut buf);

    db_origin
        .simple_query(&buf)
        .await
        .map_into_report::<CacheError>()
}

/// Insert fetched rows into cache table.
async fn population_insert(
    db_cache: &Client,
    table: &TableMetadata,
    response: &[SimpleQueryMessage],
) -> CacheResult<usize> {
    let [
        SimpleQueryMessage::RowDescription(row_description),
        data_rows @ ..,
        _command_complete,
    ] = response
    else {
        return Ok(0);
    };

    let mut cached_bytes: usize = 0;

    let pkey_columns = table
        .primary_key_columns
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>();
    let schema = &table.schema;
    let table_name = &table.name;

    let rows: Vec<_> = data_rows
        .iter()
        .filter_map(|msg| {
            if let SimpleQueryMessage::Row(row) = msg {
                Some(row)
            } else {
                None
            }
        })
        .collect();

    let mut sql_list = Vec::new();
    let columns: Vec<String> = row_description
        .iter()
        .map(|c| format!("\"{}\"", c.name()))
        .collect();

    for row in &rows {
        let mut values: Vec<String> = Vec::new();
        for idx in 0..row.columns().len() {
            let value = row.get(idx);
            cached_bytes += value.map_or(0, |v| v.len());
            values.push(
                value
                    .map(escape::escape_literal)
                    .unwrap_or("NULL".to_owned()),
            );
        }

        let update_columns: Vec<_> = columns
            .iter()
            .filter(|&c| !pkey_columns.contains(&c.to_owned()))
            .map(|c| format!("{c} = EXCLUDED.{c}"))
            .collect();

        let mut insert_table = format!(
            "insert into \"{schema}\".\"{table_name}\"({}) values (",
            columns.join(",")
        );
        insert_table.push_str(&values.join(","));
        insert_table.push_str(") on conflict (");
        insert_table.push_str(&pkey_columns.join(","));
        insert_table.push_str(") do update set ");
        insert_table.push_str(&update_columns.join(", "));

        sql_list.push(insert_table);
    }

    if !sql_list.is_empty() {
        db_cache
            .simple_query(sql_list.join(";").as_str())
            .await
            .map_into_report::<CacheError>()?;
    }

    Ok(cached_bytes)
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
