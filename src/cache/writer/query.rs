use std::sync::Arc;
use std::time::Instant;

use tokio::sync::oneshot;
use tracing::{debug, error, info, instrument, trace};

use crate::cache::query::limit_rows_needed;
use crate::catalog::TableMetadata;
use crate::metrics::names;
use crate::query::ast::{Deparse, QueryBody, QueryExpr, TableNode};
use crate::query::constraints::{analyze_query_constraints, table_constraints_subsumed};
use crate::query::decorrelate::query_expr_decorrelate;
use crate::query::resolved::{
    ResolvedQueryExpr, ResolvedSelectNode, ResolvedTableNode, query_expr_resolve,
};
use crate::query::transform::predicate_pushdown_apply;
use crate::query::update::query_table_update_queries;

use super::super::{
    CacheError, CacheResult, MapIntoReport, ReportExt,
    messages::{AdmitAction, SubsumptionResult},
    query::CacheableQuery,
    types::{
        CachedQuery, CachedQueryState, QueryMetrics, SharedResolved, UpdateQueries, UpdateQuery,
    },
};
use super::{CacheWriter, PopulationWork};

/// Intermediate result from resolving a query before subsumption check or population.
struct QueryResolution {
    resolved: SharedResolved,
    relation_oids: Vec<u32>,
    base_query: QueryExpr,
    max_limit: Option<u64>,
}

/// Clone the query, strip LIMIT, and compute max_limit for population.
/// Set operations force max_limit = None since population runs per-branch.
fn base_query_prepare(query: &QueryExpr) -> (QueryExpr, Option<u64>) {
    let is_set_op = matches!(query.body, QueryBody::SetOp(_));
    let max_limit = if is_set_op {
        None
    } else {
        limit_rows_needed(&query.limit)
    };
    let mut base_query = query.clone();
    base_query.limit = None;
    (base_query, max_limit)
}

impl CacheWriter {
    /// Build population work for a query, handling decorrelation and branch extraction.
    ///
    /// Decorrelates the resolved AST so correlated subqueries are merged into JOINs,
    /// then extracts SELECT branches, collects table metadata, and builds PopulationWork.
    fn population_work_build(
        &self,
        fingerprint: u64,
        generation: u64,
        resolved: &SharedResolved,
        max_limit: Option<u64>,
    ) -> PopulationWork {
        let population_resolved = query_expr_decorrelate(resolved, &self.aggregate_functions)
            .map(|d| {
                if d.transformed {
                    d.resolved
                } else {
                    ResolvedQueryExpr::clone(resolved)
                }
            })
            .unwrap_or_else(|_| ResolvedQueryExpr::clone(resolved));

        let branches: Vec<ResolvedSelectNode> = population_resolved
            .select_nodes()
            .into_iter()
            .cloned()
            .collect();

        let branch_relation_oids: Vec<u32> = branches
            .iter()
            .flat_map(|branch: &ResolvedSelectNode| branch.nodes::<ResolvedTableNode>())
            .map(|tn| tn.relation_oid)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let table_metadata: Vec<TableMetadata> = branch_relation_oids
            .iter()
            .filter_map(|oid| self.cache.tables.get1(oid).cloned())
            .collect();

        PopulationWork {
            fingerprint,
            generation,
            table_metadata,
            branches,
            max_limit,
        }
    }

    /// Resolve schema for a table: use explicit schema if provided, otherwise lookup via search path.
    async fn table_schema_resolve(
        &self,
        table_name: &str,
        explicit_schema: Option<&str>,
        search_path: &[&str],
    ) -> CacheResult<String> {
        if let Some(schema) = explicit_schema {
            Ok(schema.to_owned())
        } else {
            self.schema_for_table_find(table_name, search_path).await
        }
    }

    /// Register an update query for a relation, maintaining complexity sort order.
    fn update_query_register(&mut self, relation_oid: u32, update_query: UpdateQuery) {
        self.cache
            .update_queries
            .entry(relation_oid)
            .and_modify(|mut queries| {
                queries.queries.push(update_query.clone());
                // Keep queries sorted by complexity (ascending) so simpler queries are tried first
                queries.queries.sort_by_key(|q| q.complexity);
            })
            .or_insert_with(|| UpdateQueries {
                relation_oid,
                queries: vec![update_query],
            });
    }

    /// Dispatch population work to next worker using round-robin scheduling.
    fn populate_work_dispatch(&mut self, work: PopulationWork) -> CacheResult<()> {
        let idx = self.populate_next;
        self.populate_next = (self.populate_next + 1) % self.populate_txs.len();

        let Some(tx) = self.populate_txs.get(idx) else {
            return Err(CacheError::Other.into());
        };

        if tx.send(work).is_err() {
            error!("population worker {idx} channel closed");
        }

        Ok(())
    }

    /// Ensure all tables referenced in the query exist in the cache.
    /// Resolves schemas and creates cache tables as needed.
    async fn cache_tables_ensure(
        &mut self,
        base_query: &QueryExpr,
        search_path: &[&str],
    ) -> CacheResult<()> {
        for table_node in base_query.nodes::<TableNode>() {
            let table_name = table_node.name.as_str();
            let schema = self
                .table_schema_resolve(table_name, table_node.schema.as_deref(), search_path)
                .await?;

            if !self
                .cache
                .tables
                .contains_key2(&(schema.as_str(), table_name))
            {
                let table = self.cache_table_create(Some(&schema), table_name).await?;
                self.cache.tables.insert_overwrite(table);
            }
        }
        Ok(())
    }

    /// Decorrelate the resolved AST and register update queries for each table.
    /// Returns the relation OIDs that have update queries registered.
    fn update_queries_register(
        &mut self,
        fingerprint: u64,
        resolved: &SharedResolved,
        has_limit: bool,
    ) -> CacheResult<Vec<u32>> {
        let decorrelated = query_expr_decorrelate(resolved, &self.aggregate_functions)
            .map_err(|e| e.context_transform(CacheError::from))
            .attach_loc("decorrelating correlated subqueries")?;
        let update_source = if decorrelated.transformed {
            &decorrelated.resolved
        } else {
            resolved
        };

        let mut relation_oids = Vec::new();
        for (table_node, update_resolved, source) in query_table_update_queries(update_source) {
            let relation_oid = table_node.relation_oid;
            let constraints = update_resolved
                .as_select()
                .map(analyze_query_constraints)
                .unwrap_or_default();
            let complexity = update_resolved.complexity();
            let update_query = UpdateQuery {
                fingerprint,
                resolved: update_resolved,
                complexity,
                source,
                constraints,
                has_limit,
            };

            self.update_query_register(relation_oid, update_query);
            relation_oids.push(relation_oid);
        }
        Ok(relation_oids)
    }

    /// Assign a generation number and insert the CachedQuery entry.
    /// Returns `(generation, relations_changed)`.
    #[allow(clippy::too_many_arguments)]
    fn cached_query_insert(
        &mut self,
        fingerprint: u64,
        relation_oids: Vec<u32>,
        base_query: QueryExpr,
        resolved: SharedResolved,
        max_limit: Option<u64>,
        started_at: Instant,
        pinned: bool,
    ) -> (u64, bool) {
        self.cache.generation_counter += 1;
        let generation = self.cache.generation_counter;
        self.cache.generations.insert(generation);

        let cached_query = CachedQuery {
            fingerprint,
            generation,
            relation_oids,
            query: base_query,
            resolved,
            max_limit,
            cached_bytes: 0,
            registration_started_at: Some(started_at),
            invalidated: false,
            pinned,
        };

        self.cache.cached_queries.insert_overwrite(cached_query);
        let changed = self.active_relations_rebuild();
        (generation, changed)
    }

    /// Resolve a query's tables and AST, register update queries, and extract constraints.
    /// This is the first phase of registration, before subsumption or population.
    async fn query_resolve(
        &mut self,
        fingerprint: u64,
        cacheable_query: &CacheableQuery,
        search_path: &[&str],
    ) -> CacheResult<QueryResolution> {
        let (base_query, max_limit) = base_query_prepare(&cacheable_query.query);

        self.cache_tables_ensure(&base_query, search_path).await?;

        let resolved: SharedResolved = Arc::new(
            query_expr_resolve(&base_query, &self.cache.tables, search_path)
                .map_err(|e| e.context_transform(CacheError::from))
                .attach_loc("resolving query expression")
                .map(predicate_pushdown_apply)?,
        );

        let relation_oids =
            self.update_queries_register(fingerprint, &resolved, max_limit.is_some())?;

        Ok(QueryResolution {
            resolved,
            relation_oids,
            base_query,
            max_limit,
        })
    }

    /// Check whether all tables in the new query are covered by existing cached queries.
    /// Returns true only if every relation_oid has at least one Ready, non-limited
    /// UpdateQuery whose equality constraints are implied by the new query's constraints.
    fn subsumption_check(&self, resolution: &QueryResolution) -> bool {
        if resolution.relation_oids.is_empty() {
            return false;
        }

        // Set operations (UNION/INTERSECT/EXCEPT) require per-branch constraint
        // analysis which isn't implemented yet. Reject unconditionally for now.
        let Some(select) = resolution.resolved.as_select() else {
            return false;
        };

        let new_constraints = analyze_query_constraints(select);

        for &oid in &resolution.relation_oids {
            let Some(update_queries) = self.cache.update_queries.get(&oid) else {
                return false;
            };

            let Some(table_meta) = self.cache.tables.get1(&oid) else {
                return false;
            };
            let table_name = &table_meta.name;

            let table_covered = update_queries.queries.iter().any(|uq| {
                if uq.has_limit {
                    return false;
                }

                let parent = self.cache.cached_queries.get1(&uq.fingerprint);

                let parent_ready =
                    parent.is_some_and(|q| !q.invalidated && q.registration_started_at.is_none());
                if !parent_ready {
                    return false;
                }

                // Only single-table cached queries are subsumption candidates.
                // Multi-table queries have implicit join filtering that constraint
                // analysis doesn't capture, so we can't safely reason about coverage.
                let parent_single_table = parent.is_some_and(|q| q.relation_oids.len() == 1);
                if !parent_single_table {
                    return false;
                }

                table_constraints_subsumed(&new_constraints, &uq.constraints, table_name)
            });

            if !table_covered {
                return false;
            }
        }

        true
    }

    /// Handle a subsumed query: assign generation, stamp rows in cache DB, mark Ready.
    /// Returns (generation, resolved) on success. Falls back to None if cache DB execution fails.
    async fn query_subsume(
        &mut self,
        fingerprint: u64,
        resolution: QueryResolution,
        started_at: Instant,
        pinned: bool,
    ) -> CacheResult<Option<(u64, SharedResolved)>> {
        let subsume_start = Instant::now();

        let (generation, relations_changed) = self.cached_query_insert(
            fingerprint,
            resolution.relation_oids,
            resolution.base_query,
            Arc::clone(&resolution.resolved),
            resolution.max_limit,
            started_at,
            pinned,
        );

        if relations_changed {
            self.publication_update().await?;
        }

        // Stamp rows: SET generation, execute query, reset generation
        let set_gen_sql = format!("SET mem.query_generation = {generation}");
        if let Err(e) = self
            .db_cache
            .execute(&set_gen_sql, &[])
            .await
            .map_into_report::<CacheError>()
        {
            error!("subsumption generation set failed: {e}");
            return Ok(None);
        }

        let mut sql = String::with_capacity(512);
        resolution.resolved.deparse(&mut sql);

        let cache_exec_result = self
            .db_cache
            .query(&sql, &[])
            .await
            .map_into_report::<CacheError>();

        // Always reset generation, even on failure
        let _ = self
            .db_cache
            .execute("SET mem.query_generation = 0", &[])
            .await;

        if let Err(e) = cache_exec_result {
            error!("subsumption cache query failed: {e}");
            return Ok(None);
        }

        // Mark Ready in state view
        self.state_view_update(
            fingerprint,
            CachedQueryState::Ready,
            generation,
            &resolution.resolved,
            resolution.max_limit,
        );

        // Clear registration_started_at to signal completion
        if let Some(mut q) = self.cache.cached_queries.get1_mut(&fingerprint) {
            q.registration_started_at = None;
        }

        metrics::counter!(names::CACHE_SUBSUMPTIONS).increment(1);
        metrics::histogram!(names::CACHE_SUBSUMPTION_LATENCY_SECONDS)
            .record(subsume_start.elapsed().as_secs_f64());

        debug!("query subsumed {fingerprint}");
        Ok(Some((generation, resolution.resolved)))
    }

    /// Registers a query in the cache. Checks subsumption first — if the data
    /// is already cached by a broader query, stamps rows and marks Ready immediately.
    /// Otherwise, dispatches population (if `admit_action` is `Admit`).
    ///
    /// If the query was previously invalidated (CLOCK policy), takes the fast
    /// readmission path that reuses existing metadata.
    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    #[allow(clippy::too_many_arguments)]
    pub async fn query_register(
        &mut self,
        fingerprint: u64,
        cacheable_query: &CacheableQuery,
        search_path: &[&str],
        started_at: Instant,
        subsumption_tx: oneshot::Sender<SubsumptionResult>,
        admit_action: AdmitAction,
        pinned: bool,
    ) -> CacheResult<()> {
        // Fast readmit path for invalidated queries — skip subsumption
        if let Some(query) = self.cache.cached_queries.get1(&fingerprint)
            && query.invalidated
        {
            let _ = subsumption_tx.send(SubsumptionResult::NotSubsumed);
            return self.query_readmit(fingerprint, started_at).await;
        }

        // Phase 1: Resolve
        let resolution = self
            .query_resolve(fingerprint, cacheable_query, search_path)
            .await?;

        // Phase 2: Subsumption check
        let subsumed = self.subsumption_check(&resolution);

        if subsumed {
            // Phase 3a: Subsume — stamp rows, mark Ready
            let fallback_resolved = Arc::clone(&resolution.resolved);
            let fallback_max_limit = resolution.max_limit;

            match self
                .query_subsume(fingerprint, resolution, started_at, pinned)
                .await?
            {
                Some((generation, resolved)) => {
                    let _ = subsumption_tx.send(SubsumptionResult::Subsumed {
                        generation,
                        resolved,
                    });
                    return Ok(());
                }
                None => {
                    // Cache DB execution failed — fall back to population.
                    // The query was already inserted by query_subsume, so we need
                    // to clean it up and re-insert properly, or just populate.
                    // Since cached_query_insert was already called, just dispatch population.
                    let _ = subsumption_tx.send(SubsumptionResult::NotSubsumed);
                    let generation = self
                        .cache
                        .cached_queries
                        .get1(&fingerprint)
                        .map(|q| q.generation)
                        .unwrap_or(0);
                    if generation > 0 {
                        let work = self.population_work_build(
                            fingerprint,
                            generation,
                            &fallback_resolved,
                            fallback_max_limit,
                        );
                        self.populate_work_dispatch(work)?;
                        trace!("subsumption fallback: population queued {fingerprint}");
                    }
                    return Ok(());
                }
            }
        }

        // Phase 3b: Not subsumed
        let _ = subsumption_tx.send(SubsumptionResult::NotSubsumed);

        if admit_action == AdmitAction::CheckOnly {
            // Pending below threshold — don't register, don't populate.
            // Clean up the update_queries we registered in query_resolve.
            for &oid in &resolution.relation_oids {
                if let Some(mut queries) = self.cache.update_queries.get_mut(&oid) {
                    queries.queries.retain(|q| q.fingerprint != fingerprint);
                }
            }
            return Ok(());
        }

        // Register and populate
        let (generation, relations_changed) = self.cached_query_insert(
            fingerprint,
            resolution.relation_oids,
            resolution.base_query,
            Arc::clone(&resolution.resolved),
            resolution.max_limit,
            started_at,
            pinned,
        );
        self.state_view
            .metrics
            .entry(fingerprint)
            .or_insert_with(QueryMetrics::new);

        if relations_changed {
            self.publication_update().await?;
        }

        let work = self.population_work_build(
            fingerprint,
            generation,
            &resolution.resolved,
            resolution.max_limit,
        );
        self.populate_work_dispatch(work)?;
        trace!("population work queued for query {fingerprint}");
        Ok(())
    }

    /// Fast readmission for a CDC-invalidated query.
    /// Reuses existing metadata (relation_oids, resolved, update_queries) and
    /// dispatches population work without re-resolving tables.
    pub(super) async fn query_readmit(
        &mut self,
        fingerprint: u64,
        started_at: Instant,
    ) -> CacheResult<()> {
        debug!("readmitting query {fingerprint}");
        metrics::counter!(names::CACHE_READMISSIONS).increment(1);
        if let Some(mut m) = self.state_view.metrics.get_mut(&fingerprint) {
            m.readmission_count += 1;
        }

        // Assign new generation
        self.cache.generation_counter += 1;
        let new_generation = self.cache.generation_counter;
        self.cache.generations.insert(new_generation);

        // Extract data before remove/reinsert (generation is key2)
        let Some(mut cached) = self.cache.cached_queries.remove1(&fingerprint) else {
            return Ok(());
        };

        let resolved = Arc::clone(&cached.resolved);
        let max_limit = cached.max_limit;

        cached.generation = new_generation;
        cached.invalidated = false;
        cached.cached_bytes = 0;
        cached.registration_started_at = Some(started_at);
        self.cache.cached_queries.insert_overwrite(cached);
        if self.active_relations_rebuild() {
            self.publication_update().await?;
        }

        // Update state view to Loading
        self.state_view_update(
            fingerprint,
            CachedQueryState::Loading,
            new_generation,
            &resolved,
            max_limit,
        );

        let work = self.population_work_build(fingerprint, new_generation, &resolved, max_limit);
        self.populate_work_dispatch(work)?;
        trace!("readmission population queued for query {fingerprint}");
        Ok(())
    }

    /// Mark a query as ready after successful population.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn query_ready_mark(&mut self, fingerprint: u64, cached_bytes: usize) {
        trace!("query_ready_mark {fingerprint}");
        let update_info = if let Some(mut query) = self.cache.cached_queries.get1_mut(&fingerprint)
        {
            query.cached_bytes = cached_bytes;
            let started_at = query.registration_started_at.take();
            Some((
                query.generation,
                Arc::clone(&query.resolved),
                query.max_limit,
                started_at,
            ))
        } else {
            None
        };

        if let Some((generation, resolved, max_limit, started_at)) = update_info {
            // Record registration latency metric
            if let Some(started) = started_at {
                let latency = started.elapsed();
                metrics::histogram!(names::QUERY_REGISTRATION_LATENCY_SECONDS)
                    .record(latency.as_secs_f64());
            }

            // Update shared state view
            self.state_view_update(
                fingerprint,
                CachedQueryState::Ready,
                generation,
                &resolved,
                max_limit,
            );
            trace!("cached query ready, cached_bytes={cached_bytes} {fingerprint}");
        }
    }

    /// Clean up after a failed population.
    pub fn query_failed_cleanup(&mut self, fingerprint: u64) {
        trace!("query_failed_cleanup {fingerprint}");
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
            self.state_view.cached_queries.remove(&fingerprint);

            self.relations_dirty = true;
            debug!("cleaned up failed query {fingerprint}");
        }
    }

    /// Handle a limit bump: re-populate with a higher limit.
    ///
    /// Bumps the generation number, updates max_limit, and re-populates.
    /// During re-population the query state goes to Loading.
    #[instrument(skip_all)]
    pub async fn limit_bump_handle(
        &mut self,
        fingerprint: u64,
        new_max_limit: Option<u64>,
    ) -> CacheResult<()> {
        let Some(cached_query) = self.cache.cached_queries.get1(&fingerprint) else {
            trace!("limit bump: query {fingerprint} not found, skipping");
            return Ok(());
        };

        // Collect data needed before mutating
        let resolved = Arc::clone(&cached_query.resolved);
        let relation_oids = cached_query.relation_oids.clone();
        let old_generation = cached_query.generation;

        // Bump generation
        self.cache.generation_counter += 1;
        let new_generation = self.cache.generation_counter;
        self.cache.generations.insert(new_generation);
        self.cache.generations.remove(&old_generation);

        // Update cached query — must remove and reinsert because generation is key2
        if let Some(mut cached) = self.cache.cached_queries.remove1(&fingerprint) {
            cached.generation = new_generation;
            cached.max_limit = new_max_limit;
            cached.registration_started_at = Some(Instant::now());
            self.cache.cached_queries.insert_overwrite(cached);
        }

        // Update has_limit on update queries
        let has_limit = new_max_limit.is_some();
        for oid in &relation_oids {
            if let Some(mut queries) = self.cache.update_queries.get_mut(oid) {
                for uq in &mut queries.queries {
                    if uq.fingerprint == fingerprint {
                        uq.has_limit = has_limit;
                    }
                }
            }
        }

        // Set state to Loading while re-populating
        self.state_view_update(
            fingerprint,
            CachedQueryState::Loading,
            new_generation,
            &resolved,
            new_max_limit,
        );

        let work =
            self.population_work_build(fingerprint, new_generation, &resolved, new_max_limit);
        self.populate_work_dispatch(work)?;
        trace!("limit bump population queued for query {fingerprint}");
        Ok(())
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
}
