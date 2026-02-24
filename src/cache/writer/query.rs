use std::time::Instant;

use tracing::{debug, error, info, instrument, trace};

use crate::cache::query::limit_rows_needed;
use crate::catalog::TableMetadata;
use crate::metrics::names;
use crate::query::ast::{QueryBody, TableNode};
use crate::query::constraints::analyze_query_constraints;
use crate::query::decorrelate::query_expr_decorrelate;
use crate::query::resolved::{ResolvedTableNode, query_expr_resolve};
use crate::query::transform::predicate_pushdown_apply;
use crate::query::update::query_table_update_queries;

use super::super::{
    CacheError, CacheResult, ReportExt,
    query::CacheableQuery,
    types::{CachedQuery, CachedQueryState, UpdateQueries, UpdateQuery},
};
use super::{CacheWriter, POPULATE_POOL_SIZE, PopulationWork};

impl CacheWriter {
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
        self.populate_next = (self.populate_next + 1) % POPULATE_POOL_SIZE;

        let Some(tx) = self.populate_txs.get(idx) else {
            return Err(CacheError::Other.into());
        };

        if tx.send(work).is_err() {
            error!("population worker {idx} channel closed");
        }

        Ok(())
    }

    /// Registers a query in the cache and spawns background population.
    /// Registration is synchronous (updates Cache state), population is async.
    /// If the query was previously invalidated (CLOCK policy), takes the fast
    /// readmission path that reuses existing metadata.
    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn query_register(
        &mut self,
        fingerprint: u64,
        cacheable_query: &CacheableQuery,
        search_path: &[&str],
        started_at: Instant,
    ) -> CacheResult<()> {
        // Fast readmission: reuse metadata from invalidated entry
        if let Some(query) = self.cache.cached_queries.get1(&fingerprint)
            && query.invalidated
        {
            return self.query_readmit(fingerprint, started_at);
        }

        let mut relation_oids = Vec::new();

        // Extract max_limit from the incoming query's LIMIT clause,
        // then strip LIMIT from the stored query (base query only).
        // For set operations, force max_limit = None — population runs per-branch
        // so a top-level LIMIT can't be applied during population. All rows are
        // cached, and the incoming LIMIT is applied at serve time.
        let is_set_op = matches!(cacheable_query.query.body, QueryBody::SetOp(_));
        let max_limit = if is_set_op {
            None
        } else {
            limit_rows_needed(&cacheable_query.query.limit)
        };
        let has_limit = max_limit.is_some();
        let mut base_query = cacheable_query.query.clone();
        base_query.limit = None;

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

        let resolved = query_expr_resolve(&base_query, &self.cache.tables, search_path)
            .map_err(|e| e.context_transform(CacheError::from))
            .attach_loc("resolving query expression")
            .map(predicate_pushdown_apply)?;

        // Decorrelate correlated subqueries for update query generation.
        // Works entirely on resolved AST — no re-resolution needed.
        let decorrelated = query_expr_decorrelate(&resolved, &self.aggregate_functions)
            .map_err(|e| e.context_transform(CacheError::from))
            .attach_loc("decorrelating correlated subqueries")?;
        let update_source = if decorrelated.transformed {
            &decorrelated.resolved
        } else {
            &resolved
        };

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

        // Assign generation number synchronously (before any await)
        self.cache.generation_counter += 1;
        let generation = self.cache.generation_counter;
        self.cache.generations.insert(generation);

        // Create CachedQuery entry with Loading state
        let cached_query = CachedQuery {
            fingerprint,
            generation,
            relation_oids: relation_oids.clone(),
            query: base_query,
            resolved: resolved.clone(),
            max_limit,
            cached_bytes: 0,
            registration_started_at: Some(started_at),
            invalidated: false,
        };

        self.cache.cached_queries.insert_overwrite(cached_query);
        self.active_relations_rebuild();
        trace!("cached query loading");

        // Extract SELECT branches for population.
        // When decorrelation was applied, use the decorrelated form so that inner
        // correlated tables are merged into JOINs. This avoids extracting standalone
        // correlated branches that reference outer-scope columns and can't run independently.
        let population_source = if decorrelated.transformed {
            &decorrelated.resolved
        } else {
            &resolved
        };
        let branches: Vec<_> = population_source
            .select_nodes()
            .into_iter()
            .cloned()
            .collect();

        // Collect all unique table OIDs across all branches for metadata lookup
        let branch_relation_oids: Vec<u32> = branches
            .iter()
            .flat_map(|branch| branch.nodes::<ResolvedTableNode>())
            .map(|tn| tn.relation_oid)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        // Collect table metadata needed for population
        let table_metadata: Vec<TableMetadata> = branch_relation_oids
            .iter()
            .filter_map(|oid| self.cache.tables.get1(oid).cloned())
            .collect();

        // Create work item for population worker
        let work = PopulationWork {
            fingerprint,
            generation,
            table_metadata,
            branches,
            max_limit,
        };

        self.populate_work_dispatch(work)?;
        trace!("population work queued for query {fingerprint}");
        Ok(())
    }

    /// Fast readmission for a CDC-invalidated query.
    /// Reuses existing metadata (relation_oids, resolved, update_queries) and
    /// dispatches population work without re-resolving tables.
    fn query_readmit(&mut self, fingerprint: u64, started_at: Instant) -> CacheResult<()> {
        info!("readmitting query {fingerprint}");
        metrics::counter!(names::CACHE_READMISSIONS).increment(1);

        // Assign new generation
        self.cache.generation_counter += 1;
        let new_generation = self.cache.generation_counter;
        self.cache.generations.insert(new_generation);

        // Extract data before remove/reinsert (generation is key2)
        let Some(mut cached) = self.cache.cached_queries.remove1(&fingerprint) else {
            return Ok(());
        };

        let resolved = cached.resolved.clone();
        let max_limit = cached.max_limit;

        cached.generation = new_generation;
        cached.invalidated = false;
        cached.cached_bytes = 0;
        cached.registration_started_at = Some(started_at);
        self.cache.cached_queries.insert_overwrite(cached);
        self.active_relations_rebuild();

        // Update state view to Loading
        self.state_view_update(
            fingerprint,
            CachedQueryState::Loading,
            new_generation,
            &resolved,
            max_limit,
        );

        // Use decorrelated form for population branches to avoid standalone
        // correlated branches that can't run independently.
        let population_resolved = query_expr_decorrelate(&resolved, &self.aggregate_functions)
            .map(|d| if d.transformed { d.resolved } else { resolved.clone() })
            .unwrap_or_else(|_| resolved.clone());

        let branches: Vec<_> = population_resolved
            .select_nodes()
            .into_iter()
            .cloned()
            .collect();

        let branch_relation_oids: Vec<u32> = branches
            .iter()
            .flat_map(|branch| branch.nodes::<ResolvedTableNode>())
            .map(|tn| tn.relation_oid)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let table_metadata: Vec<TableMetadata> = branch_relation_oids
            .iter()
            .filter_map(|oid| self.cache.tables.get1(oid).cloned())
            .collect();

        let work = PopulationWork {
            fingerprint,
            generation: new_generation,
            table_metadata,
            branches,
            max_limit,
        };

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
                query.resolved.clone(),
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
            if let Ok(mut view) = self.state_view.write() {
                view.cached_queries.remove(&fingerprint);
            }

            self.active_relations_rebuild();
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
        let resolved = cached_query.resolved.clone();
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

        // Use decorrelated form for population branches (same rationale as query_register)
        let population_resolved = query_expr_decorrelate(&resolved, &self.aggregate_functions)
            .map(|d| if d.transformed { d.resolved } else { resolved.clone() })
            .unwrap_or_else(|_| resolved.clone());

        let branches: Vec<_> = population_resolved
            .select_nodes()
            .into_iter()
            .cloned()
            .collect();

        let branch_relation_oids: Vec<u32> = branches
            .iter()
            .flat_map(|branch| branch.nodes::<ResolvedTableNode>())
            .map(|tn| tn.relation_oid)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let table_metadata: Vec<TableMetadata> = branch_relation_oids
            .iter()
            .filter_map(|oid| self.cache.tables.get1(oid).cloned())
            .collect();

        let work = PopulationWork {
            fingerprint,
            generation: new_generation,
            table_metadata,
            branches,
            max_limit: new_max_limit,
        };

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
