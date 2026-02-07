use std::time::Instant;

use rootcause::Report;
use tracing::{debug, error, info, instrument, trace};

use crate::metrics::names;
use crate::query::ast::TableNode;
use crate::query::constraints::analyze_query_constraints;
use crate::query::resolved::{ResolvedTableNode, query_expr_resolve};
use crate::query::transform::query_table_update_queries;
use crate::catalog::TableMetadata;

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
    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn query_register(
        &mut self,
        fingerprint: u64,
        cacheable_query: &CacheableQuery,
        search_path: &[&str],
        started_at: Instant,
    ) -> CacheResult<()> {
        let mut relation_oids = Vec::new();

        for table_node in cacheable_query.query.nodes::<TableNode>() {
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

        let resolved = query_expr_resolve(&cacheable_query.query, &self.cache.tables, search_path)
            .map_err(|e| Report::from(CacheError::from(e.into_current_context())))
            .attach_loc("resolving query expression")?;

        for (table_node, update_query_expr, source) in query_table_update_queries(cacheable_query) {
            let schema = self
                .table_schema_resolve(
                    table_node.name.as_str(),
                    table_node.schema.as_deref(),
                    search_path,
                )
                .await?;
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
                query_expr_resolve(&update_query_expr, &self.cache.tables, search_path)
                    .map_err(|e| Report::from(CacheError::from(e.into_current_context())))
                    .attach_loc("resolving update query expression")?;

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
            query: cacheable_query.query.clone(),
            resolved: resolved.clone(),
            cached_bytes: 0,
            registration_started_at: Some(started_at),
        };

        self.cache.cached_queries.insert_overwrite(cached_query);
        self.active_relations_rebuild();
        trace!("cached query loading");

        // Extract SELECT branches for population
        // Each branch is processed independently, which handles set operations correctly
        let branches: Vec<_> = resolved
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
        };

        self.populate_work_dispatch(work)?;
        trace!("population work queued for query {fingerprint}");
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
