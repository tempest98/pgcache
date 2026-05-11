use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Instant;

use ecow::EcoString;
use tokio::sync::oneshot;
use tracing::{debug, error, info, instrument, trace};

use crate::cache::query::limit_rows_needed;
use crate::catalog::TableMetadata;
use crate::metrics::names;
use crate::query::ast::{Deparse, QueryBody, QueryExpr, TableNode};
use crate::query::constraints::{analyze_query_constraints, table_constraints_subsumed};
use crate::query::decorrelate::query_expr_decorrelate;
use crate::query::evaluate::resolved_where_expr_supported;
use crate::query::resolved::{
    ResolvedQueryExpr, ResolvedSelectNode, ResolvedTableNode, query_expr_resolve,
};
use crate::query::transform::predicate_pushdown_apply;
use crate::query::update::query_table_update_queries;
use crate::result::error_chain_format;
use crate::timing::{duration_to_ns_u64, duration_to_us_u64};

use super::super::{
    CacheError, CacheResult, MapIntoReport, ReportExt,
    messages::{AdmitAction, SubsumptionResult, WriterNotify},
    mv::{ShapeGate, shape_classify},
    query::CacheableQuery,
    types::{
        CachedQuery, CachedQueryState, QueryMetrics, SharedResolved, UpdateEvalStrategy,
        UpdateQueries, UpdateQuery, UpdateQuerySource,
    },
};
use super::{CacheWriter, PopulationWork};

/// Decide whether CDC can evaluate this update query's WHERE in Rust.
///
/// Conservative classifier: rejects anything the Rust evaluator can't decide
/// from a single CDC row. GROUP BY / HAVING are rejected because row-level
/// matching doesn't capture post-aggregation filtering. Non-FromClause sources
/// are rejected because their CDC semantics (subquery membership, outer join
/// null-padding cascade) aren't expressible as a row-level predicate.
fn update_eval_strategy_classify(
    resolved: &ResolvedQueryExpr,
    source: UpdateQuerySource,
) -> UpdateEvalStrategy {
    if source != UpdateQuerySource::FromClause {
        return UpdateEvalStrategy::PgEval;
    }
    let Some(select) = resolved.as_select() else {
        return UpdateEvalStrategy::PgEval;
    };
    if !select.is_single_table() {
        return UpdateEvalStrategy::PgEval;
    }
    if !select.group_by.is_empty() || select.having.is_some() {
        return UpdateEvalStrategy::PgEval;
    }
    let Some(where_expr) = &select.where_clause else {
        return UpdateEvalStrategy::LocalEval;
    };
    if resolved_where_expr_supported(where_expr) {
        UpdateEvalStrategy::LocalEval
    } else {
        UpdateEvalStrategy::PgEval
    }
}

/// Intermediate result from resolving a query before subsumption check or population.
struct QueryResolution {
    resolved: SharedResolved,
    /// Deparsed SQL body of `resolved`. Computed once here and reused on the
    /// serving hot path; see `CachedQuery.deparsed_sql`.
    deparsed_sql: EcoString,
    relation_oids: Vec<u32>,
    base_query: QueryExpr,
    max_limit: Option<u64>,
    /// MV shape gate. Also gates `max_limit`: reducer shapes force
    /// `max_limit = None` so source-row population isn't truncated in a way
    /// that would break re-evaluation (aggregates, GROUP BY, DISTINCT,
    /// windows all depend on the full input row set to produce correct
    /// result rows).
    shape_gate: ShapeGate,
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
    /// Classify a resolved query's shape for MV eligibility. Runs decorrelation
    /// first so the classification matches what first-population / rebuild will
    /// actually see (correlated subqueries get rewritten to JOIN + DISTINCT,
    /// which affects classification). Falls back to the original resolved form
    /// if decorrelation fails.
    ///
    /// NOTE: `population_work_build` and `update_queries_register` also decorrelate
    /// the same resolved query. Factoring these three callers onto a single
    /// decorrelation pass is a worthwhile follow-up but out of scope for v1.
    fn shape_gate_classify(&self, resolved: &SharedResolved) -> ShapeGate {
        let decorrelated = query_expr_decorrelate(resolved, &self.aggregate_functions).ok();
        let query: &ResolvedQueryExpr = match &decorrelated {
            Some(d) if d.transformed => &d.resolved,
            _ => resolved,
        };
        shape_classify(query, &self.aggregate_functions)
    }

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
            enqueued_at: Instant::now(),
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
            let eval_strategy = update_eval_strategy_classify(&update_resolved, source);
            let update_query = UpdateQuery {
                fingerprint,
                resolved: update_resolved,
                complexity,
                source,
                constraints,
                has_limit,
                eval_strategy,
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
        deparsed_sql: EcoString,
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
            deparsed_sql,
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
        let (base_query, user_max_limit) = base_query_prepare(&cacheable_query.query);

        self.cache_tables_ensure(&base_query, search_path).await?;

        let resolved: SharedResolved = Arc::new(
            query_expr_resolve(&base_query, &self.cache.tables, search_path)
                .map_err(|e| e.context_transform(CacheError::from))
                .attach_loc("resolving query expression")
                .map(predicate_pushdown_apply)?,
        );

        // Deparse once at registration. The output is a pure function of the
        // resolved AST, so every cache hit can splice it in instead of
        // re-running the deparse traversal.
        let deparse_start = Instant::now();
        let mut buf = String::with_capacity(256);
        resolved.deparse(&mut buf);
        let deparsed_sql: EcoString = buf.into();
        metrics::histogram!(names::CACHE_WRITER_RESOLVE_DEPARSE_SECONDS)
            .record(deparse_start.elapsed().as_secs_f64());

        // Classify the shape once here; `query_register` and MV setup both reuse
        // the result via `QueryResolution.shape_gate` to avoid re-running
        // decorrelation + classification.
        let shape_gate = self.shape_gate_classify(&resolved);

        // Reducer shapes transform row cardinality — applying the user's
        // LIMIT to source-row population truncates the input and breaks
        // re-evaluation (e.g. `SELECT count(*) FROM t LIMIT 3` cached with 3
        // source rows returns 3, not the real count). Force unbounded
        // population for those shapes.
        let max_limit = if shape_gate.is_reducer() {
            None
        } else {
            user_max_limit
        };

        let uq_start = Instant::now();
        let relation_oids =
            self.update_queries_register(fingerprint, &resolved, max_limit.is_some())?;
        metrics::histogram!(names::CACHE_WRITER_RESOLVE_UPDATE_QUERIES_REGISTER_SECONDS)
            .record(uq_start.elapsed().as_secs_f64());

        Ok(QueryResolution {
            resolved,
            deparsed_sql,
            relation_oids,
            base_query,
            max_limit,
            shape_gate,
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
    ) -> CacheResult<Option<(u64, SharedResolved, EcoString)>> {
        let subsume_start = Instant::now();

        let (generation, relations_changed) = self.cached_query_insert(
            fingerprint,
            resolution.relation_oids,
            resolution.base_query,
            Arc::clone(&resolution.resolved),
            resolution.deparsed_sql.clone(),
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
            error!(
                "subsumption generation set failed: {}",
                error_chain_format(e.current_context()),
            );
            return Ok(None);
        }

        let cache_exec_result = self
            .db_cache
            .query(resolution.deparsed_sql.as_str(), &[])
            .await
            .map_into_report::<CacheError>();

        // Always reset generation, even on failure
        let _ = self
            .db_cache
            .execute("SET mem.query_generation = 0", &[])
            .await;

        if let Err(e) = cache_exec_result {
            error!(
                "subsumption cache query failed: {}",
                error_chain_format(e.current_context()),
            );
            return Ok(None);
        }

        // Mark Ready in state view
        self.state_view_update(
            fingerprint,
            CachedQueryState::Ready,
            generation,
            &resolution.resolved,
            &resolution.deparsed_sql,
            resolution.max_limit,
        );

        // Clear registration_started_at to signal completion
        if let Some(mut q) = self.cache.cached_queries.get1_mut(&fingerprint) {
            q.registration_started_at = None;
        }

        // Record per-query metrics for subsumption
        if let Some(mut m) = self.state_view.metrics.get_mut(&fingerprint) {
            m.cached_since_ns =
                NonZeroU64::new(duration_to_ns_u64(self.state_view.started_at.elapsed()));
            m.subsumption_count += 1;
        }

        metrics::counter!(names::CACHE_SUBSUMPTIONS).increment(1);
        metrics::histogram!(names::CACHE_SUBSUMPTION_LATENCY_SECONDS)
            .record(subsume_start.elapsed().as_secs_f64());

        debug!("query subsumed {fingerprint}");
        Ok(Some((
            generation,
            resolution.resolved,
            resolution.deparsed_sql,
        )))
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
        let resolve_start = Instant::now();
        let resolution = self
            .query_resolve(fingerprint, cacheable_query, search_path)
            .await?;
        metrics::histogram!(names::CACHE_WRITER_REGISTER_RESOLVE_SECONDS)
            .record(resolve_start.elapsed().as_secs_f64());

        // Classify shape for MV eligibility. Sticky — readmit and limit-bump
        // paths preserve the result via state_view_update. Classification
        // was done in `query_resolve`; reuse it here.
        self.mv_state_set(fingerprint, resolution.shape_gate);

        // Phase 2: Subsumption check
        let subsumption_start = Instant::now();
        let subsumed = self.subsumption_check(&resolution);
        metrics::histogram!(names::CACHE_WRITER_REGISTER_SUBSUMPTION_CHECK_SECONDS)
            .record(subsumption_start.elapsed().as_secs_f64());

        if subsumed {
            // Phase 3a: Subsume — stamp rows, mark Ready
            let fallback_resolved = Arc::clone(&resolution.resolved);
            let fallback_max_limit = resolution.max_limit;

            let subsume_start = Instant::now();
            let subsume_result = self
                .query_subsume(fingerprint, resolution, started_at, pinned)
                .await?;
            metrics::histogram!(names::CACHE_WRITER_REGISTER_SUBSUME_SECONDS)
                .record(subsume_start.elapsed().as_secs_f64());
            match subsume_result {
                Some((generation, resolved, deparsed_sql)) => {
                    let _ = subsumption_tx.send(SubsumptionResult::Subsumed {
                        generation,
                        resolved,
                        deparsed_sql,
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
            self.cache
                .update_queries_remove_fingerprint(fingerprint, &resolution.relation_oids);
            return Ok(());
        }

        // Register and populate
        let insert_start = Instant::now();
        let (generation, relations_changed) = self.cached_query_insert(
            fingerprint,
            resolution.relation_oids,
            resolution.base_query,
            Arc::clone(&resolution.resolved),
            resolution.deparsed_sql,
            resolution.max_limit,
            started_at,
            pinned,
        );
        let now = NonZeroU64::new(duration_to_ns_u64(self.state_view.started_at.elapsed()));
        self.state_view
            .metrics
            .entry(fingerprint)
            .or_insert_with(|| QueryMetrics::new(now));
        metrics::histogram!(names::CACHE_WRITER_REGISTER_INSERT_SECONDS)
            .record(insert_start.elapsed().as_secs_f64());

        if relations_changed {
            let pub_start = Instant::now();
            self.publication_update().await?;
            metrics::histogram!(names::CACHE_WRITER_REGISTER_PUBLICATION_UPDATE_SECONDS)
                .record(pub_start.elapsed().as_secs_f64());
        }

        let dispatch_start = Instant::now();
        let work = self.population_work_build(
            fingerprint,
            generation,
            &resolution.resolved,
            resolution.max_limit,
        );
        self.populate_work_dispatch(work)?;
        metrics::histogram!(names::CACHE_WRITER_REGISTER_POPULATE_DISPATCH_SECONDS)
            .record(dispatch_start.elapsed().as_secs_f64());
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
        let deparsed_sql = cached.deparsed_sql.clone();
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
            &deparsed_sql,
            max_limit,
        );

        let work = self.population_work_build(fingerprint, new_generation, &resolved, max_limit);
        self.populate_work_dispatch(work)?;
        trace!("readmission population queued for query {fingerprint}");
        Ok(())
    }

    /// Mark a query as ready after successful population.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub fn query_ready_mark(&mut self, fingerprint: u64, cached_bytes: usize, row_count: u64) {
        trace!("query_ready_mark {fingerprint}");
        let update_info = if let Some(mut query) = self.cache.cached_queries.get1_mut(&fingerprint)
        {
            query.cached_bytes = cached_bytes;
            let started_at = query.registration_started_at.take();
            Some((
                query.generation,
                Arc::clone(&query.resolved),
                query.deparsed_sql.clone(),
                query.max_limit,
                started_at,
            ))
        } else {
            None
        };

        if let Some((generation, resolved, deparsed_sql, max_limit, started_at)) = update_info {
            // Record registration latency metric
            let population_duration_us = started_at.map(|s| {
                let latency = s.elapsed();
                metrics::histogram!(names::QUERY_REGISTRATION_LATENCY_SECONDS)
                    .record(latency.as_secs_f64());
                duration_to_us_u64(latency)
            });

            // Record per-query population metrics
            if let Some(mut m) = self.state_view.metrics.get_mut(&fingerprint) {
                m.population_count += 1;
                m.population_row_count = row_count;
                m.cached_since_ns =
                    NonZeroU64::new(duration_to_ns_u64(self.state_view.started_at.elapsed()));
                m.last_population_duration_us = population_duration_us.and_then(NonZeroU64::new);
            }

            // Update shared state view
            self.state_view_update(
                fingerprint,
                CachedQueryState::Ready,
                generation,
                &resolved,
                &deparsed_sql,
                max_limit,
            );
            // Notify coordinator to drain coalesced waiters
            let _ = self.notify_tx.send(WriterNotify::Ready {
                fingerprint,
                generation,
                resolved,
                deparsed_sql,
                max_limit,
            });

            trace!(
                "cached query ready, cached_bytes={cached_bytes} rows={row_count} {fingerprint}"
            );
        }
    }

    /// Clean up after a failed register/populate/readmit/limit-bump.
    ///
    /// Always clears the coordinator-owned `state_view` entry and drains any
    /// coalesced `waiting` requests via `WriterNotify::Failed` — even when the
    /// fingerprint never made it into `cached_queries` (e.g. the resolver
    /// rejected the query). Without this, a failed Register would leave
    /// `state_view` stuck in `Loading` and every subsequent client request for
    /// that fingerprint would coalesce into `waiting` and hang.
    pub fn query_failed_cleanup(&mut self, fingerprint: u64) {
        trace!("query_failed_cleanup {fingerprint}");

        match self.cache.cached_queries.remove1(&fingerprint) {
            Some(query) => {
                self.cache.generations.remove(&query.generation);
                self.cache
                    .update_queries_remove_fingerprint(fingerprint, &query.relation_oids);
                self.relations_dirty = true;
                debug!("cleaned up failed query {fingerprint}");
            }
            None => {
                // No cached_query but `update_queries_register` may have run
                // before the failure — sweep orphan entries by fingerprint.
                for mut entry in self.cache.update_queries.iter_mut() {
                    entry.queries.retain(|q| q.fingerprint != fingerprint);
                }
            }
        }

        self.state_view.cached_queries.remove(&fingerprint);
        let _ = self.notify_tx.send(WriterNotify::Failed { fingerprint });
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

        // A larger max_limit means the existing MV (sized for the old max_limit)
        // is short of rows. Flip Fresh → Dirty before any other mutation so
        // coordinators fall through while the new population runs.
        self.mv_dirty_mark(fingerprint);

        // Collect data needed before mutating
        let resolved = Arc::clone(&cached_query.resolved);
        let deparsed_sql = cached_query.deparsed_sql.clone();
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
            &deparsed_sql,
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

#[cfg(test)]
mod classify_tests {

    use super::*;

    use std::collections::HashMap;

    use iddqd::BiHashMap;
    use tokio_postgres::types::Type;

    use crate::cache::query::CacheableQuery;
    use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
    use crate::query::ast::query_expr_convert;
    use crate::query::resolved::query_expr_resolve;

    fn make_table(name: &str, oid: u32, columns: &[&str]) -> TableMetadata {
        let cols = ColumnStore::new(columns.iter().enumerate().map(|(i, c)| {
            let is_pk = i == 0;
            ColumnMetadata {
                name: (*c).into(),
                position: i16::try_from(i + 1).expect("column position fits in i16"),
                type_oid: if is_pk { 23 } else { 25 },
                data_type: if is_pk { Type::INT4 } else { Type::TEXT },
                type_name: if is_pk { "int4" } else { "text" }.into(),
                cache_type_name: if is_pk { "int4" } else { "text" }.into(),
                is_primary_key: is_pk,
            }
        }));
        TableMetadata {
            relation_oid: oid,
            name: name.into(),
            schema: "public".into(),
            primary_key_columns: vec![columns[0].to_owned()],
            columns: cols,
            indexes: Vec::new(),
        }
    }

    fn resolve(sql: &str, tables: &BiHashMap<TableMetadata>) -> ResolvedQueryExpr {
        let ast = pg_query::parse(sql).expect("parse");
        let query_expr = query_expr_convert(&ast).expect("convert");
        let cacheable = CacheableQuery::try_new(&query_expr, &HashMap::new()).expect("cacheable");
        query_expr_resolve(&cacheable.query, tables, &["public"]).expect("resolve")
    }

    fn classify_single_table(sql: &str) -> UpdateEvalStrategy {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(make_table("t", 1, &["id", "name", "status", "age"]));
        let resolved = resolve(sql, &tables);
        update_eval_strategy_classify(&resolved, UpdateQuerySource::FromClause)
    }

    #[test]
    fn simple_equality_is_local_eval() {
        assert_eq!(
            classify_single_table("SELECT * FROM t WHERE id = 5"),
            UpdateEvalStrategy::LocalEval
        );
    }

    #[test]
    fn no_where_is_local_eval() {
        assert_eq!(
            classify_single_table("SELECT * FROM t"),
            UpdateEvalStrategy::LocalEval
        );
    }

    #[test]
    fn and_or_with_comparisons_is_local_eval() {
        assert_eq!(
            classify_single_table("SELECT * FROM t WHERE (id = 1 OR id = 2) AND name IS NOT NULL"),
            UpdateEvalStrategy::LocalEval
        );
    }

    #[test]
    fn in_list_is_pg_eval() {
        // IN is a Multi op — not yet evaluable in Rust
        assert_eq!(
            classify_single_table("SELECT * FROM t WHERE id IN (1, 2, 3)"),
            UpdateEvalStrategy::PgEval
        );
    }

    #[test]
    fn like_is_pg_eval() {
        assert_eq!(
            classify_single_table("SELECT * FROM t WHERE name LIKE 'j%'"),
            UpdateEvalStrategy::PgEval
        );
    }

    #[test]
    fn group_by_is_pg_eval() {
        assert_eq!(
            classify_single_table("SELECT status, count(*) FROM t GROUP BY status"),
            UpdateEvalStrategy::PgEval
        );
    }

    #[test]
    fn multi_table_is_pg_eval() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(make_table("a", 1, &["id", "bid"]));
        tables.insert_overwrite(make_table("b", 2, &["id", "name"]));
        let resolved = resolve("SELECT * FROM a JOIN b ON a.bid = b.id", &tables);
        assert_eq!(
            update_eval_strategy_classify(&resolved, UpdateQuerySource::FromClause),
            UpdateEvalStrategy::PgEval
        );
    }

    #[test]
    fn non_fromclause_source_is_pg_eval() {
        use crate::cache::SubqueryKind;
        let resolved = resolve("SELECT * FROM t WHERE id = 5", &{
            let mut tables = BiHashMap::new();
            tables.insert_overwrite(make_table("t", 1, &["id", "name"]));
            tables
        });
        // Same query, but classified as a subquery-sourced update query
        assert_eq!(
            update_eval_strategy_classify(
                &resolved,
                UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
            ),
            UpdateEvalStrategy::PgEval
        );
    }
}
