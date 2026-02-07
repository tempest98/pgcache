use std::rc::Rc;
use std::time::Instant;

use postgres_protocol::escape;
use tokio::sync::mpsc;
use tokio::task::spawn_local;
use tokio_postgres::Row;
use tracing::{debug, error, instrument, trace};

use crate::catalog::TableMetadata;
use crate::metrics::names;

use super::super::types::{CachedQuery, CdcEventKind, SubqueryKind, UpdateQuery, UpdateQuerySource};
use super::super::{CacheError, CacheResult, MapIntoReport, ReportExt};
use super::CacheWriter;

impl CacheWriter {
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
            .update_queries_check_invalidate(relation_oid, &None, &row_data, None, CdcEventKind::Insert)
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

        self.update_queries_execute_concurrent(relation_oid, &row_data)
            .await?;

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
            CdcEventKind::Update,
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

        let matched = self
            .update_queries_execute_concurrent(relation_oid, &new_row_data)
            .await?;

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
    ///
    /// Deletes the row from cache tables and checks for subquery invalidations.
    /// For Exclusion subquery tables (NOT IN, NOT EXISTS), a DELETE shrinks the
    /// exclusion set, which grows the outer result set — requiring invalidation.
    #[instrument(skip_all)]
    pub async fn handle_delete(
        &mut self,
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

        // Check for subquery invalidations — Exclusion/Scalar subquery tables
        // need invalidation on DELETE because the outer result set may grow
        if self.cache.update_queries.contains_key(&relation_oid) {
            let fp_list = self
                .update_queries_check_invalidate(
                    relation_oid, &None, &row_data, None, CdcEventKind::Delete,
                )
                .attach_loc("checking delete invalidations")?;

            let invalidation_count = fp_list.len() as u64;
            for fp in fp_list {
                self.cache_query_invalidate(fp)
                    .await
                    .attach_loc("invalidating query on delete")?;
            }
            if invalidation_count > 0 {
                metrics::counter!(names::CACHE_INVALIDATIONS).increment(invalidation_count);
                self.state_gauges_update();
            }
        }

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

    /// Invalidate all cached queries that reference a table.
    pub(super) async fn cache_table_invalidate(&mut self, relation_oid: u32) -> CacheResult<()> {
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
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn cache_query_invalidate(&mut self, fingerprint: u64) -> CacheResult<()> {
        let Some(query) = self.cache.cached_queries.remove1(&fingerprint) else {
            return Ok(());
        };

        debug!("invalidating query {fingerprint}");

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

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn query_row_changes(
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

    /// Check if all WHERE constraints for a table match the given row values.
    /// Returns true if all constraints match (or no constraints exist for this table).
    fn row_constraints_match(
        &self,
        cached_query: &CachedQuery,
        table_metadata: &TableMetadata,
        row_data: &[Option<String>],
    ) -> bool {
        let Some(constraints) = cached_query
            .constraints
            .table_constraints
            .get(&table_metadata.name)
        else {
            return true;
        };

        for (column_name, constraint_value) in constraints {
            if let Some(column_meta) = table_metadata.columns.get1(column_name.as_str()) {
                let position = column_meta.position as usize - 1;
                if let Some(row_value) = row_data.get(position)
                    && !constraint_value.matches(row_value)
                {
                    return false;
                }
            }
        }

        true
    }

    /// Determine if a query should be invalidated when the row is not currently cached.
    /// Returns true if the query should be invalidated.
    fn row_uncached_invalidation_check(
        &self,
        update_query: &UpdateQuery,
        cached_query: &CachedQuery,
        table_metadata: &TableMetadata,
        row_data: &[Option<String>],
        key_data: Option<&[Option<String>]>,
        event_kind: CdcEventKind,
    ) -> bool {
        match update_query.source {
            UpdateQuerySource::Direct => {
                // Single-table queries don't need invalidation for uncached rows
                if update_query.resolved.is_single_table() {
                    return false;
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
                            return false;
                        }
                    }

                    return true;
                }

                // Check if row matches table constraints - invalidate only if it matches
                self.row_constraints_match(cached_query, table_metadata, row_data)
            }
            UpdateQuerySource::Subquery(kind) => {
                // Check constraints — if row doesn't match constraints for this
                // table, it's not relevant to the cached query
                if !self.row_constraints_match(cached_query, table_metadata, row_data) {
                    return false;
                }

                // Directional invalidation based on kind + event type.
                // Inclusion: set growth (INSERT) → outer grows → invalidate;
                //            set shrink (DELETE) → outer shrinks → skip.
                // Exclusion: inverse of Inclusion.
                // Scalar: always invalidate.
                match kind {
                    SubqueryKind::Scalar => true,
                    SubqueryKind::Inclusion => match event_kind {
                        CdcEventKind::Insert => true,
                        CdcEventKind::Delete => false,
                        CdcEventKind::Update => true,
                    },
                    SubqueryKind::Exclusion => match event_kind {
                        CdcEventKind::Insert => false,
                        CdcEventKind::Delete => true,
                        CdcEventKind::Update => true,
                    },
                }
            }
        }
    }

    /// Determine if a query should be invalidated when the row exists in cache.
    /// Returns true if the query should be invalidated.
    fn row_cached_invalidation_check(
        &self,
        update_query: &UpdateQuery,
        cached_query: &CachedQuery,
        table_metadata: &TableMetadata,
        row_data: &[Option<String>],
        row_changes: &Row,
    ) -> bool {
        // Subquery tables: always invalidate on UPDATE — column changes
        // could shift set membership in either direction
        if matches!(update_query.source, UpdateQuerySource::Subquery(_)) {
            return true;
        }

        for column in cached_query
            .constraints
            .table_join_columns(&table_metadata.name)
        {
            let column_changed = row_changes.get::<&str, bool>(column);

            if !column_changed {
                continue;
            }

            // Check constraints - skip if row doesn't match
            if !self.row_constraints_match(cached_query, table_metadata, row_data) {
                continue;
            }

            return true;
        }

        false
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) fn update_queries_check_invalidate(
        &self,
        relation_oid: u32,
        row_changes: &Option<&Row>,
        row_data: &[Option<String>],
        key_data: Option<&[Option<String>]>,
        event_kind: CdcEventKind,
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

            // Guard clause: handle uncached rows (INSERT or UPDATE where row not in cache)
            if row_changes.is_none() {
                if self.row_uncached_invalidation_check(
                    update_query,
                    cached_query,
                    table_metadata,
                    row_data,
                    key_data,
                    event_kind,
                ) {
                    fp_list.push(update_query.fingerprint);
                }
                continue;
            }

            // Main path: handle cached rows (UPDATE where row exists in cache)
            // row_changes is guaranteed to be Some here due to the guard clause above
            if let Some(row_changes) = row_changes
                && self.row_cached_invalidation_check(
                    update_query,
                    cached_query,
                    table_metadata,
                    row_data,
                    row_changes,
                )
            {
                fp_list.push(update_query.fingerprint);
            }
        }

        Ok(fp_list)
    }

    /// Execute update queries concurrently with lazy SQL building.
    /// Builds SQL only for each batch just before execution, avoiding
    /// unnecessary work when a match is found early.
    /// Returns true if any statement modified exactly 1 row.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn update_queries_execute_concurrent(
        &self,
        relation_oid: u32,
        row_data: &[Option<String>],
    ) -> CacheResult<bool> {
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

        let total_queries = update_queries.queries.len();
        trace!("update_queries_execute_concurrent start [{total_queries}]");

        if total_queries == 0 {
            return Ok(false);
        }

        let pool_size = self.cache_pool.len();
        let mut query_iter = update_queries.queries.iter().enumerate();
        let mut total_executed = 0;

        // Process in batches
        loop {
            let (tx, mut rx) =
                mpsc::channel::<(u64, Result<u64, tokio_postgres::Error>)>(pool_size);

            // Build SQL and spawn tasks inline, tracking batch count
            let mut batch_count = 0;
            for (idx, update_query) in query_iter.by_ref().take(pool_size) {
                let fingerprint = update_query.fingerprint;
                let sql = self.cache_upsert_with_predicate_sql(
                    &update_query.resolved,
                    table_metadata,
                    row_data,
                )?;

                let conn = self
                    .cache_pool
                    .get(idx % pool_size)
                    .map(Rc::clone)
                    .ok_or(CacheError::Other)?;
                let tx = tx.clone();

                spawn_local(async move {
                    let result = conn.execute(sql.as_str(), &[]).await;
                    let _ = tx.send((fingerprint, result)).await;
                });
                batch_count += 1;
            }
            drop(tx); // Close sender so rx completes when all tasks finish

            if batch_count == 0 {
                break;
            }

            // Collect results from this batch
            let mut batch_matched = false;
            let mut batch_error: Option<CacheError> = None;

            while let Some((fingerprint, result)) = rx.recv().await {
                total_executed += 1;
                match result {
                    Ok(1) => {
                        trace!("update_queries matched fingerprint {fingerprint}");
                        batch_matched = true;
                    }
                    Ok(n) if n > 1 => {
                        batch_error = Some(CacheError::TooManyModifiedRows);
                    }
                    Ok(_) => {}
                    Err(e) => {
                        error!("sql execution error for fingerprint {fingerprint}: {e}");
                        batch_error = Some(CacheError::PgError(e));
                    }
                }
            }

            // Check for errors
            if let Some(err) = batch_error {
                return Err(err.into());
            }

            // If we found a match in this batch, we're done
            if batch_matched {
                trace!(
                    "update_queries_execute_concurrent done [{total_executed}/{total_queries}] - matched"
                );
                return Ok(true);
            }
        }

        trace!(
            "update_queries_execute_concurrent done [{total_executed}/{total_queries}] - no match"
        );
        Ok(false)
    }

    pub(super) fn cache_upsert_with_predicate_sql(
        &self,
        resolved: &crate::query::resolved::ResolvedQueryExpr,
        table_metadata: &crate::catalog::TableMetadata,
        row_data: &[Option<String>],
    ) -> CacheResult<String> {
        // Extract SELECT body - update queries are always SELECT queries
        let resolved_select = resolved.as_select().ok_or(CacheError::InvalidQuery)?;

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

        let value_select = crate::query::transform::resolved_select_node_table_replace_with_values(
            resolved_select,
            table_metadata,
            row_data,
        )
        .map_err(|e| CacheError::from(e.into_current_context()))?;
        let mut select = String::with_capacity(1024);
        crate::query::ast::Deparse::deparse(&value_select, &mut select);

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

    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) fn cache_delete_sql(
        &self,
        table_metadata: &crate::catalog::TableMetadata,
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
