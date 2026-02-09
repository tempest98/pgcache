use std::rc::Rc;
use std::time::Instant;

use postgres_protocol::escape;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio_postgres::{Client, SimpleQueryMessage};
use tracing::{debug, error, trace};

use crate::catalog::TableMetadata;
use crate::metrics::names;
use crate::query::ast::Deparse;
use crate::query::resolved::{ResolvedSelectNode, ResolvedTableNode};

use super::super::{CacheError, CacheResult, MapIntoReport, messages::QueryCommand};
use super::PopulationWork;

/// Persistent population worker that processes work items from a channel.
/// Each worker owns its own cache database connection.
pub async fn population_worker(
    id: usize,
    mut rx: UnboundedReceiver<PopulationWork>,
    db_origin: Rc<Client>,
    db_cache: Client,
    query_tx: UnboundedSender<QueryCommand>,
) {
    debug!("population worker {id} started");

    while let Some(work) = rx.recv().await {
        metrics::gauge!(names::CACHE_POPULATION_WORKER_QUEUE, "worker" => id.to_string())
            .set(rx.len() as f64);

        let result = population_task(
            work.fingerprint,
            work.generation,
            &work.branches,
            &work.table_metadata,
            Rc::clone(&db_origin),
            &db_cache,
        )
        .await;

        match result {
            Ok(cached_bytes) => {
                if query_tx
                    .send(QueryCommand::Ready {
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
                if query_tx
                    .send(QueryCommand::Failed {
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
///
/// For queries with multiple SELECT branches (set operations), each branch is
/// processed independently. This correctly handles UNION/INTERSECT/EXCEPT where
/// different branches may reference different tables with different columns.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn population_task(
    fingerprint: u64,
    generation: u64,
    branches: &[ResolvedSelectNode],
    table_metadata: &[TableMetadata],
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

    // Process each SELECT branch independently
    // For simple SELECT queries, there's just one branch
    // For set operations, each branch fetches its own tables
    for branch in branches {
        // Find tables directly in this branch's FROM clause (not in subqueries).
        // Subquery tables are handled as separate branches.
        for table_node in branch.direct_table_nodes() {
            let table = table_metadata
                .iter()
                .find(|t| t.relation_oid == table_node.relation_oid)
                .ok_or(CacheError::UnknownTable {
                    oid: Some(table_node.relation_oid),
                    name: Some(table_node.name.clone()),
                })?;

            // Fetch from origin using this branch
            let fetch_start = Instant::now();
            let rows = population_fetch(&db_origin, table, branch).await?;
            let fetch_elapsed = fetch_start.elapsed();

            // Populate cache (UPSERT handles duplicates if same table in multiple branches)
            let insert_start = Instant::now();
            let bytes = population_insert(db_cache, table, &rows).await?;
            let insert_elapsed = insert_start.elapsed();

            total_bytes += bytes;

            trace!(
                "population table {}.{} fetch={:?} insert={:?} bytes={bytes}",
                table.schema, table.name, fetch_elapsed, insert_elapsed
            );
        }
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

/// Fetch data from origin database for a single table using a SELECT branch.
async fn population_fetch(
    db_origin: &Client,
    table: &TableMetadata,
    branch: &ResolvedSelectNode,
) -> CacheResult<Vec<SimpleQueryMessage>> {
    // Find the table's alias in this branch (if any)
    let maybe_alias = branch
        .nodes::<ResolvedTableNode>()
        .find(|tn| tn.relation_oid == table.relation_oid)
        .and_then(|t| t.alias.as_deref());

    // Use table metadata to get column list
    let select_columns = table.resolved_select_columns(maybe_alias);

    // Build query with table columns
    use crate::query::transform::resolved_select_node_replace;
    let new_ast = resolved_select_node_replace(branch, select_columns);
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
