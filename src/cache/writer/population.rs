use std::rc::Rc;
use std::time::Instant;

use postgres_protocol::escape;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio_postgres::{Client, SimpleQueryMessage};
use tokio_stream::StreamExt;
use tracing::{debug, error, trace};

use crate::catalog::TableMetadata;
use crate::metrics::names;
use crate::query::ast::Deparse;
use crate::query::resolved::{ResolvedSelectNode, ResolvedTableNode};
use crate::query::transform::resolved_select_node_replace;

use super::super::{CacheError, CacheResult, MapIntoReport, messages::QueryCommand};
use super::PopulationWork;

/// Number of rows to batch per INSERT statement sent to the cache database.
const POPULATION_INSERT_BATCH_SIZE: usize = 200;

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

            let stream_start = Instant::now();
            let bytes = population_stream(&db_origin, db_cache, table, branch).await?;
            let stream_elapsed = stream_start.elapsed();

            total_bytes += bytes;

            trace!(
                "population table {}.{} elapsed={:?} bytes={bytes}",
                table.schema, table.name, stream_elapsed
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

/// Fetch data from origin and stream it into the cache database in batches.
///
/// Streams rows from origin via SimpleQueryStream, batching INSERT...ON CONFLICT
/// statements in groups of POPULATION_INSERT_BATCH_SIZE rows. This avoids materializing
/// the entire result set in memory.
async fn population_stream(
    db_origin: &Client,
    db_cache: &Client,
    table: &TableMetadata,
    branch: &ResolvedSelectNode,
) -> CacheResult<usize> {
    // Build the SELECT query
    let maybe_alias = branch
        .nodes::<ResolvedTableNode>()
        .find(|tn| tn.relation_oid == table.relation_oid)
        .and_then(|t| t.alias.as_deref());

    let select_columns = table.resolved_select_columns(maybe_alias);
    let new_ast = resolved_select_node_replace(branch, select_columns);
    let mut buf = String::with_capacity(1024);
    new_ast.deparse(&mut buf);

    // Start streaming from origin
    let stream = db_origin
        .simple_query_raw(&buf)
        .await
        .map_into_report::<CacheError>()?;
    tokio::pin!(stream);

    // Extract RowDescription (first item from stream)
    let row_description = match stream.next().await {
        Some(Ok(SimpleQueryMessage::RowDescription(cols))) => cols,
        Some(Ok(_)) => return Err(CacheError::InvalidMessage.into()),
        Some(Err(e)) => return Err(CacheError::from(e).into()),
        None => return Ok(0),
    };

    // Pre-compute column and conflict clause strings
    let columns: Vec<String> = row_description
        .iter()
        .map(|c| format!("\"{}\"", c.name()))
        .collect();

    let pkey_columns: Vec<String> = table
        .primary_key_columns
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect();

    let update_columns: Vec<String> = columns
        .iter()
        .filter(|c| !pkey_columns.contains(c))
        .map(|c| format!("{c} = EXCLUDED.{c}"))
        .collect();

    let columns_joined = columns.join(",");
    let pkey_joined = pkey_columns.join(",");
    let update_joined = update_columns.join(", ");

    let schema = &table.schema;
    let table_name = &table.name;

    let mut cached_bytes: usize = 0;
    let mut batch: Vec<String> = Vec::with_capacity(POPULATION_INSERT_BATCH_SIZE);

    // Pre-compute the fixed prefix and suffix for INSERT statements
    let insert_prefix = format!(
        "INSERT INTO \"{schema}\".\"{table_name}\"({columns_joined}) VALUES ("
    );
    let insert_suffix = format!(
        ") ON CONFLICT ({pkey_joined}) DO UPDATE SET {update_joined}"
    );

    let num_columns = row_description.len();
    let mut values: Vec<String> = Vec::with_capacity(num_columns);
    let mut insert_sql = String::new();

    // Consume stream, batching rows into INSERT statements
    loop {
        match stream.next().await {
            Some(Ok(SimpleQueryMessage::Row(row))) => {
                values.clear();
                for idx in 0..num_columns {
                    let value = row.get(idx);
                    cached_bytes += value.map_or(0, |v| v.len());
                    values.push(
                        value
                            .map(escape::escape_literal)
                            .unwrap_or_else(|| "NULL".to_owned()),
                    );
                }

                insert_sql.clear();
                insert_sql.push_str(&insert_prefix);
                insert_sql.push_str(&values.join(","));
                insert_sql.push_str(&insert_suffix);

                batch.push(insert_sql.clone());

                if batch.len() >= POPULATION_INSERT_BATCH_SIZE {
                    db_cache
                        .batch_execute(&batch.join(";"))
                        .await
                        .map_into_report::<CacheError>()?;
                    batch.clear();
                }
            }
            Some(Ok(SimpleQueryMessage::CommandComplete(_))) => break,
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Err(CacheError::from(e).into()),
            None => break,
        }
    }

    // Flush remaining rows
    if !batch.is_empty() {
        db_cache
            .batch_execute(&batch.join(";"))
            .await
            .map_into_report::<CacheError>()?;
    }

    Ok(cached_bytes)
}
