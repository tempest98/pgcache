use std::fmt::Write;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use postgres_protocol::escape;
use rootcause::prelude::ResultExt;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio_postgres::{Client, SimpleColumn, SimpleQueryMessage, SimpleQueryRow};
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
            work.max_limit,
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
    max_limit: Option<u64>,
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
                    name: Some(table_node.name.to_string()),
                })?;

            let stream_start = Instant::now();
            let bytes =
                population_stream(&db_origin, db_cache, table, table_node, branch, max_limit)
                    .await?;
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

/// Pre-computed parts of the batched INSERT...ON CONFLICT statement.
struct InsertStatement {
    prefix: String,
    suffix: String,
    /// Column positions of primary key fields, for detecting NULL-padded phantom rows.
    pkey_positions: Vec<usize>,
    num_columns: usize,
}

/// Build the INSERT statement template from the row description and table metadata.
///
/// Pre-computes column lists, conflict clause, and primary key positions so that
/// the streaming loop only needs to format value tuples.
fn insert_statement_build(
    table: &TableMetadata,
    row_description: &Arc<[SimpleColumn]>,
) -> InsertStatement {
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

    let pkey_positions: Vec<usize> = table
        .primary_key_columns
        .iter()
        .filter_map(|pk| row_description.iter().position(|c| c.name() == pk.as_str()))
        .collect();

    let columns_joined = columns.join(",");
    let pkey_joined = pkey_columns.join(",");
    let update_joined = update_columns.join(", ");

    InsertStatement {
        prefix: format!(
            "INSERT INTO \"{}\".\"{}\"({columns_joined}) VALUES ",
            table.schema, table.name
        ),
        suffix: format!(" ON CONFLICT ({pkey_joined}) DO UPDATE SET {update_joined}"),
        pkey_positions,
        num_columns: row_description.len(),
    }
}

/// Convert a streamed row into a SQL value tuple string.
///
/// Returns `None` for phantom rows (NULL primary keys from outer joins).
/// Returns the tuple string and the number of bytes in the row's values.
fn row_to_tuple(
    row: &SimpleQueryRow,
    insert: &InsertStatement,
    values_buf: &mut Vec<String>,
    tuple_buf: &mut String,
) -> Option<(String, usize)> {
    // Skip NULL-padded phantom rows from outer joins
    if insert
        .pkey_positions
        .iter()
        .any(|&pos| row.get(pos).is_none())
    {
        return None;
    }

    let mut row_bytes = 0;
    values_buf.clear();
    for idx in 0..insert.num_columns {
        let value = row.get(idx);
        row_bytes += value.map_or(0, |v| v.len());
        values_buf.push(
            value
                .map(escape::escape_literal)
                .unwrap_or_else(|| "NULL".to_owned()),
        );
    }

    tuple_buf.clear();
    tuple_buf.push('(');
    tuple_buf.push_str(&values_buf.join(","));
    tuple_buf.push(')');

    Some((tuple_buf.clone(), row_bytes))
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
    table_node: &ResolvedTableNode,
    branch: &ResolvedSelectNode,
    max_limit: Option<u64>,
) -> CacheResult<usize> {
    // Build the SELECT query
    let select_columns = table.resolved_select_columns(table_node.alias.as_deref());
    let new_ast = resolved_select_node_replace(branch, select_columns);
    let mut buf = String::with_capacity(1024);
    new_ast.deparse(&mut buf);

    if let Some(limit) = max_limit {
        write!(buf, " LIMIT {limit}").ok();
    }

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
        Some(Err(e)) => {
            let report: CacheResult<usize> = Err(CacheError::from(e).into());
            return report.attach(buf);
        }
        None => return Ok(0),
    };

    let insert = insert_statement_build(table, &row_description);

    let mut cached_bytes: usize = 0;
    let mut value_tuples: Vec<String> = Vec::with_capacity(POPULATION_INSERT_BATCH_SIZE);
    let mut values_buf: Vec<String> = Vec::with_capacity(insert.num_columns);
    let mut tuple_buf = String::new();

    loop {
        match stream.next().await {
            Some(Ok(SimpleQueryMessage::Row(row))) => {
                if let Some((tuple, bytes)) =
                    row_to_tuple(&row, &insert, &mut values_buf, &mut tuple_buf)
                {
                    cached_bytes += bytes;
                    value_tuples.push(tuple);

                    if value_tuples.len() >= POPULATION_INSERT_BATCH_SIZE {
                        population_batch_flush(
                            db_cache,
                            &insert.prefix,
                            &insert.suffix,
                            &mut value_tuples,
                        )
                        .await?;
                    }
                }
            }
            Some(Ok(SimpleQueryMessage::CommandComplete(_))) => break,
            Some(Ok(_)) => continue,
            Some(Err(e)) => return Err(CacheError::from(e).into()),
            None => break,
        }
    }

    if !value_tuples.is_empty() {
        population_batch_flush(db_cache, &insert.prefix, &insert.suffix, &mut value_tuples)
            .await?;
    }

    Ok(cached_bytes)
}

/// Flush a batch of value tuples as a single multi-row INSERT statement.
async fn population_batch_flush(
    db_cache: &Client,
    insert_prefix: &str,
    insert_suffix: &str,
    value_tuples: &mut Vec<String>,
) -> CacheResult<()> {
    let mut sql = String::with_capacity(
        insert_prefix.len()
            + insert_suffix.len()
            + value_tuples.iter().map(|t| t.len() + 1).sum::<usize>(),
    );
    sql.push_str(insert_prefix);
    sql.push_str(&value_tuples.join(","));
    sql.push_str(insert_suffix);

    db_cache
        .batch_execute(&sql)
        .await
        .map_into_report::<CacheError>()?;

    value_tuples.clear();
    Ok(())
}
