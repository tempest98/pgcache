#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

//! Regression test for PGC-96: source-row cache generated
//! `INSERT ... ON CONFLICT (<pk>) DO UPDATE SET ` with an empty SET list for
//! tables whose only columns are primary keys. PG rejects the SQL syntax and
//! population silently failed with the cache entry stuck at `Loading`.
//!
//! Covers three code paths that generate upsert SQL:
//!   1. `insert_statement_build` in writer/population.rs (initial population).
//!   2. `handle_pg_eval_cdc_sql` in writer/cdc.rs (CDC PG-eval upsert).
//!   3. `cache_upsert_unconditional_sql` in writer/cdc.rs (CDC LocalEval fast path).
//!
//! All three must emit `DO NOTHING` instead of `DO UPDATE SET` when no non-PK
//! columns exist.

use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, wait_cache_load, wait_for_cdc};

mod util;

/// PK-only table: verifies initial population and CDC-driven in-place updates
/// both succeed.
#[tokio::test]
async fn test_pk_only_table_caching() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query("CREATE TABLE pk_only (id integer primary key)", &[])
        .await?;
    ctx.query(
        "INSERT INTO pk_only (id) VALUES (1), (2), (3)",
        &[],
    )
    .await?;

    // First query — cache miss, exercises insert_statement_build (site 1).
    let m = ctx.metrics().await?;
    let res = ctx.simple_query("SELECT id FROM pk_only").await?;
    let rows: Vec<_> = res
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .collect();
    assert_eq!(rows.len(), 3, "first query returns 3 rows");
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // Second query — cache hit. Proves population (site 1) succeeded.
    let res = ctx.simple_query("SELECT id FROM pk_only").await?;
    let rows: Vec<_> = res
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .collect();
    assert_eq!(rows.len(), 3);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // CDC INSERT — single-table unconditional upsert path (site 3,
    // cache_upsert_unconditional_sql in LocalEval).
    ctx.origin_query("INSERT INTO pk_only (id) VALUES (4)", &[])
        .await?;
    wait_for_cdc().await;

    let res = ctx.simple_query("SELECT id FROM pk_only").await?;
    let rows: Vec<_> = res
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .collect();
    assert_eq!(rows.len(), 4, "CDC INSERT landed in cache");
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}
