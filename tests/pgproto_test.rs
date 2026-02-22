#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

use std::io::Error;

use crate::util::{
    TestContext, assert_cache_hit, assert_cache_miss, metrics_delta, pgproto_run, wait_cache_load,
};

mod util;

/// Assert pgproto output contains expected backend response messages.
/// pgproto debug output shows `<= BE MessageType` lines.
fn assert_pgproto_select(output: &str, expected_rows: usize) {
    assert!(
        output.contains("ReadyForQuery"),
        "expected ReadyForQuery in output:\n{output}",
    );
    let row_count = output.matches("<= BE DataRow").count();
    assert!(
        row_count >= expected_rows,
        "expected at least {expected_rows} DataRow messages, got {row_count}:\n{output}",
    );
}

/// Integration tests for extended protocol variations using pgproto.
///
/// These tests exercise wire-level protocol sequences that tokio-postgres cannot
/// produce, such as Flush-before-Sync (JDBC pattern), multi-Execute pipelines,
/// named statement reuse without re-Parse, and named portals.
#[tokio::test]
async fn test_pgproto_extended_protocol() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create test table and seed data
    ctx.query(
        "CREATE TABLE proto_test (id integer PRIMARY KEY, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO proto_test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')",
        &[],
    )
    .await?;

    // --- Scenario 1: Parse/Bind/Execute/Sync (standard cacheable) ---
    // First run: cache miss
    let m = ctx.metrics().await?;
    let output = pgproto_run(
        ctx.cache_port,
        "tests/data/pgproto/parse_bind_execute_sync.data",
    );
    assert_pgproto_select(&output, 3);
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // Second run: cache hit (same query, new connection)
    let output = pgproto_run(
        ctx.cache_port,
        "tests/data/pgproto/parse_bind_execute_sync.data",
    );
    assert_pgproto_select(&output, 3);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    // --- Scenario 2: Named statement reuse (Bind-only after prior Parse) ---
    // Parse once with Bind/Execute/Sync, then Bind-only/Execute/Sync.
    // The proxy proactively forwards Parse to origin on cache hit,
    // so the Bind-only second cycle works without re-Parse.
    let m = ctx.metrics().await?;
    let output = pgproto_run(ctx.cache_port, "tests/data/pgproto/bind_reuse_sync.data");
    // Two Sync boundaries should each return 3 rows = 6 DataRows total
    assert_pgproto_select(&output, 6);
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert!(
        delta.queries_total >= 2,
        "expected at least 2 queries from bind reuse, got {}",
        delta.queries_total,
    );

    // --- Scenario 3: Named portal ---
    // Bind to a named portal "P1", then Execute referencing that portal.
    // Verifies the proxy correctly tracks named portals and resolves them
    // during Execute for cacheability routing.
    let m = ctx.metrics().await?;
    let output = pgproto_run(ctx.cache_port, "tests/data/pgproto/named_portal.data");
    assert_pgproto_select(&output, 3);
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert!(
        delta.queries_total >= 1,
        "expected at least 1 query from named portal, got {}",
        delta.queries_total,
    );

    // --- Scenario 4: JDBC Describe/Flush pattern ---
    // Flush sends Parse/Bind/Describe to origin immediately.
    // Execute/Sync follows separately.
    let m = ctx.metrics().await?;
    let output = pgproto_run(
        ctx.cache_port,
        "tests/data/pgproto/jdbc_describe_flush.data",
    );
    assert_pgproto_select(&output, 3);
    // Flush-then-Sync pattern should still process the query
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert!(
        delta.queries_total >= 1,
        "expected at least 1 query from JDBC pattern, got {}",
        delta.queries_total,
    );

    // --- Scenario 5: Multiple Executes before Sync (must forward) ---
    // Two Parse/Bind/Execute pairs before a single Sync.
    // multiple_executes flag forces forward to origin (uncacheable).
    let m = ctx.metrics().await?;
    let output = pgproto_run(ctx.cache_port, "tests/data/pgproto/multi_execute.data");
    assert_pgproto_select(&output, 3); // at least the first SELECT returns 3 rows
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert!(
        delta.queries_uncacheable >= 1,
        "expected uncacheable increment for multi-execute, got {}",
        delta.queries_uncacheable,
    );

    // --- Scenario 6: Non-cacheable INSERT forwarded at Sync ---
    let m = ctx.metrics().await?;
    let output = pgproto_run(ctx.cache_port, "tests/data/pgproto/non_cacheable_sync.data");
    assert!(
        output.contains("ReadyForQuery"),
        "expected ReadyForQuery for INSERT:\n{output}",
    );
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert!(
        delta.queries_unsupported >= 1,
        "expected unsupported increment for INSERT, got {}",
        delta.queries_unsupported,
    );

    // Verify the INSERT actually executed on origin
    let rows = ctx
        .query("SELECT name FROM proto_test WHERE id = 99", &[])
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>("name"), "pgproto");

    Ok(())
}
