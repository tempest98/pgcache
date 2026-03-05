#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

use std::io::Error;

use crate::util::{
    TestContext, assert_cache_miss, assert_not_subsumed, assert_row_at, assert_subsume_hit,
    wait_cache_load,
};

mod util;

/// Test that an unconstrained cached query subsumes a constrained query on the same table.
/// SELECT * FROM t (no WHERE) covers SELECT * FROM t WHERE id = 1.
#[tokio::test]
async fn test_subsumption_unconstrained_covers_constrained() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_uncon (id integer PRIMARY KEY, data text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_uncon VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')",
        &[],
    )
    .await?;

    // Cache the unconstrained query
    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query("SELECT id, data FROM test_sub_uncon")
        .await?;
    assert_eq!(res.len(), 5); // 3 rows + RowDescription + CommandComplete
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // Constrained query should be subsumed — served from cache immediately
    let res = ctx
        .simple_query("SELECT id, data FROM test_sub_uncon WHERE id = 1")
        .await?;
    assert_eq!(res.len(), 3); // 1 row
    assert_row_at(&res, 1, &[("id", "1"), ("data", "alice")])?;
    let _m = assert_subsume_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test equality superset subsumption: cached WHERE tenant_id = 1 covers
/// WHERE tenant_id = 1 AND status = 'active'.
#[tokio::test]
async fn test_subsumption_equality_superset() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_eq (id integer PRIMARY KEY, tenant_id integer, status text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_eq VALUES \
         (1, 1, 'active'), (2, 1, 'inactive'), (3, 2, 'active')",
        &[],
    )
    .await?;

    // Cache with single equality constraint
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_sub_eq WHERE tenant_id = 1")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // Narrower query (superset of constraints) should be subsumed
    let res = ctx
        .simple_query("SELECT * FROM test_sub_eq WHERE tenant_id = 1 AND status = 'active'")
        .await?;
    assert_eq!(res.len(), 3); // 1 row
    assert_row_at(
        &res,
        1,
        &[("id", "1"), ("tenant_id", "1"), ("status", "active")],
    )?;
    let _m = assert_subsume_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test that different equality values are NOT subsumed.
/// Cached WHERE tenant_id = 1 does NOT cover WHERE tenant_id = 2.
#[tokio::test]
async fn test_subsumption_different_values_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_diff (id integer PRIMARY KEY, tenant_id integer)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO test_sub_diff VALUES (1, 1), (2, 2)", &[])
        .await?;

    // Cache tenant_id = 1
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_sub_diff WHERE tenant_id = 1")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // Query tenant_id = 2 — different value, should NOT be subsumed
    ctx.simple_query("SELECT * FROM test_sub_diff WHERE tenant_id = 2")
        .await?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// Test that queries on different tables are NOT subsumed.
#[tokio::test]
async fn test_subsumption_different_table_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_t1 (id integer PRIMARY KEY, data text)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE test_sub_t2 (id integer PRIMARY KEY, data text)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO test_sub_t1 VALUES (1, 'a')", &[])
        .await?;
    ctx.query("INSERT INTO test_sub_t2 VALUES (1, 'b')", &[])
        .await?;

    // Cache t1
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_sub_t1").await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // Query t2 — different table, not subsumed
    ctx.simple_query("SELECT * FROM test_sub_t2").await?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// Test that cached queries with inequality constraints do NOT subsume.
/// Cached WHERE value > 50 does NOT cover WHERE value > 100.
#[tokio::test]
async fn test_subsumption_inequality_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_ineq (id integer PRIMARY KEY, value integer)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_ineq VALUES (1, 25), (2, 75), (3, 150)",
        &[],
    )
    .await?;

    // Cache with inequality
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_sub_ineq WHERE value > 50")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // Narrower inequality — not subsumed (v1 rejects non-equality cached constraints)
    ctx.simple_query("SELECT * FROM test_sub_ineq WHERE value > 100")
        .await?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// Test that a cached JOIN query does NOT subsume, even with same tables and
/// additional WHERE constraints. Multi-table cached queries are excluded from
/// subsumption candidates because join conditions act as implicit filters.
#[tokio::test]
async fn test_subsumption_join_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_orders (id integer PRIMARY KEY, customer_id integer, status text)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE test_sub_customers (id integer PRIMARY KEY, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_orders VALUES (1, 1, 'pending'), (2, 2, 'active')",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_customers VALUES (1, 'alice'), (2, 'bob')",
        &[],
    )
    .await?;

    // Cache the JOIN query
    let m = ctx.metrics().await?;
    ctx.simple_query(
        "SELECT o.id, c.name FROM test_sub_orders o \
         JOIN test_sub_customers c ON o.customer_id = c.id",
    )
    .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // Same JOIN with additional WHERE — not subsumed (multi-table cached query)
    ctx.simple_query(
        "SELECT o.id, c.name FROM test_sub_orders o \
         JOIN test_sub_customers c ON o.customer_id = c.id \
         WHERE o.status = 'pending'",
    )
    .await?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// Test that a single-table cached query does NOT subsume a JOIN query
/// that references an additional uncovered table.
#[tokio::test]
async fn test_subsumption_join_partial_coverage_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_orders2 (id integer PRIMARY KEY, customer_id integer, tenant_id integer)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE test_sub_customers2 (id integer PRIMARY KEY, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_orders2 VALUES (1, 1, 1), (2, 2, 1)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_customers2 VALUES (1, 'alice'), (2, 'bob')",
        &[],
    )
    .await?;

    // Cache single-table query on orders
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_sub_orders2 WHERE tenant_id = 1")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // JOIN query — orders is covered but customers is not
    ctx.simple_query(
        "SELECT o.id, c.name FROM test_sub_orders2 o \
         JOIN test_sub_customers2 c ON o.customer_id = c.id \
         WHERE o.tenant_id = 1",
    )
    .await?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// Test that a cached query with LIMIT does NOT subsume.
/// LIMIT restricts the number of rows cached, so coverage can't be guaranteed.
#[tokio::test]
async fn test_subsumption_with_limit_not_subsumed() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_limit (id integer PRIMARY KEY, data text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_limit VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        &[],
    )
    .await?;

    // Cache with LIMIT
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_sub_limit LIMIT 5")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // Query without LIMIT — not subsumed (cached has LIMIT)
    ctx.simple_query("SELECT * FROM test_sub_limit WHERE id = 1")
        .await?;
    let _m = assert_not_subsumed(&mut ctx, m).await?;

    Ok(())
}

/// Test that two separate single-table cached queries can subsume a JOIN query
/// that references both tables. Each table is independently covered by its own
/// cached query, so the JOIN query's data is fully available in the cache DB.
#[tokio::test]
async fn test_subsumption_two_single_tables_cover_join() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE test_sub_items (id integer PRIMARY KEY, category_id integer, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE test_sub_categories (id integer PRIMARY KEY, label text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_items VALUES (1, 10, 'widget'), (2, 20, 'gadget'), (3, 10, 'thing')",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO test_sub_categories VALUES (10, 'tools'), (20, 'electronics')",
        &[],
    )
    .await?;

    // Cache both tables independently
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM test_sub_items").await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    ctx.simple_query("SELECT * FROM test_sub_categories")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // JOIN query referencing both tables — each table covered by its own cached query
    let res = ctx
        .simple_query(
            "SELECT i.name, c.label FROM test_sub_items i \
             JOIN test_sub_categories c ON i.category_id = c.id \
             ORDER BY i.name",
        )
        .await?;
    assert_eq!(res.len(), 5); // 3 rows (widget+tools, gadget+electronics, thing+tools)
    assert_row_at(&res, 1, &[("name", "gadget"), ("label", "electronics")])?;
    assert_row_at(&res, 2, &[("name", "thing"), ("label", "tools")])?;
    assert_row_at(&res, 3, &[("name", "widget"), ("label", "tools")])?;
    let _m = assert_subsume_hit(&mut ctx, m).await?;

    Ok(())
}
