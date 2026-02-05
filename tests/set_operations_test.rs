#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

use std::io::Error;

use crate::util::{TestContext, assert_row_at, metrics_delta, wait_cache_load, wait_for_cdc};

mod util;

/// Consolidated test for set operation (UNION/INTERSECT/EXCEPT) caching.
#[tokio::test]
async fn test_set_operations() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    set_op_union(&mut ctx).await?;
    set_op_union_cdc(&mut ctx).await?;
    set_op_intersect(&mut ctx).await?;
    set_op_except(&mut ctx).await?;
    set_op_union_all(&mut ctx).await?;

    Ok(())
}

/// Test basic UNION query caching with metrics verification
async fn set_op_union(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    // Create two tables with similar structure
    ctx.query(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, tenant_id INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE admins (id INTEGER PRIMARY KEY, name TEXT, tenant_id INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO users (id, name, tenant_id) VALUES (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Charlie', 2)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO admins (id, name, tenant_id) VALUES (10, 'Admin1', 1), (11, 'Admin2', 2)",
        &[],
    )
    .await?;

    // UNION query - combines users and admins for tenant 1
    let query = "SELECT id, name FROM users WHERE tenant_id = 1 \
                 UNION \
                 SELECT id, name FROM admins WHERE tenant_id = 1 \
                 ORDER BY id";

    // First query - cache miss, populates cache
    let res = ctx.simple_query(query).await?;

    // Should have 3 results: Alice (1), Bob (2), Admin1 (10)
    assert_eq!(res.len(), 5); // 3 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("id", "1"), ("name", "Alice")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("name", "Bob")])?;
    assert_row_at(&res, 3, &[("id", "10"), ("name", "Admin1")])?;

    wait_cache_load().await;

    // Second query - should hit cache
    let res = ctx.simple_query(query).await?;

    assert_eq!(res.len(), 5);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "Alice")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("name", "Bob")])?;
    assert_row_at(&res, 3, &[("id", "10"), ("name", "Admin1")])?;

    // Verify metrics
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    // 4 setup queries (2 CREATE + 2 INSERT) + 2 UNION queries = 6 total
    assert_eq!(delta.queries_total, 6, "total queries");
    // Both UNION queries should be cacheable
    assert_eq!(delta.queries_cacheable, 2, "cacheable queries");
    // Setup queries via extended protocol are both uncacheable and unsupported
    assert_eq!(delta.queries_uncacheable, 4, "uncacheable queries");
    assert_eq!(delta.queries_unsupported, 4, "unsupported queries");
    // First UNION is cache miss, second is cache hit
    assert_eq!(delta.queries_cache_miss, 1, "cache misses");
    assert_eq!(delta.queries_cache_hit, 1, "cache hits");

    Ok(())
}

/// Test UNION query with CDC updates
async fn set_op_union_cdc(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    let query = "SELECT id, name FROM users WHERE tenant_id = 1 \
                 UNION \
                 SELECT id, name FROM admins WHERE tenant_id = 1 \
                 ORDER BY id";

    // Insert a new user directly on origin (simulating external change)
    ctx.origin_query(
        "INSERT INTO users (id, name, tenant_id) VALUES (4, 'Dave', 1)",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Query should now include Dave (cache was invalidated by CDC)
    let res = ctx.simple_query(query).await?;

    assert_eq!(res.len(), 6); // 4 rows now
    assert_row_at(&res, 1, &[("id", "1"), ("name", "Alice")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("name", "Bob")])?;
    assert_row_at(&res, 3, &[("id", "4"), ("name", "Dave")])?;
    assert_row_at(&res, 4, &[("id", "10"), ("name", "Admin1")])?;

    // Insert a new admin on origin
    ctx.origin_query(
        "INSERT INTO admins (id, name, tenant_id) VALUES (12, 'Admin3', 1)",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Query should now include Admin3
    let res = ctx.simple_query(query).await?;

    assert_eq!(res.len(), 7); // 5 rows now
    assert_row_at(&res, 5, &[("id", "12"), ("name", "Admin3")])?;

    // Delete a user
    ctx.origin_query("DELETE FROM users WHERE id = 4", &[])
        .await?;

    wait_for_cdc().await;

    // Dave should be gone
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 6); // back to 4 rows

    // Verify metrics - CDC updates the cache in place, so queries still hit cache
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    // 3 UNION queries after CDC changes
    assert_eq!(delta.queries_cacheable, 3, "cacheable queries");
    // CDC updates cache in place, so queries are cache hits (not misses)
    assert_eq!(delta.queries_cache_hit, 3, "cache hits after CDC updates");

    Ok(())
}

/// Test INTERSECT query caching
async fn set_op_intersect(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    // Create tables for intersection test
    ctx.query(
        "CREATE TABLE products_a (id INTEGER PRIMARY KEY, sku TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE products_b (id INTEGER PRIMARY KEY, sku TEXT)",
        &[],
    )
    .await?;

    // Insert products - some overlap
    ctx.query(
        "INSERT INTO products_a (id, sku) VALUES (1, 'SKU-001'), (2, 'SKU-002'), (3, 'SKU-003')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO products_b (id, sku) VALUES (10, 'SKU-002'), (11, 'SKU-003'), (12, 'SKU-004')",
        &[],
    )
    .await?;

    // INTERSECT - only SKUs present in both tables
    let query = "SELECT sku FROM products_a \
                 INTERSECT \
                 SELECT sku FROM products_b \
                 ORDER BY sku";

    let res = ctx.simple_query(query).await?;

    // Should have SKU-002 and SKU-003 (the overlap)
    assert_eq!(res.len(), 4); // 2 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("sku", "SKU-002")])?;
    assert_row_at(&res, 2, &[("sku", "SKU-003")])?;

    wait_cache_load().await;

    // Cache hit
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);

    // Add a new overlapping product via CDC
    ctx.origin_query(
        "INSERT INTO products_a (id, sku) VALUES (4, 'SKU-004')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Now SKU-004 should also be in the intersection
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5); // 3 rows now
    assert_row_at(&res, 3, &[("sku", "SKU-004")])?;

    // Verify metrics
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    // 4 setup + 3 INTERSECT queries = 7 total
    assert_eq!(delta.queries_total, 7, "total queries");
    // INTERSECT queries are cacheable
    assert_eq!(delta.queries_cacheable, 3, "cacheable INTERSECT queries");
    // Setup queries are both uncacheable and unsupported
    assert_eq!(delta.queries_uncacheable, 4, "uncacheable queries");
    assert_eq!(delta.queries_unsupported, 4, "unsupported queries");
    // First query: miss, second and third (after CDC): hits (CDC updates cache in place)
    assert_eq!(delta.queries_cache_miss, 1, "cache misses");
    assert_eq!(delta.queries_cache_hit, 2, "cache hits");

    Ok(())
}

/// Test EXCEPT query caching
async fn set_op_except(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    // Create tables for except test
    ctx.query(
        "CREATE TABLE all_items (id INTEGER PRIMARY KEY, item TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE excluded_items (id INTEGER PRIMARY KEY, item TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO all_items (id, item) VALUES (1, 'Apple'), (2, 'Banana'), (3, 'Cherry'), (4, 'Date')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO excluded_items (id, item) VALUES (10, 'Banana'), (11, 'Date')",
        &[],
    )
    .await?;

    // EXCEPT - items in all_items but not in excluded_items
    let query = "SELECT item FROM all_items \
                 EXCEPT \
                 SELECT item FROM excluded_items \
                 ORDER BY item";

    let res = ctx.simple_query(query).await?;

    // Should have Apple and Cherry (not Banana or Date)
    assert_eq!(res.len(), 4); // 2 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("item", "Apple")])?;
    assert_row_at(&res, 2, &[("item", "Cherry")])?;

    wait_cache_load().await;

    // Cache hit
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);

    // Add Cherry to excluded via CDC - should disappear from result
    ctx.origin_query(
        "INSERT INTO excluded_items (id, item) VALUES (12, 'Cherry')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Now only Apple should remain
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3); // 1 row
    assert_row_at(&res, 1, &[("item", "Apple")])?;

    // Verify metrics
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    // 4 setup + 3 EXCEPT queries = 7 total
    assert_eq!(delta.queries_total, 7, "total queries");
    // EXCEPT queries are cacheable
    assert_eq!(delta.queries_cacheable, 3, "cacheable EXCEPT queries");
    // Setup queries are both uncacheable and unsupported
    assert_eq!(delta.queries_uncacheable, 4, "uncacheable queries");
    assert_eq!(delta.queries_unsupported, 4, "unsupported queries");
    // First: miss, second and third (after CDC): hits (CDC updates cache in place)
    assert_eq!(delta.queries_cache_miss, 1, "cache misses");
    assert_eq!(delta.queries_cache_hit, 2, "cache hits");

    Ok(())
}

/// Test UNION ALL (no deduplication) vs UNION (with deduplication)
async fn set_op_union_all(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    // Create tables with duplicate values across them
    ctx.query(
        "CREATE TABLE list_a (id INTEGER PRIMARY KEY, value TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE list_b (id INTEGER PRIMARY KEY, value TEXT)",
        &[],
    )
    .await?;

    // Insert values with duplicates
    ctx.query(
        "INSERT INTO list_a (id, value) VALUES (1, 'X'), (2, 'Y')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO list_b (id, value) VALUES (10, 'Y'), (11, 'Z')",
        &[],
    )
    .await?;

    // UNION (deduplicated) - should have X, Y, Z
    let query_union = "SELECT value FROM list_a \
                       UNION \
                       SELECT value FROM list_b \
                       ORDER BY value";

    let res = ctx.simple_query(query_union).await?;
    assert_eq!(res.len(), 5); // 3 rows (deduplicated)
    assert_row_at(&res, 1, &[("value", "X")])?;
    assert_row_at(&res, 2, &[("value", "Y")])?;
    assert_row_at(&res, 3, &[("value", "Z")])?;

    wait_cache_load().await;

    // UNION ALL (not deduplicated) - should have X, Y, Y, Z
    let query_union_all = "SELECT value FROM list_a \
                           UNION ALL \
                           SELECT value FROM list_b \
                           ORDER BY value";

    let res = ctx.simple_query(query_union_all).await?;
    assert_eq!(res.len(), 6); // 4 rows (not deduplicated)
    assert_row_at(&res, 1, &[("value", "X")])?;
    assert_row_at(&res, 2, &[("value", "Y")])?;
    assert_row_at(&res, 3, &[("value", "Y")])?; // duplicate
    assert_row_at(&res, 4, &[("value", "Z")])?;

    wait_cache_load().await;

    // Verify both queries hit cache on subsequent runs
    let res = ctx.simple_query(query_union).await?;
    assert_eq!(res.len(), 5);

    let res = ctx.simple_query(query_union_all).await?;
    assert_eq!(res.len(), 6);

    // Verify metrics
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    // 4 setup + 4 UNION/UNION ALL queries = 8 total
    assert_eq!(delta.queries_total, 8, "total queries");
    // All UNION/UNION ALL queries are cacheable
    assert_eq!(delta.queries_cacheable, 4, "cacheable queries");
    // Setup queries are both uncacheable and unsupported
    assert_eq!(delta.queries_uncacheable, 4, "uncacheable queries");
    assert_eq!(delta.queries_unsupported, 4, "unsupported queries");
    // First UNION: miss, UNION ALL: miss, second UNION: hit, second UNION ALL: hit
    assert_eq!(delta.queries_cache_miss, 2, "cache misses");
    assert_eq!(delta.queries_cache_hit, 2, "cache hits");

    Ok(())
}
