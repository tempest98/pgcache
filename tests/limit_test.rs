#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

use std::io::Error;

use crate::util::{
    TestContext, assert_cache_hit, assert_cache_miss, assert_row_at, wait_cache_load, wait_for_cdc,
};

mod util;

/// Shared table setup used across all limit/offset sub-tests.
/// Creates an `items` table with 10 rows ordered by id (1..=10).
async fn setup_items_table(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, category TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO items (id, name, category) VALUES \
         (1, 'a', 'x'), (2, 'b', 'x'), (3, 'c', 'x'), (4, 'd', 'y'), (5, 'e', 'y'), \
         (6, 'f', 'y'), (7, 'g', 'z'), (8, 'h', 'z'), (9, 'i', 'z'), (10, 'j', 'z')",
        &[],
    )
    .await?;

    Ok(())
}

/// Test that a LIMIT query is cached and served correctly.
///
/// Flow:
///   1. SELECT ... LIMIT 3 → cache miss, forwarded to origin
///   2. Wait for cache population
///   3. Same query → cache hit, served from cache with correct 3 rows
#[tokio::test]
async fn test_limit_basic_cache() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_items_table(&mut ctx).await?;

    let query = "SELECT id, name FROM items WHERE category = 'x' ORDER BY id LIMIT 3";

    // First query — cache miss
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    // RowDescription + 3 DataRows + CommandComplete = 5 messages
    assert_eq!(res.len(), 5);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "a")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("name", "b")])?;
    assert_row_at(&res, 3, &[("id", "3"), ("name", "c")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // Second query — cache hit, same data
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "a")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("name", "b")])?;
    assert_row_at(&res, 3, &[("id", "3"), ("name", "c")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test that LIMIT and OFFSET queries share a cache entry.
///
/// Flow:
///   1. SELECT ... ORDER BY id LIMIT 5 → cache miss, populates with 5 rows
///   2. SELECT ... ORDER BY id LIMIT 3 → cache hit (3 < 5, sufficient)
///   3. SELECT ... ORDER BY id LIMIT 2 OFFSET 1 → cache hit (2+1=3 ≤ 5)
#[tokio::test]
async fn test_limit_shared_fingerprint() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_items_table(&mut ctx).await?;

    let base = "SELECT id, name FROM items WHERE category = 'z' ORDER BY id";

    // LIMIT 4 → cache miss, populates up to 4 rows
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(&format!("{base} LIMIT 4")).await?;
    assert_eq!(res.len(), 6); // RowDescription + 4 rows + CommandComplete
    assert_row_at(&res, 1, &[("id", "7")])?;
    assert_row_at(&res, 4, &[("id", "10")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // LIMIT 2 → cache hit (2 ≤ 4)
    let res = ctx.simple_query(&format!("{base} LIMIT 2")).await?;
    assert_eq!(res.len(), 4); // RowDescription + 2 rows + CommandComplete
    assert_row_at(&res, 1, &[("id", "7"), ("name", "g")])?;
    assert_row_at(&res, 2, &[("id", "8"), ("name", "h")])?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // LIMIT 2 OFFSET 1 → cache hit (2+1=3 ≤ 4)
    let res = ctx
        .simple_query(&format!("{base} LIMIT 2 OFFSET 1"))
        .await?;
    assert_eq!(res.len(), 4); // RowDescription + 2 rows + CommandComplete
    assert_row_at(&res, 1, &[("id", "8"), ("name", "h")])?;
    assert_row_at(&res, 2, &[("id", "9"), ("name", "i")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test limit bump: when a query requests more rows than currently cached,
/// it should forward to origin and re-populate with the higher limit.
///
/// Flow:
///   1. SELECT ... LIMIT 2 → cache miss, populates with 2 rows
///   2. SELECT ... LIMIT 2 → cache hit
///   3. SELECT ... LIMIT 4 → cache miss (4 > 2, triggers limit bump)
///   4. Wait for re-population
///   5. SELECT ... LIMIT 4 → cache hit with 4 rows
///   6. SELECT ... LIMIT 2 → cache hit (2 ≤ 4, still works)
#[tokio::test]
async fn test_limit_bump() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_items_table(&mut ctx).await?;

    let base = "SELECT id, name FROM items WHERE category = 'y' ORDER BY id";

    // LIMIT 2 → cache miss, populates with 2 rows
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(&format!("{base} LIMIT 2")).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "4"), ("name", "d")])?;
    assert_row_at(&res, 2, &[("id", "5"), ("name", "e")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // LIMIT 2 → cache hit
    let res = ctx.simple_query(&format!("{base} LIMIT 2")).await?;
    assert_eq!(res.len(), 4);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // LIMIT 3 → cache miss (3 > 2, limit bump)
    let res = ctx.simple_query(&format!("{base} LIMIT 3")).await?;
    assert_eq!(res.len(), 5);
    assert_row_at(&res, 1, &[("id", "4")])?;
    assert_row_at(&res, 2, &[("id", "5")])?;
    assert_row_at(&res, 3, &[("id", "6")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    dbg!("hihihi");

    // LIMIT 3 → cache hit after re-population
    let res = ctx.simple_query(&format!("{base} LIMIT 3")).await?;

    dbg!("hihihi");
    assert_eq!(res.len(), 5);
    assert_row_at(&res, 1, &[("id", "4"), ("name", "d")])?;
    assert_row_at(&res, 2, &[("id", "5"), ("name", "e")])?;
    assert_row_at(&res, 3, &[("id", "6"), ("name", "f")])?;
    let m = assert_cache_hit(&mut ctx, m).await?;
    dbg!("hihihi");

    // LIMIT 2 → still a cache hit (2 ≤ 3)
    let res = ctx.simple_query(&format!("{base} LIMIT 2")).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "4"), ("name", "d")])?;
    assert_row_at(&res, 2, &[("id", "5"), ("name", "e")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    dbg!("hihihi");

    Ok(())
}

/// Test that the same base query without LIMIT also works with an
/// existing limited cache entry — it should trigger a limit bump to
/// unlimited and then serve all rows.
///
/// Flow:
///   1. SELECT ... LIMIT 2 → cache miss, populates with 2 rows
///   2. SELECT ... (no LIMIT) → cache miss (needs unlimited, triggers bump)
///   3. Wait for re-population
///   4. SELECT ... (no LIMIT) → cache hit with all rows
///   5. SELECT ... LIMIT 2 → still cache hit
#[tokio::test]
async fn test_limit_bump_to_unlimited() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_items_table(&mut ctx).await?;

    let base = "SELECT id, name FROM items WHERE category = 'x' ORDER BY id";

    // LIMIT 2 → cache miss
    let m = ctx.metrics().await?;
    ctx.simple_query(&format!("{base} LIMIT 2")).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // No LIMIT → cache miss (needs all rows, only 2 cached)
    let res = ctx.simple_query(base).await?;
    // category 'x' has 3 rows (ids 1,2,3)
    assert_eq!(res.len(), 5); // RowDescription + 3 rows + CommandComplete
    assert_row_at(&res, 1, &[("id", "1")])?;
    assert_row_at(&res, 2, &[("id", "2")])?;
    assert_row_at(&res, 3, &[("id", "3")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // No LIMIT → cache hit (now unlimited)
    let res = ctx.simple_query(base).await?;
    assert_eq!(res.len(), 5);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // LIMIT 2 → cache hit (2 ≤ unlimited)
    let res = ctx.simple_query(&format!("{base} LIMIT 2")).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "a")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("name", "b")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test CDC invalidation: DELETE on a limited query's table should invalidate
/// the cache because the cached result may have fewer rows than the LIMIT window.
///
/// Flow:
///   1. SELECT ... LIMIT 3 → cache miss, populates
///   2. Cache hit
///   3. DELETE a row from origin (via CDC)
///   4. Same query → cache miss (invalidated by DELETE)
#[tokio::test]
async fn test_limit_cdc_delete_invalidates() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_items_table(&mut ctx).await?;

    let query = "SELECT id, name FROM items WHERE category = 'z' ORDER BY id LIMIT 3";

    // First query — cache miss
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5);
    assert_row_at(&res, 1, &[("id", "7")])?;
    assert_row_at(&res, 2, &[("id", "8")])?;
    assert_row_at(&res, 3, &[("id", "9")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // Cache hit
    ctx.simple_query(query).await?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Delete a row from origin (bypassing proxy, directly on origin)
    ctx.origin_query("DELETE FROM items WHERE id = 7", &[])
        .await?;

    wait_for_cdc().await;

    // Should be cache miss — DELETE on a limited query invalidates
    let res = ctx.simple_query(query).await?;
    // After delete: remaining z-category rows are 8,9,10 → LIMIT 3 returns all 3
    assert_eq!(res.len(), 5);
    assert_row_at(&res, 1, &[("id", "8")])?;
    assert_row_at(&res, 2, &[("id", "9")])?;
    assert_row_at(&res, 3, &[("id", "10")])?;
    let _m = assert_cache_miss(&mut ctx, m).await?;

    Ok(())
}

/// Test CDC behavior: INSERT on a limited query's table should NOT invalidate.
/// The extra row is added to the cache table; LIMIT is applied at serve time,
/// so the result is still correct.
///
/// Flow:
///   1. SELECT ... LIMIT 2 → cache miss, populates
///   2. Cache hit
///   3. INSERT a matching row on origin (via CDC)
///   4. Same query → cache hit (not invalidated by INSERT)
#[tokio::test]
async fn test_limit_cdc_insert_no_invalidation() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_items_table(&mut ctx).await?;

    let query = "SELECT id, name FROM items WHERE category = 'x' ORDER BY id LIMIT 2";

    // First query — cache miss
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "a")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("name", "b")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // Cache hit
    ctx.simple_query(query).await?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Insert a new matching row on origin
    ctx.origin_query(
        "INSERT INTO items (id, name, category) VALUES (11, 'k', 'x')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Should still be cache hit — INSERT doesn't invalidate limited queries
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    // LIMIT 2 ORDER BY id → still returns first 2 rows
    assert_row_at(&res, 1, &[("id", "1"), ("name", "a")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("name", "b")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test LIMIT with OFFSET serving from cache.
///
/// Flow:
///   1. SELECT ... LIMIT 4 → cache miss, populates 4 rows
///   2. SELECT ... LIMIT 2 OFFSET 1 → cache hit with correct window
#[tokio::test]
async fn test_limit_offset_cache_hit() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_items_table(&mut ctx).await?;

    let base = "SELECT id, name FROM items WHERE category = 'z' ORDER BY id";

    // LIMIT 4 → cache miss, populates 4 rows
    let m = ctx.metrics().await?;
    ctx.simple_query(&format!("{base} LIMIT 4")).await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // LIMIT 2 OFFSET 1 → cache hit (2+1=3 ≤ 4)
    let res = ctx
        .simple_query(&format!("{base} LIMIT 2 OFFSET 1"))
        .await?;
    assert_eq!(res.len(), 4); // RowDescription + 2 rows + CommandComplete
    assert_row_at(&res, 1, &[("id", "8"), ("name", "h")])?;
    assert_row_at(&res, 2, &[("id", "9"), ("name", "i")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test LIMIT with OFFSET serving from cache.
///
/// Flow:
///   1. SELECT ... LIMIT 2 offset 2 → cache miss, populates 4 rows
///   2. SELECT ... LIMIT 4 → cache hit with correct window
#[tokio::test]
async fn test_limit_offset_limit_cache_hit() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_items_table(&mut ctx).await?;

    let base = "SELECT id, name FROM items WHERE category = 'z' ORDER BY id";

    // LIMIT 4 → cache miss, populates 4 rows
    let m = ctx.metrics().await?;
    ctx.simple_query(&format!("{base} LIMIT 2 OFFSET 2"))
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    let res = ctx.simple_query(&format!("{base} LIMIT 4")).await?;
    assert_eq!(res.len(), 6); // RowDescription + 4 rows + CommandComplete
    assert_row_at(&res, 1, &[("id", "7"), ("name", "g")])?;
    assert_row_at(&res, 2, &[("id", "8"), ("name", "h")])?;
    assert_row_at(&res, 3, &[("id", "9"), ("name", "i")])?;
    assert_row_at(&res, 4, &[("id", "10"), ("name", "j")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test LIMIT with extended protocol (parameterized queries).
///
/// Flow:
///   1. Prepared statement with LIMIT $2 → cache miss
///   2. Same params → cache hit
///   3. Different LIMIT value within cached range → cache hit
#[tokio::test]
async fn test_limit_extended_protocol() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_items_table(&mut ctx).await?;

    let stmt = ctx
        .prepare("SELECT id, name FROM items WHERE category = $1 ORDER BY id LIMIT $2")
        .await?;

    // LIMIT 4 → cache miss
    let m = ctx.metrics().await?;
    let rows = ctx.query(&stmt, &[&"z", &4i64]).await?;
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].get::<_, i32>("id"), 7);
    assert_eq!(rows[3].get::<_, i32>("id"), 10);
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // LIMIT 4 → cache hit
    let rows = ctx.query(&stmt, &[&"z", &4i64]).await?;
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0].get::<_, i32>("id"), 7);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // LIMIT 2 → cache hit (2 ≤ 4)
    let rows = ctx.query(&stmt, &[&"z", &2i64]).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>("id"), 7);
    assert_eq!(rows[1].get::<_, i32>("id"), 8);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test that LIMIT on set operations (UNION) is cacheable.
/// All rows are populated per-branch, LIMIT is applied at serve time.
#[tokio::test]
async fn test_limit_union_cacheable() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_items_table(&mut ctx).await?;

    let query = "SELECT id FROM items WHERE category = 'x' \
                 UNION SELECT id FROM items WHERE category = 'y' \
                 LIMIT 3";

    // First query — cache miss, triggers population
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    // UNION produces 6 unique ids (1-6), LIMIT 3 → 3 rows
    assert_eq!(res.len(), 5); // RowDescription + 3 rows + CommandComplete
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // Second query — cache hit, LIMIT applied at serve time
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Test OFFSET-only queries (no LIMIT count).
/// OFFSET without LIMIT means "all rows starting from offset",
/// which requires all rows to be cached (unlimited).
///
/// Flow:
///   1. SELECT ... OFFSET 1 → cache miss, populates all rows (unlimited)
///   2. Same query → cache hit
#[tokio::test]
async fn test_offset_only() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_items_table(&mut ctx).await?;

    let query = "SELECT id, name FROM items WHERE category = 'x' ORDER BY id OFFSET 1";

    // OFFSET only → cache miss (needs unlimited)
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    // category 'x' has 3 rows, OFFSET 1 → 2 rows
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "2"), ("name", "b")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("name", "c")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // Same query → cache hit (unlimited rows cached)
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "2"), ("name", "b")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("name", "c")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}
