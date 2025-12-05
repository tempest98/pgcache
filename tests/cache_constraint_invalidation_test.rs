#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

use std::io::Error;

use crate::util::{TestContext, assert_row_at, setup_constraint_test_tables, wait_for_cdc};

mod util;

// TODO: Add mechanism to verify if cache was invalidated or not
// Currently tests verify correctness (results are accurate) but not optimization
// (whether invalidation occurred). Would be useful to expose invalidation metrics
// or events for testing the optimization behavior directly.

/// Test that INSERT with matching constraints properly caches the row
#[tokio::test]
async fn test_insert_matching_constraint() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    setup_constraint_test_tables(&mut ctx).await?;

    // Prime the cache with a query that has constraint test_map.test_id = 1
    // Base data has: (1, 1, 'alpha'), (2, 1, 'beta')
    let _ = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // Insert row that matches constraint (test_id = 1)
    ctx.origin_query(
        "insert into test_map (test_id, data) values (1, 'delta')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Query should show all 3 rows with test_id = 1
    let res = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // RowDescription + 3 data rows + CommandComplete = 5
    assert_eq!(res.len(), 5);
    assert_row_at(&res, 1, &[("id", "1"), ("test_id", "1"), ("data", "alpha")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("test_id", "1"), ("data", "beta")])?;
    assert_row_at(&res, 3, &[("id", "4"), ("test_id", "1"), ("data", "delta")])?;

    Ok(())
}

/// Test that INSERT with non-matching constraints is optimized (no invalidation)
#[tokio::test]
async fn test_insert_non_matching_constraint() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    setup_constraint_test_tables(&mut ctx).await?;

    // Prime the cache with a query that has constraint test_map.test_id = 1
    let _ = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // Insert row that does NOT match constraint (test_id = 5, not 1)
    // This should be optimized - no invalidation because row won't appear in results
    ctx.origin_query(
        "insert into test_map (test_id, data) values (5, 'no_match')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Query should still return only the original 2 rows (cache not invalidated, new row doesn't match)
    let res = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // RowDescription + 2 data rows + CommandComplete = 4
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1"), ("test_id", "1"), ("data", "alpha")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("test_id", "1"), ("data", "beta")])?;

    Ok(())
}

/// Test UPDATE where JOIN column changes from non-matching to matching value
/// This should invalidate because row is entering the result set
#[tokio::test]
async fn test_update_entering_result_set() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    setup_constraint_test_tables(&mut ctx).await?;

    // Prime the cache - query has constraint test_map.test_id = 1
    // Base data: gamma has test_id = 2, so only alpha and beta match
    let _ = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // UPDATE: Change gamma's test_id from 2 to 1 (entering result set)
    ctx.origin_query("update test_map set test_id = 1 where data = 'gamma'", &[])
        .await?;

    wait_for_cdc().await;

    // Query should now show 3 rows including gamma
    let res = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // RowDescription + 3 data rows + CommandComplete = 5
    assert_eq!(res.len(), 5);
    assert_row_at(&res, 1, &[("id", "1"), ("test_id", "1"), ("data", "alpha")])?;
    assert_row_at(&res, 2, &[("id", "2"), ("test_id", "1"), ("data", "beta")])?;
    assert_row_at(&res, 3, &[("id", "3"), ("test_id", "1"), ("data", "gamma")])?;

    Ok(())
}

/// Test UPDATE where JOIN column changes from matching to non-matching value
/// This should NOT invalidate because row is leaving the result set (UPDATE handles removal)
#[tokio::test]
async fn test_update_leaving_result_set() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    setup_constraint_test_tables(&mut ctx).await?;

    // Prime the cache - query has constraint test_map.test_id = 1
    let _ = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // UPDATE: Change alpha's test_id from 1 to 5 (leaving result set)
    // Optimization: no invalidation, UPDATE mechanism removes row from cache
    ctx.origin_query("update test_map set test_id = 5 where data = 'alpha'", &[])
        .await?;

    wait_for_cdc().await;

    // Query should now return 1 row (only beta)
    let res = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // RowDescription + 1 data row + CommandComplete = 3
    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "2"), ("test_id", "1"), ("data", "beta")])?;

    Ok(())
}

/// Test UPDATE where non-JOIN column changes (data field)
/// This should NOT invalidate because JOIN key is unchanged
#[tokio::test]
async fn test_update_non_join_column() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    setup_constraint_test_tables(&mut ctx).await?;

    // Prime the cache
    let _ = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // UPDATE: Change data field only (not JOIN column)
    // This should NOT invalidate
    ctx.origin_query(
        "update test_map set data = 'alpha_updated' where data = 'alpha'",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Query should show updated data
    let res = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // RowDescription + 2 data rows + CommandComplete = 4
    assert_eq!(res.len(), 4);
    assert_row_at(
        &res,
        1,
        &[("id", "1"), ("test_id", "1"), ("data", "alpha_updated")],
    )?;
    assert_row_at(&res, 2, &[("id", "2"), ("test_id", "1"), ("data", "beta")])?;

    Ok(())
}
