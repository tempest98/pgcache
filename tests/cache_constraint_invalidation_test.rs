#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

use std::io::Error;

use crate::util::{TestContext, assert_row_at, wait_for_cdc};

mod util;

// TODO: Add mechanism to verify if cache was invalidated or not
// Currently tests verify correctness (results are accurate) but not optimization
// (whether invalidation occurred). Would be useful to expose invalidation metrics
// or events for testing the optimization behavior directly.

/// Consolidated test for cache constraint invalidation functionality.
/// Combines 5 individual tests into one to reduce setup overhead.
#[tokio::test]
async fn test_cache_constraint_invalidation() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    insert_matching_constraint(&mut ctx).await?;
    insert_non_matching_constraint(&mut ctx).await?;
    update_entering_result_set(&mut ctx).await?;
    update_leaving_result_set(&mut ctx).await?;
    update_non_join_column(&mut ctx).await?;
    update_where_column_entering_result_set(&mut ctx).await?;

    Ok(())
}

/// Test that INSERT with matching constraints properly caches the row
async fn insert_matching_constraint(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query(
        "create table test_match (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table test_map_match (id serial primary key, test_id integer, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_match (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_map_match (test_id, data) values (1, 'alpha'), (1, 'beta'), (2, 'gamma')",
        &[],
    )
    .await?;

    // Prime the cache with a query that has constraint test_map_match.test_id = 1
    // Base data has: (1, 1, 'alpha'), (2, 1, 'beta')
    let _ = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_match t join test_map_match tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // Insert row that matches constraint (test_id = 1)
    ctx.origin_query(
        "insert into test_map_match (test_id, data) values (1, 'delta')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Query should show all 3 rows with test_id = 1
    let res = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_match t join test_map_match tm on tm.test_id = t.id where t.id = 1 \
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
async fn insert_non_matching_constraint(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query(
        "create table test_nonmatch (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table test_map_nonmatch (id serial primary key, test_id integer, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_nonmatch (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_map_nonmatch (test_id, data) values (1, 'alpha'), (1, 'beta'), (2, 'gamma')",
        &[],
    )
    .await?;

    // Prime the cache with a query that has constraint test_map_nonmatch.test_id = 1
    let _ = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_nonmatch t join test_map_nonmatch tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // Insert row that does NOT match constraint (test_id = 5, not 1)
    // This should be optimized - no invalidation because row won't appear in results
    ctx.origin_query(
        "insert into test_map_nonmatch (test_id, data) values (5, 'no_match')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Query should still return only the original 2 rows (cache not invalidated, new row doesn't match)
    let res = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_nonmatch t join test_map_nonmatch tm on tm.test_id = t.id where t.id = 1 \
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
async fn update_entering_result_set(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query(
        "create table test_enter (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table test_map_enter (id serial primary key, test_id integer, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_enter (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_map_enter (test_id, data) values (1, 'alpha'), (1, 'beta'), (2, 'gamma')",
        &[],
    )
    .await?;

    // Prime the cache - query has constraint test_map_enter.test_id = 1
    // Base data: gamma has test_id = 2, so only alpha and beta match
    let _ = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_enter t join test_map_enter tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // UPDATE: Change gamma's test_id from 2 to 1 (entering result set)
    ctx.origin_query(
        "update test_map_enter set test_id = 1 where data = 'gamma'",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Query should now show 3 rows including gamma
    let res = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_enter t join test_map_enter tm on tm.test_id = t.id where t.id = 1 \
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
async fn update_leaving_result_set(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query(
        "create table test_leave (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table test_map_leave (id serial primary key, test_id integer, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_leave (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_map_leave (test_id, data) values (1, 'alpha'), (1, 'beta'), (2, 'gamma')",
        &[],
    )
    .await?;

    // Prime the cache - query has constraint test_map_leave.test_id = 1
    let _ = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_leave t join test_map_leave tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // UPDATE: Change alpha's test_id from 1 to 5 (leaving result set)
    // Optimization: no invalidation, UPDATE mechanism removes row from cache
    ctx.origin_query(
        "update test_map_leave set test_id = 5 where data = 'alpha'",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Query should now return 1 row (only beta)
    let res = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_leave t join test_map_leave tm on tm.test_id = t.id where t.id = 1 \
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
async fn update_non_join_column(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query(
        "create table test_nonjoin (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table test_map_nonjoin (id serial primary key, test_id integer, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_nonjoin (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_map_nonjoin (test_id, data) values (1, 'alpha'), (1, 'beta'), (2, 'gamma')",
        &[],
    )
    .await?;

    // Prime the cache
    let _ = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_nonjoin t join test_map_nonjoin tm on tm.test_id = t.id where t.id = 1 \
            order by tm.id",
        )
        .await?;

    // UPDATE: Change data field only (not JOIN column)
    // This should NOT invalidate
    ctx.origin_query(
        "update test_map_nonjoin set data = 'alpha_updated' where data = 'alpha'",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Query should show updated data
    let res = ctx
        .simple_query(
            "select tm.id, tm.test_id, tm.data \
            from test_nonjoin t join test_map_nonjoin tm on tm.test_id = t.id where t.id = 1 \
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

/// Test UPDATE where a WHERE column (not join column) changes to match the constraint.
/// This should invalidate because a row is entering the result set via WHERE clause.
///
/// Scenario: SELECT * FROM orders JOIN users ON orders.user_id = users.id WHERE users.status = 'active'
/// If users.status changes from 'inactive' to 'active', the user's orders should now appear.
async fn update_where_column_entering_result_set(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query(
        "create table users_where (id integer primary key, status text)",
        &[],
    )
    .await?;

    ctx.query(
        "create table orders_where (id serial primary key, user_id integer, amount integer)",
        &[],
    )
    .await?;

    // Insert users: user 1 is active, user 2 is inactive
    ctx.query(
        "insert into users_where (id, status) values (1, 'active'), (2, 'inactive')",
        &[],
    )
    .await?;

    // Insert orders for both users
    ctx.query(
        "insert into orders_where (user_id, amount) values (1, 100), (1, 200), (2, 300), (2, 400)",
        &[],
    )
    .await?;

    // Prime the cache with join query filtering by users.status = 'active'
    // Should only return orders for user 1 (amounts 100, 200)
    let res = ctx
        .simple_query(
            "select o.id, o.user_id, o.amount, u.status \
            from orders_where o join users_where u on o.user_id = u.id \
            where u.status = 'active' \
            order by o.id",
        )
        .await?;

    // RowDescription + 2 data rows + CommandComplete = 4
    assert_eq!(res.len(), 4);
    assert_row_at(
        &res,
        1,
        &[("user_id", "1"), ("amount", "100"), ("status", "active")],
    )?;
    assert_row_at(
        &res,
        2,
        &[("user_id", "1"), ("amount", "200"), ("status", "active")],
    )?;

    // UPDATE: Change user 2's status from 'inactive' to 'active'
    // This should invalidate the cache because user 2's orders should now appear
    ctx.origin_query("update users_where set status = 'active' where id = 2", &[])
        .await?;

    wait_for_cdc().await;

    // Query should now return 4 orders (both users are active)
    let res = ctx
        .simple_query(
            "select o.id, o.user_id, o.amount, u.status \
            from orders_where o join users_where u on o.user_id = u.id \
            where u.status = 'active' \
            order by o.id",
        )
        .await?;

    // RowDescription + 4 data rows + CommandComplete = 6
    assert_eq!(res.len(), 6);
    assert_row_at(
        &res,
        1,
        &[("user_id", "1"), ("amount", "100"), ("status", "active")],
    )?;
    assert_row_at(
        &res,
        2,
        &[("user_id", "1"), ("amount", "200"), ("status", "active")],
    )?;
    assert_row_at(
        &res,
        3,
        &[("user_id", "2"), ("amount", "300"), ("status", "active")],
    )?;
    assert_row_at(
        &res,
        4,
        &[("user_id", "2"), ("amount", "400"), ("status", "active")],
    )?;

    Ok(())
}
