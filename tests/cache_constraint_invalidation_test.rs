use std::{io::Error, time::Duration};

use tokio::time::sleep;
use tokio_postgres::SimpleQueryMessage;

use crate::util::{connect_pgcache, query, setup_constraint_test_tables, simple_query, start_databases};

mod util;

// TODO: Add mechanism to verify if cache was invalidated or not
// Currently tests verify correctness (results are accurate) but not optimization
// (whether invalidation occurred). Would be useful to expose invalidation metrics
// or events for testing the optimization behavior directly.

/// Test that INSERT with matching constraints properly caches the row
#[tokio::test]
async fn test_insert_matching_constraint() -> Result<(), Error> {
    let (dbs, origin) = start_databases().await?;
    let (mut pgcache, client) = connect_pgcache(&dbs).await?;

    setup_constraint_test_tables(&mut pgcache, &client).await?;

    // Prime the cache with a query that has constraint test_map.test_id = 1
    // Base data has: (1, 1, 'alpha'), (2, 1, 'beta')
    let _ = simple_query(
        &mut pgcache,
        &client,
        "select tm.id, tm.test_id, tm.data \
        from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
        order by tm.id",
    )
    .await?;

    // Insert row that matches constraint (test_id = 1)
    query(
        &mut pgcache,
        &origin,
        "insert into test_map (test_id, data) values (1, 'delta')",
        &[],
    )
    .await?;

    sleep(Duration::from_millis(250)).await;

    // Query should show all 3 rows with test_id = 1
    let res = simple_query(
        &mut pgcache,
        &client,
        "select tm.id, tm.test_id, tm.data \
        from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
        order by tm.id",
    )
    .await?;

    // RowDescription + 3 data rows + CommandComplete = 5
    assert_eq!(res.len(), 5);

    // Verify first row (alpha)
    let SimpleQueryMessage::Row(row) = &res[1] else {
        panic!("expected SimpleQueryMessage::Row at index 1");
    };
    assert_eq!(row.get::<&str>("id"), Some("1"));
    assert_eq!(row.get::<&str>("test_id"), Some("1"));
    assert_eq!(row.get::<&str>("data"), Some("alpha"));

    // Verify second row (beta)
    let SimpleQueryMessage::Row(row) = &res[2] else {
        panic!("expected SimpleQueryMessage::Row at index 2");
    };
    assert_eq!(row.get::<&str>("id"), Some("2"));
    assert_eq!(row.get::<&str>("test_id"), Some("1"));
    assert_eq!(row.get::<&str>("data"), Some("beta"));

    // Verify third row (delta - the newly inserted row)
    let SimpleQueryMessage::Row(row) = &res[3] else {
        panic!("expected SimpleQueryMessage::Row at index 3");
    };
    assert_eq!(row.get::<&str>("id"), Some("4"));
    assert_eq!(row.get::<&str>("test_id"), Some("1"));
    assert_eq!(row.get::<&str>("data"), Some("delta"));

    pgcache.kill().expect("command killed");
    pgcache.wait().expect("exit_status");

    Ok(())
}

/// Test that INSERT with non-matching constraints is optimized (no invalidation)
#[tokio::test]
async fn test_insert_non_matching_constraint() -> Result<(), Error> {
    let (dbs, origin) = start_databases().await?;
    let (mut pgcache, client) = connect_pgcache(&dbs).await?;

    setup_constraint_test_tables(&mut pgcache, &client).await?;

    // Prime the cache with a query that has constraint test_map.test_id = 1
    let _ = simple_query(
        &mut pgcache,
        &client,
        "select tm.id, tm.test_id, tm.data \
        from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
        order by tm.id",
    )
    .await?;

    // Insert row that does NOT match constraint (test_id = 5, not 1)
    // This should be optimized - no invalidation because row won't appear in results
    query(
        &mut pgcache,
        &origin,
        "insert into test_map (test_id, data) values (5, 'no_match')",
        &[],
    )
    .await?;

    sleep(Duration::from_millis(250)).await;

    // Query should still return only the original 2 rows (cache not invalidated, new row doesn't match)
    let res = simple_query(
        &mut pgcache,
        &client,
        "select tm.id, tm.test_id, tm.data \
        from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
        order by tm.id",
    )
    .await?;

    // RowDescription + 2 data rows + CommandComplete = 4
    assert_eq!(res.len(), 4);

    // Verify first row (alpha)
    let SimpleQueryMessage::Row(row) = &res[1] else {
        panic!("expected SimpleQueryMessage::Row at index 1");
    };
    assert_eq!(row.get::<&str>("id"), Some("1"));
    assert_eq!(row.get::<&str>("test_id"), Some("1"));
    assert_eq!(row.get::<&str>("data"), Some("alpha"));

    // Verify second row (beta)
    let SimpleQueryMessage::Row(row) = &res[2] else {
        panic!("expected SimpleQueryMessage::Row at index 2");
    };
    assert_eq!(row.get::<&str>("id"), Some("2"));
    assert_eq!(row.get::<&str>("test_id"), Some("1"));
    assert_eq!(row.get::<&str>("data"), Some("beta"));

    pgcache.kill().expect("command killed");
    pgcache.wait().expect("exit_status");

    Ok(())
}

/// Test UPDATE where JOIN column changes from non-matching to matching value
/// This should invalidate because row is entering the result set
#[tokio::test]
async fn test_update_entering_result_set() -> Result<(), Error> {
    let (dbs, origin) = start_databases().await?;
    let (mut pgcache, client) = connect_pgcache(&dbs).await?;

    setup_constraint_test_tables(&mut pgcache, &client).await?;

    // Prime the cache - query has constraint test_map.test_id = 1
    // Base data: gamma has test_id = 2, so only alpha and beta match
    let _ = simple_query(
        &mut pgcache,
        &client,
        "select tm.id, tm.test_id, tm.data \
        from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
        order by tm.id",
    )
    .await?;

    // UPDATE: Change gamma's test_id from 2 to 1 (entering result set)
    query(
        &mut pgcache,
        &origin,
        "update test_map set test_id = 1 where data = 'gamma'",
        &[],
    )
    .await?;

    sleep(Duration::from_millis(250)).await;

    // Query should now show 3 rows including gamma
    let res = simple_query(
        &mut pgcache,
        &client,
        "select tm.id, tm.test_id, tm.data \
        from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
        order by tm.id",
    )
    .await?;

    // RowDescription + 3 data rows + CommandComplete = 5
    assert_eq!(res.len(), 5);

    // Verify first row (alpha)
    let SimpleQueryMessage::Row(row) = &res[1] else {
        panic!("expected SimpleQueryMessage::Row at index 1");
    };
    assert_eq!(row.get::<&str>("id"), Some("1"));
    assert_eq!(row.get::<&str>("test_id"), Some("1"));
    assert_eq!(row.get::<&str>("data"), Some("alpha"));

    // Verify second row (beta)
    let SimpleQueryMessage::Row(row) = &res[2] else {
        panic!("expected SimpleQueryMessage::Row at index 2");
    };
    assert_eq!(row.get::<&str>("id"), Some("2"));
    assert_eq!(row.get::<&str>("test_id"), Some("1"));
    assert_eq!(row.get::<&str>("data"), Some("beta"));

    // Verify third row (gamma - now entering result set)
    let SimpleQueryMessage::Row(row) = &res[3] else {
        panic!("expected SimpleQueryMessage::Row at index 3");
    };
    assert_eq!(row.get::<&str>("id"), Some("3"));
    assert_eq!(row.get::<&str>("test_id"), Some("1"));
    assert_eq!(row.get::<&str>("data"), Some("gamma"));

    pgcache.kill().expect("command killed");
    pgcache.wait().expect("exit_status");

    Ok(())
}

/// Test UPDATE where JOIN column changes from matching to non-matching value
/// This should NOT invalidate because row is leaving the result set (UPDATE handles removal)
#[tokio::test]
async fn test_update_leaving_result_set() -> Result<(), Error> {
    let (dbs, origin) = start_databases().await?;
    let (mut pgcache, client) = connect_pgcache(&dbs).await?;

    setup_constraint_test_tables(&mut pgcache, &client).await?;

    // Prime the cache - query has constraint test_map.test_id = 1
    let _ = simple_query(
        &mut pgcache,
        &client,
        "select tm.id, tm.test_id, tm.data \
        from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
        order by tm.id",
    )
    .await?;

    // UPDATE: Change alpha's test_id from 1 to 5 (leaving result set)
    // Optimization: no invalidation, UPDATE mechanism removes row from cache
    query(
        &mut pgcache,
        &origin,
        "update test_map set test_id = 5 where data = 'alpha'",
        &[],
    )
    .await?;

    sleep(Duration::from_millis(250)).await;

    // Query should now return 1 row (only beta)
    let res = simple_query(
        &mut pgcache,
        &client,
        "select tm.id, tm.test_id, tm.data \
        from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
        order by tm.id",
    )
    .await?;

    // RowDescription + 1 data row + CommandComplete = 3
    assert_eq!(res.len(), 3);

    // Verify only beta remains
    let SimpleQueryMessage::Row(row) = &res[1] else {
        panic!("expected SimpleQueryMessage::Row at index 1");
    };
    assert_eq!(row.get::<&str>("id"), Some("2"));
    assert_eq!(row.get::<&str>("test_id"), Some("1"));
    assert_eq!(row.get::<&str>("data"), Some("beta"));

    pgcache.kill().expect("command killed");
    pgcache.wait().expect("exit_status");

    Ok(())
}

/// Test UPDATE where non-JOIN column changes (data field)
/// This should NOT invalidate because JOIN key is unchanged
#[tokio::test]
async fn test_update_non_join_column() -> Result<(), Error> {
    let (dbs, origin) = start_databases().await?;
    let (mut pgcache, client) = connect_pgcache(&dbs).await?;

    setup_constraint_test_tables(&mut pgcache, &client).await?;

    // Prime the cache
    let _ = simple_query(
        &mut pgcache,
        &client,
        "select tm.id, tm.test_id, tm.data \
        from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
        order by tm.id",
    )
    .await?;

    // UPDATE: Change data field only (not JOIN column)
    // This should NOT invalidate
    query(
        &mut pgcache,
        &origin,
        "update test_map set data = 'alpha_updated' where data = 'alpha'",
        &[],
    )
    .await?;

    sleep(Duration::from_millis(250)).await;

    // Query should show updated data
    let res = simple_query(
        &mut pgcache,
        &client,
        "select tm.id, tm.test_id, tm.data \
        from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
        order by tm.id",
    )
    .await?;

    // RowDescription + 2 data rows + CommandComplete = 4
    assert_eq!(res.len(), 4);

    // Verify first row (alpha_updated)
    let SimpleQueryMessage::Row(row) = &res[1] else {
        panic!("expected SimpleQueryMessage::Row at index 1");
    };
    assert_eq!(row.get::<&str>("id"), Some("1"));
    assert_eq!(row.get::<&str>("test_id"), Some("1"));
    assert_eq!(row.get::<&str>("data"), Some("alpha_updated"));

    // Verify second row (beta)
    let SimpleQueryMessage::Row(row) = &res[2] else {
        panic!("expected SimpleQueryMessage::Row at index 2");
    };
    assert_eq!(row.get::<&str>("id"), Some("2"));
    assert_eq!(row.get::<&str>("test_id"), Some("1"));
    assert_eq!(row.get::<&str>("data"), Some("beta"));

    pgcache.kill().expect("command killed");
    pgcache.wait().expect("exit_status");

    Ok(())
}
