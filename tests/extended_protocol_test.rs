use std::io::Error;

use crate::util::{PgCacheProcess, connect_pgcache, query, start_databases, wait_cache_load};

mod util;

/// Setup basic test table with id and data columns
async fn setup_basic_test_table(
    pgcache: &mut PgCacheProcess,
    client: &tokio_postgres::Client,
) -> Result<(), Error> {
    query(
        pgcache,
        client,
        "create table test (id integer primary key, data text)",
        &[],
    )
    .await?;

    query(
        pgcache,
        client,
        "insert into test (id, data) values (1, 'foo'), (2, 'bar')",
        &[],
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_extended_protocol_basic() -> Result<(), Error> {
    let (dbs, _origin) = start_databases().await?;
    let (mut pgcache, client) = connect_pgcache(&dbs).await?;

    setup_basic_test_table(&mut pgcache, &client).await?;

    // Use extended protocol with parameterized query
    // tokio-postgres uses extended protocol by default for prepared statements
    let stmt = client
        .prepare("select id, data from test where data = $1")
        .await
        .map_err(Error::other)?;

    let rows = query(&mut pgcache, &client, &stmt, &[&"foo"]).await?;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[0].get::<_, &str>("data"), "foo");

    wait_cache_load().await;

    let rows = query(&mut pgcache, &client, &stmt, &[&"foo"]).await?;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[0].get::<_, &str>("data"), "foo");

    Ok(())
}

#[tokio::test]
async fn test_extended_protocol_statement_reuse() -> Result<(), Error> {
    let (dbs, _origin) = start_databases().await?;
    let (mut pgcache, client) = connect_pgcache(&dbs).await?;

    query(
        &mut pgcache,
        &client,
        "create table test (id integer primary key, data text)",
        &[],
    )
    .await?;

    query(
        &mut pgcache,
        &client,
        "insert into test (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    // Prepare statement once
    let stmt = client
        .prepare("select id, data from test where data = $1")
        .await
        .map_err(Error::other)?;

    // Execute multiple times with different parameters
    let rows1 = query(&mut pgcache, &client, &stmt, &[&"foo"]).await?;
    assert_eq!(rows1.len(), 1);
    assert_eq!(rows1[0].get::<_, i32>("id"), 1);
    assert_eq!(rows1[0].get::<_, &str>("data"), "foo");

    let rows2 = query(&mut pgcache, &client, &stmt, &[&"bar"]).await?;
    assert_eq!(rows2.len(), 1);
    assert_eq!(rows2[0].get::<_, i32>("id"), 2);
    assert_eq!(rows2[0].get::<_, &str>("data"), "bar");

    let rows3 = query(&mut pgcache, &client, &stmt, &[&"baz"]).await?;
    assert_eq!(rows3.len(), 1);
    assert_eq!(rows3[0].get::<_, i32>("id"), 3);
    assert_eq!(rows3[0].get::<_, &str>("data"), "baz");

    Ok(())
}

#[tokio::test]
async fn test_extended_protocol_multiple_params() -> Result<(), Error> {
    let (dbs, _origin) = start_databases().await?;
    let (mut pgcache, client) = connect_pgcache(&dbs).await?;

    query(
        &mut pgcache,
        &client,
        "create table test (id integer primary key, data text, value integer)",
        &[],
    )
    .await?;

    query(
        &mut pgcache,
        &client,
        "insert into test (id, data, value) values (1, 'foo', 100), (2, 'bar', 200), (3, 'baz', 150)",
        &[],
    )
    .await?;

    // Prepare statement with multiple parameters
    let stmt = client
        .prepare("select id, data, value from test where data = $1 and value > $2")
        .await
        .map_err(Error::other)?;

    // Execute with multiple parameters
    let rows = query(&mut pgcache, &client, &stmt, &[&"foo", &50]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[0].get::<_, &str>("data"), "foo");
    assert_eq!(rows[0].get::<_, i32>("value"), 100);

    // Execute with different parameters - should not match
    let rows = query(&mut pgcache, &client, &stmt, &[&"foo", &150]).await?;
    assert_eq!(rows.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_extended_protocol_null_parameter() -> Result<(), Error> {
    let (dbs, _origin) = start_databases().await?;
    let (mut pgcache, client) = connect_pgcache(&dbs).await?;

    query(
        &mut pgcache,
        &client,
        "create table test (id integer primary key, data text)",
        &[],
    )
    .await?;

    // Insert test data with NULL
    query(
        &mut pgcache,
        &client,
        "insert into test (id, data) values (1, 'foo'), (2, NULL)",
        &[],
    )
    .await?;

    // Prepare statement
    let stmt = client
        .prepare("select id, data from test where data IS NOT DISTINCT FROM $1")
        .await
        .map_err(Error::other)?;

    // Execute with NULL parameter
    let null_data: Option<&str> = None;
    let rows = query(&mut pgcache, &client, &stmt, &[&null_data]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 2);
    assert_eq!(rows[0].get::<_, Option<&str>>("data"), None);

    Ok(())
}

#[tokio::test]
async fn test_extended_protocol_unnamed_statement() -> Result<(), Error> {
    let (dbs, _origin) = start_databases().await?;
    let (mut pgcache, client) = connect_pgcache(&dbs).await?;

    setup_basic_test_table(&mut pgcache, &client).await?;

    // Use query_one which creates unnamed statements
    // Each execution replaces the previous unnamed statement
    let row = client
        .query_one("select data from test where id = $1", &[&1])
        .await
        .map_err(Error::other)?;
    assert_eq!(row.get::<_, &str>("data"), "foo");

    let row = client
        .query_one("select data from test where id = $1", &[&2])
        .await
        .map_err(Error::other)?;
    assert_eq!(row.get::<_, &str>("data"), "bar");

    Ok(())
}

#[tokio::test]
async fn test_extended_protocol_insert_returning() -> Result<(), Error> {
    let (dbs, _origin) = start_databases().await?;
    let (mut pgcache, client) = connect_pgcache(&dbs).await?;

    query(
        &mut pgcache,
        &client,
        "create table test (id serial primary key, data text)",
        &[],
    )
    .await?;

    // Prepare INSERT RETURNING statement
    let stmt = client
        .prepare("insert into test (data) values ($1) returning id, data")
        .await
        .map_err(Error::other)?;

    // Execute insert
    let rows = query(&mut pgcache, &client, &stmt, &[&"foo"]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[0].get::<_, &str>("data"), "foo");

    // Execute another insert
    let rows = query(&mut pgcache, &client, &stmt, &[&"bar"]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 2);
    assert_eq!(rows[0].get::<_, &str>("data"), "bar");

    Ok(())
}

// TODO: Enable this test once cache metrics are available
// Currently the cache falls back to origin on any error, making it difficult to verify
// that parameterized queries are actually hitting the cache vs being forwarded.
// Once metrics (cache hits, misses, forwarding) are exposed, this test can verify
// that the second execution with same parameters is a cache hit.
//
// #[tokio::test]
// async fn test_extended_protocol_parameterized_cache_hit() -> Result<(), Error> {
//     use crate::util::{simple_query, wait_for_cdc};
//
//     let (dbs, origin) = start_databases().await?;
//     let (mut pgcache, client) = connect_pgcache(&dbs).await?;
//
//     setup_basic_test_table(&mut pgcache, &client).await?;
//
//     // Prepare parameterized SELECT statement
//     let stmt = client
//         .prepare("select id, data from test where data = $1 order by id")
//         .await
//         .map_err(Error::other)?;
//
//     // First execution - populates cache
//     let rows1 = query(&mut pgcache, &client, &stmt, &[&"foo"]).await?;
//     assert_eq!(rows1.len(), 1);
//     assert_eq!(rows1[0].get::<_, i32>("id"), 1);
//     assert_eq!(rows1[0].get::<_, &str>("data"), "foo");
//
//     // TODO: Check metrics - should show 1 cache miss, 0 cache hits
//
//     // Second execution with same parameter - should hit cache
//     let rows2 = query(&mut pgcache, &client, &stmt, &[&"foo"]).await?;
//     assert_eq!(rows2.len(), 1);
//     assert_eq!(rows2[0].get::<_, i32>("id"), 1);
//
//     // TODO: Check metrics - should show 1 cache miss, 1 cache hit
//
//     // Insert new row directly to origin to trigger CDC
//     query(
//         &mut pgcache,
//         &origin,
//         "insert into test (id, data) values (3, 'foo')",
//         &[],
//     )
//     .await?;
//
//     wait_for_cdc().await;
//
//     // Query again - cache should be updated via CDC to include new row
//     let rows3 = query(&mut pgcache, &client, &stmt, &[&"foo"]).await?;
//     assert_eq!(rows3.len(), 2);
//     assert_eq!(rows3[0].get::<_, i32>("id"), 1);
//     assert_eq!(rows3[1].get::<_, i32>("id"), 3);
//
//     // TODO: Check metrics - should still show cache hit (CDC updated the cache)
//
//     // Query with different parameter - should create separate cache entry
//     let rows4 = query(&mut pgcache, &client, &stmt, &[&"bar"]).await?;
//     assert_eq!(rows4.len(), 1);
//     assert_eq!(rows4[0].get::<_, i32>("id"), 2);
//     assert_eq!(rows4[0].get::<_, &str>("data"), "bar");
//
//     // TODO: Check metrics - should show 2 cache misses, 2 cache hits
//     // (one miss for 'foo', one for 'bar', two hits for repeated 'foo' queries)
//
//     Ok(())
// }
