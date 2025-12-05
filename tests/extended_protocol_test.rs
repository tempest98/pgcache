use std::io::Error;

use crate::util::{wait_cache_load, wait_for_cdc, TestContext};

mod util;

/// Setup basic test table with id and data columns
async fn setup_basic_test_table(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query(
        "create table test (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test (id, data) values (1, 'foo'), (2, 'bar')",
        &[],
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn test_extended_protocol_basic() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    setup_basic_test_table(&mut ctx).await?;

    // Use extended protocol with parameterized query
    // tokio-postgres uses extended protocol by default for prepared statements
    let stmt = ctx.prepare("select id, data from test where data = $1").await?;

    ctx.query(&stmt, &[&"foo"]).await?;
    wait_cache_load().await;
    let rows = ctx.query(&stmt, &[&"foo"]).await?;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[0].get::<_, &str>("data"), "foo");

    let metrics = ctx.metrics()?;
    assert_eq!(metrics.queries_total, 4);
    assert_eq!(metrics.queries_cacheable, 2);
    assert_eq!(metrics.queries_uncacheable, 2);
    assert_eq!(metrics.queries_unsupported, 2);
    assert_eq!(metrics.queries_cache_hit, 1);
    assert_eq!(metrics.queries_cache_miss, 1);

    Ok(())
}

#[tokio::test]
async fn test_extended_protocol_statement_reuse() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    // Prepare statement once
    let stmt = ctx.prepare("select id, data from test where data = $1").await?;

    // Execute multiple times with different parameters
    ctx.query(&stmt, &[&"foo"]).await?;
    wait_cache_load().await;
    let rows1 = ctx.query(&stmt, &[&"foo"]).await?;

    assert_eq!(rows1.len(), 1);
    assert_eq!(rows1[0].get::<_, i32>("id"), 1);
    assert_eq!(rows1[0].get::<_, &str>("data"), "foo");

    ctx.query(&stmt, &[&"bar"]).await?;
    wait_cache_load().await;
    let rows2 = ctx.query(&stmt, &[&"bar"]).await?;

    assert_eq!(rows2.len(), 1);
    assert_eq!(rows2[0].get::<_, i32>("id"), 2);
    assert_eq!(rows2[0].get::<_, &str>("data"), "bar");

    let metrics = ctx.metrics()?;
    assert_eq!(metrics.queries_total, 6);
    assert_eq!(metrics.queries_cacheable, 4);
    assert_eq!(metrics.queries_uncacheable, 2);
    assert_eq!(metrics.queries_unsupported, 2);
    assert_eq!(metrics.queries_cache_hit, 2);
    assert_eq!(metrics.queries_cache_miss, 2);

    Ok(())
}

#[tokio::test]
async fn test_extended_protocol_multiple_params() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test (id integer primary key, data text, value integer)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test (id, data, value) values (1, 'foo', 100), (2, 'bar', 200), (3, 'baz', 150)",
        &[],
    )
    .await?;

    // Prepare statement with multiple parameters
    let stmt = ctx
        .prepare("select id, data, value from test where data = $1 and value > $2")
        .await?;

    // Execute with multiple parameters
    ctx.query(&stmt, &[&"foo", &50]).await?;
    wait_cache_load().await;
    let rows = ctx.query(&stmt, &[&"foo", &50]).await?;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[0].get::<_, &str>("data"), "foo");
    assert_eq!(rows[0].get::<_, i32>("value"), 100);

    // Execute with different parameters - should not match
    ctx.query(&stmt, &[&"foo", &150]).await?;
    wait_cache_load().await;
    let rows = ctx.query(&stmt, &[&"foo", &150]).await?;
    assert_eq!(rows.len(), 0);

    let metrics = ctx.metrics()?;
    assert_eq!(metrics.queries_total, 6);
    assert_eq!(metrics.queries_cacheable, 4);
    assert_eq!(metrics.queries_uncacheable, 2);
    assert_eq!(metrics.queries_unsupported, 2);
    assert_eq!(metrics.queries_cache_hit, 2);
    assert_eq!(metrics.queries_cache_miss, 2);

    Ok(())
}

#[tokio::test]
async fn test_extended_protocol_null_parameter() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test (id integer primary key, data text)",
        &[],
    )
    .await?;

    // Insert test data with NULL
    ctx.query(
        "insert into test (id, data) values (1, 'foo'), (2, NULL)",
        &[],
    )
    .await?;

    // Prepare statement
    let stmt = ctx
        .prepare("select id, data from test where data IS NOT DISTINCT FROM $1")
        .await?;

    // Execute with NULL parameter
    let null_data: Option<&str> = None;
    let rows = ctx.query(&stmt, &[&null_data]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 2);
    assert_eq!(rows[0].get::<_, Option<&str>>("data"), None);

    Ok(())
}

#[tokio::test]
async fn test_extended_protocol_unnamed_statement() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    setup_basic_test_table(&mut ctx).await?;

    // Use query_one which creates unnamed statements
    // Each execution replaces the previous unnamed statement
    let row = ctx.query_one("select data from test where id = $1", &[&1]).await?;
    assert_eq!(row.get::<_, &str>("data"), "foo");

    let row = ctx.query_one("select data from test where id = $1", &[&2]).await?;
    assert_eq!(row.get::<_, &str>("data"), "bar");

    Ok(())
}

#[tokio::test]
async fn test_extended_protocol_insert_returning() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test (id serial primary key, data text)",
        &[],
    )
    .await?;

    // Prepare INSERT RETURNING statement
    let stmt = ctx
        .prepare("insert into test (data) values ($1) returning id, data")
        .await?;

    // Execute insert
    let rows = ctx.query(&stmt, &[&"foo"]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[0].get::<_, &str>("data"), "foo");

    // Execute another insert
    let rows = ctx.query(&stmt, &[&"bar"]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 2);
    assert_eq!(rows[0].get::<_, &str>("data"), "bar");

    Ok(())
}

#[tokio::test]
async fn test_extended_protocol_parameterized_cache_hit() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    setup_basic_test_table(&mut ctx).await?;

    // Prepare parameterized SELECT statement
    let stmt = ctx
        .prepare("select id, data from test where data = $1 order by id")
        .await?;

    // First execution with 'foo' - populates cache
    ctx.query(&stmt, &[&"foo"]).await?;
    wait_cache_load().await;

    // Second execution with same parameter - should hit cache
    let rows = ctx.query(&stmt, &[&"foo"]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[0].get::<_, &str>("data"), "foo");

    // Insert new row directly to origin to trigger CDC cache update
    ctx.origin_query("insert into test (id, data) values (3, 'foo')", &[])
        .await?;
    wait_for_cdc().await;

    // Query again - cache was updated via CDC, new row should appear
    let rows = ctx.query(&stmt, &[&"foo"]).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[1].get::<_, i32>("id"), 3);

    // Query with different parameter - creates separate cache entry
    ctx.query(&stmt, &[&"bar"]).await?;
    wait_cache_load().await;

    // Second execution with 'bar' - should hit cache
    let rows = ctx.query(&stmt, &[&"bar"]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 2);
    assert_eq!(rows[0].get::<_, &str>("data"), "bar");

    // Check final metrics
    // queries: create table, insert, foo x3, bar x2 = 7 total, 5 cacheable (selects)
    // cache behavior: foo miss, foo hit, foo hit (CDC updated cache), bar miss, bar hit
    let metrics = ctx.metrics()?;
    assert_eq!(metrics.queries_total, 7);
    assert_eq!(metrics.queries_cacheable, 5);
    assert_eq!(metrics.queries_cache_miss, 2); // foo (initial), bar (initial)
    assert_eq!(metrics.queries_cache_hit, 3);  // foo (second), foo (after CDC), bar (second)

    Ok(())
}
