#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

use std::io::Error;

use crate::util::{TestContext, metrics_delta, wait_cache_load, wait_for_cdc};

mod util;

/// Test basic extended protocol with parameterized query
#[tokio::test]
async fn test_extended_protocol_basic() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    let before = ctx.metrics().await?;

    ctx.query(
        "create table test_basic (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_basic (id, data) values (1, 'foo'), (2, 'bar')",
        &[],
    )
    .await?;

    let stmt = ctx
        .prepare("select id, data from test_basic where data = $1")
        .await?;

    ctx.query(&stmt, &[&"foo"]).await?;
    wait_cache_load().await;
    let rows = ctx.query(&stmt, &[&"foo"]).await?;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[0].get::<_, &str>("data"), "foo");

    let after = ctx.metrics().await?;

    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.queries_total, 4);
    assert_eq!(delta.queries_cacheable, 2);
    assert_eq!(delta.queries_uncacheable, 2);
    assert_eq!(delta.queries_unsupported, 2);
    assert_eq!(delta.queries_cache_hit, 1);
    assert_eq!(delta.queries_cache_miss, 1);

    Ok(())
}

/// Test statement reuse with different parameter values
#[tokio::test]
async fn test_extended_protocol_statement_reuse() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    let before = ctx.metrics().await?;

    ctx.query(
        "create table test_reuse (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_reuse (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    let stmt = ctx
        .prepare("select id, data from test_reuse where data = $1")
        .await?;

    // First parameter value
    ctx.query(&stmt, &[&"foo"]).await?;
    wait_cache_load().await;
    let rows1 = ctx.query(&stmt, &[&"foo"]).await?;

    assert_eq!(rows1.len(), 1);
    assert_eq!(rows1[0].get::<_, i32>("id"), 1);
    assert_eq!(rows1[0].get::<_, &str>("data"), "foo");

    // Second parameter value
    ctx.query(&stmt, &[&"bar"]).await?;
    wait_cache_load().await;
    let rows2 = ctx.query(&stmt, &[&"bar"]).await?;

    assert_eq!(rows2.len(), 1);
    assert_eq!(rows2[0].get::<_, i32>("id"), 2);
    assert_eq!(rows2[0].get::<_, &str>("data"), "bar");

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.queries_total, 6);
    assert_eq!(delta.queries_cacheable, 4);
    assert_eq!(delta.queries_uncacheable, 2);
    assert_eq!(delta.queries_unsupported, 2);
    assert_eq!(delta.queries_cache_hit, 2);
    assert_eq!(delta.queries_cache_miss, 2);

    Ok(())
}

/// Test query with multiple parameters
#[tokio::test]
async fn test_extended_protocol_multiple_params() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    let before = ctx.metrics().await?;

    ctx.query(
        "create table test_multi (id integer primary key, data text, value integer)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_multi (id, data, value) values (1, 'foo', 100), (2, 'bar', 200), (3, 'baz', 150)",
        &[],
    )
    .await?;

    let stmt = ctx
        .prepare("select id, data, value from test_multi where data = $1 and value > $2")
        .await?;

    // First parameter combination
    ctx.query(&stmt, &[&"foo", &50]).await?;
    wait_cache_load().await;
    let rows = ctx.query(&stmt, &[&"foo", &50]).await?;

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[0].get::<_, &str>("data"), "foo");
    assert_eq!(rows[0].get::<_, i32>("value"), 100);

    // Different parameters - should not match
    ctx.query(&stmt, &[&"foo", &150]).await?;
    wait_cache_load().await;
    let rows = ctx.query(&stmt, &[&"foo", &150]).await?;
    assert_eq!(rows.len(), 0);

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.queries_total, 6);
    assert_eq!(delta.queries_cacheable, 4);
    assert_eq!(delta.queries_uncacheable, 2);
    assert_eq!(delta.queries_unsupported, 2);
    assert_eq!(delta.queries_cache_hit, 2);
    assert_eq!(delta.queries_cache_miss, 2);

    Ok(())
}

/// Test NULL parameter handling
#[tokio::test]
async fn test_extended_protocol_null_parameter() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test_null (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_null (id, data) values (1, 'foo'), (2, NULL)",
        &[],
    )
    .await?;

    let stmt = ctx
        .prepare("select id, data from test_null where data IS NOT DISTINCT FROM $1")
        .await?;

    let null_data: Option<&str> = None;
    let rows = ctx.query(&stmt, &[&null_data]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 2);
    assert_eq!(rows[0].get::<_, Option<&str>>("data"), None);

    Ok(())
}

/// Test unnamed (inline) statements
#[tokio::test]
async fn test_extended_protocol_unnamed_statement() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test_unnamed (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_unnamed (id, data) values (1, 'foo'), (2, 'bar')",
        &[],
    )
    .await?;

    // query_one creates unnamed statements - each execution replaces the previous
    let row = ctx
        .query_one("select data from test_unnamed where id = $1", &[&1])
        .await?;
    assert_eq!(row.get::<_, &str>("data"), "foo");

    let row = ctx
        .query_one("select data from test_unnamed where id = $1", &[&2])
        .await?;
    assert_eq!(row.get::<_, &str>("data"), "bar");

    Ok(())
}

/// Test INSERT RETURNING statement
#[tokio::test]
async fn test_extended_protocol_insert_returning() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table test_returning (id serial primary key, data text)",
        &[],
    )
    .await?;

    let stmt = ctx
        .prepare("insert into test_returning (data) values ($1) returning id, data")
        .await?;

    let rows = ctx.query(&stmt, &[&"foo"]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[0].get::<_, &str>("data"), "foo");

    let rows = ctx.query(&stmt, &[&"bar"]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 2);
    assert_eq!(rows[0].get::<_, &str>("data"), "bar");

    Ok(())
}

/// Test parameterized cache hit with CDC updates
#[tokio::test]
async fn test_extended_protocol_parameterized_cache_hit() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    let before = ctx.metrics().await?;

    ctx.query(
        "create table test_cache_hit (id integer primary key, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_cache_hit (id, data) values (1, 'foo'), (2, 'bar')",
        &[],
    )
    .await?;

    let stmt = ctx
        .prepare("select id, data from test_cache_hit where data = $1 order by id")
        .await?;

    // First execution - populates cache
    ctx.query(&stmt, &[&"foo"]).await?;
    wait_cache_load().await;

    // Second execution - should hit cache
    let rows = ctx.query(&stmt, &[&"foo"]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[0].get::<_, &str>("data"), "foo");

    // Insert directly to origin to trigger CDC
    ctx.origin_query(
        "insert into test_cache_hit (id, data) values (3, 'foo')",
        &[],
    )
    .await?;
    wait_for_cdc().await;

    // Query again - cache was updated via CDC, new row should appear
    let rows = ctx.query(&stmt, &[&"foo"]).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[1].get::<_, i32>("id"), 3);

    // Different parameter - creates separate cache entry
    ctx.query(&stmt, &[&"bar"]).await?;
    wait_cache_load().await;

    // Cache hit for 'bar'
    let rows = ctx.query(&stmt, &[&"bar"]).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>("id"), 2);
    assert_eq!(rows[0].get::<_, &str>("data"), "bar");

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.queries_total, 7);
    assert_eq!(delta.queries_cacheable, 5);
    assert_eq!(delta.queries_cache_miss, 2); // foo (initial), bar (initial)
    assert_eq!(delta.queries_cache_hit, 3); // foo (second), foo (after CDC), bar (second)

    Ok(())
}
