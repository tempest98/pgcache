#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

use std::io::Error;

use crate::util::{
    TestContext, assert_cache_hit, assert_cache_miss, assert_row_at, metrics_delta, wait_cache_load,
};

mod util;

/// Allowlisted table queries are cached normally.
#[tokio::test]
async fn test_allowlist_allows_configured_table() -> Result<(), Error> {
    let mut ctx = TestContext::setup_allowlist("allowed_table").await?;

    ctx.query(
        "CREATE TABLE allowed_table (id int primary key, data text)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO allowed_table VALUES (1, 'hello')", &[])
        .await?;

    // First query — cache miss (query admitted to cache)
    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query("SELECT * FROM allowed_table WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("data", "hello")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // Second query — cache hit
    let res = ctx
        .simple_query("SELECT * FROM allowed_table WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("data", "hello")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Non-allowlisted table queries are forwarded and the allowlist_skipped metric increments.
#[tokio::test]
async fn test_allowlist_rejects_non_configured_table() -> Result<(), Error> {
    let mut ctx = TestContext::setup_allowlist("allowed_table").await?;

    ctx.query(
        "CREATE TABLE allowed_table (id int primary key, data text)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE blocked_table (id int primary key, data text)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO blocked_table VALUES (1, 'secret')", &[])
        .await?;

    // Query against non-allowlisted table — forwarded, never cached
    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query("SELECT * FROM blocked_table WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("data", "secret")])?;
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert_eq!(
        delta.queries_allowlist_skipped, 1,
        "expected allowlist skip"
    );
    assert_eq!(delta.queries_cache_hit, 0, "unexpected cache hit");

    wait_cache_load().await;

    // Repeat — still forwarded, never becomes a cache hit
    let m2 = ctx.metrics().await?;
    let res = ctx
        .simple_query("SELECT * FROM blocked_table WHERE id = 1")
        .await?;
    assert_row_at(&res, 1, &[("id", "1"), ("data", "secret")])?;
    let after2 = ctx.metrics().await?;
    let delta2 = metrics_delta(&m2, &after2);
    assert_eq!(
        delta2.queries_allowlist_skipped, 1,
        "expected allowlist skip on repeat"
    );
    assert_eq!(
        delta2.queries_cache_hit, 0,
        "unexpected cache hit on repeat"
    );

    Ok(())
}

/// A join between a allowlisted and non-allowlisted table is forwarded.
#[tokio::test]
async fn test_allowlist_rejects_mixed_join() -> Result<(), Error> {
    let mut ctx = TestContext::setup_allowlist("allowed_table").await?;

    ctx.query(
        "CREATE TABLE allowed_table (id int primary key, data text)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE TABLE blocked_table (id int primary key, ref_id int)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO allowed_table VALUES (1, 'hello')", &[])
        .await?;
    ctx.query("INSERT INTO blocked_table VALUES (1, 1)", &[])
        .await?;

    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query("SELECT a.data FROM allowed_table a JOIN blocked_table b ON a.id = b.ref_id")
        .await?;
    assert_row_at(&res, 1, &[("data", "hello")])?;
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert_eq!(
        delta.queries_allowlist_skipped, 1,
        "expected allowlist skip for mixed join"
    );

    Ok(())
}

/// Multiple tables can be allowlisted via comma-separated config.
#[tokio::test]
async fn test_allowlist_multiple_tables() -> Result<(), Error> {
    let mut ctx = TestContext::setup_allowlist("table_a,table_b").await?;

    ctx.query("CREATE TABLE table_a (id int primary key, data text)", &[])
        .await?;
    ctx.query("CREATE TABLE table_b (id int primary key, ref_id int)", &[])
        .await?;
    ctx.query("CREATE TABLE table_c (id int primary key)", &[])
        .await?;
    ctx.query("INSERT INTO table_a VALUES (1, 'a')", &[])
        .await?;
    ctx.query("INSERT INTO table_b VALUES (1, 1)", &[]).await?;
    ctx.query("INSERT INTO table_c VALUES (1)", &[]).await?;

    // Join of two allowlisted tables — cacheable
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT a.data FROM table_a a JOIN table_b b ON a.id = b.ref_id")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    ctx.simple_query("SELECT a.data FROM table_a a JOIN table_b b ON a.id = b.ref_id")
        .await?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Query against non-allowlisted table_c — forwarded
    ctx.simple_query("SELECT * FROM table_c WHERE id = 1")
        .await?;
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert_eq!(
        delta.queries_allowlist_skipped, 1,
        "expected allowlist skip for table_c"
    );

    Ok(())
}

/// Schema-qualified allowlist entry only matches the specified schema.
#[tokio::test]
async fn test_allowlist_schema_qualified() -> Result<(), Error> {
    let mut ctx = TestContext::setup_allowlist("public.target_table").await?;

    ctx.query(
        "CREATE TABLE public.target_table (id int primary key, data text)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO public.target_table VALUES (1, 'yes')", &[])
        .await?;

    // Explicit public.target_table — matches allowlist
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM public.target_table WHERE id = 1")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    ctx.simple_query("SELECT * FROM public.target_table WHERE id = 1")
        .await?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Unqualified allowlist entry matches the table regardless of schema in the query.
#[tokio::test]
async fn test_allowlist_unqualified_matches_any_schema() -> Result<(), Error> {
    let mut ctx = TestContext::setup_allowlist("my_table").await?;

    ctx.query("CREATE TABLE my_table (id int primary key, data text)", &[])
        .await?;
    ctx.query("INSERT INTO my_table VALUES (1, 'val')", &[])
        .await?;

    // Query with explicit public schema — unqualified allowlist should match
    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM public.my_table WHERE id = 1")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    ctx.simple_query("SELECT * FROM public.my_table WHERE id = 1")
        .await?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Subquery referencing a non-allowlisted table causes the whole query to be forwarded.
#[tokio::test]
async fn test_allowlist_rejects_subquery_with_non_allowlisted_table() -> Result<(), Error> {
    let mut ctx = TestContext::setup_allowlist("allowed_table").await?;

    ctx.query(
        "CREATE TABLE allowed_table (id int primary key, data text)",
        &[],
    )
    .await?;
    ctx.query("CREATE TABLE blocked_table (id int primary key)", &[])
        .await?;
    ctx.query("INSERT INTO allowed_table VALUES (1, 'hello')", &[])
        .await?;
    ctx.query("INSERT INTO blocked_table VALUES (1)", &[])
        .await?;

    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM allowed_table WHERE id IN (SELECT id FROM blocked_table)")
        .await?;
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&m, &after);
    assert_eq!(
        delta.queries_allowlist_skipped, 1,
        "expected allowlist skip for subquery with non-allowlisted table"
    );

    Ok(())
}

/// No allowlist configured (default) — all tables are cacheable.
#[tokio::test]
async fn test_no_allowlist_allows_all_tables() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE any_table (id int primary key, data text)",
        &[],
    )
    .await?;
    ctx.query("INSERT INTO any_table VALUES (1, 'anything')", &[])
        .await?;

    let m = ctx.metrics().await?;
    ctx.simple_query("SELECT * FROM any_table WHERE id = 1")
        .await?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    ctx.simple_query("SELECT * FROM any_table WHERE id = 1")
        .await?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // Confirm no allowlist skips occurred
    let after = ctx.metrics().await?;
    assert_eq!(
        after.queries_allowlist_skipped, 0,
        "no allowlist skips expected when allowlist is disabled"
    );
    let _ = m;

    Ok(())
}
