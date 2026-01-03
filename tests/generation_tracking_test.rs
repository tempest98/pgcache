#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

use std::io::Error;

use crate::util::{TestContext, connect_cache_db, wait_cache_load, wait_for_cdc};

mod util;

/// Test that generation tracking records row access with correct generation numbers
#[tokio::test]
async fn test_generation_tracking_basic() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create test table
    ctx.query("create table test (id integer primary key, data text)", &[])
        .await?;

    ctx.query(
        "insert into test (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    // Wait for CDC to process the inserts before caching
    wait_for_cdc().await;

    // First cached query - should get generation 1
    let _res = ctx
        .simple_query("select id, data from test where id = 1")
        .await?;

    wait_cache_load().await;

    // Execute the cached query again (cache hit) - this records row access
    let _res = ctx
        .simple_query("select id, data from test where id = 1")
        .await?;

    // Connect directly to cache db to check generation tracking
    let cache_db = connect_cache_db(&ctx.dbs).await?;

    // Dump all generation entries to see what was tracked
    let entries = cache_db
        .query(
            "SELECT t.id, t.ctid, d.generation FROM test t \
            JOIN pgcache_generation_dump() d
            ON d.table_oid = 'test'::regclass \
                AND d.pk_hash = pgcache_pk_hash(d.table_oid, t.ctid) \
            WHERE t.id = 1 AND d.generation = 1",
            &[],
        )
        .await
        .map_err(Error::other)?;

    // Should have one entry for the row we accessed
    assert_eq!(
        entries.len(),
        1,
        "Should have generation tracking entries after cache hit"
    );

    // Verify the generation is 1 (first cached query)
    let generation: i64 = entries[0].get("generation");
    assert_eq!(generation, 1, "First cached query should have generation 1");

    Ok(())
}

/// Test that multiple cached queries get incrementing generation numbers
#[tokio::test]
async fn test_generation_tracking_multiple_queries() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create test table
    ctx.query("create table test (id integer primary key, data text)", &[])
        .await?;

    ctx.query(
        "insert into test (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    // Wait for CDC to process the inserts before caching
    wait_for_cdc().await;

    // First cached query - generation 1
    let _res = ctx
        .simple_query("select id, data from test where id = 1")
        .await?;

    wait_cache_load().await;

    // Second cached query (different WHERE) - generation 2
    let _res = ctx
        .simple_query("select id, data from test where id = 2")
        .await?;

    wait_cache_load().await;

    // Execute both cached queries to record row access
    let _res = ctx
        .simple_query("select id, data from test where id = 1")
        .await?;
    let _res = ctx
        .simple_query("select id, data from test where id = 2")
        .await?;

    // Connect directly to cache db
    let cache_db = connect_cache_db(&ctx.dbs).await?;

    // Check generation for row id=1 (should be generation 1)
    let entries_id1 = cache_db
        .query(
            "SELECT t.id, t.ctid, d.generation FROM test t \
            JOIN pgcache_generation_dump() d
            ON d.table_oid = 'test'::regclass \
                AND d.pk_hash = pgcache_pk_hash(d.table_oid, t.ctid) \
            WHERE t.id = 1 AND d.generation = 1",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert_eq!(
        entries_id1.len(),
        1,
        "Should have generation 1 entry for id=1"
    );

    // Check generation for row id=2 (should be generation 2)
    let entries_id2 = cache_db
        .query(
            "SELECT t.id, t.ctid, d.generation FROM test t \
            JOIN pgcache_generation_dump() d
            ON d.table_oid = 'test'::regclass \
                AND d.pk_hash = pgcache_pk_hash(d.table_oid, t.ctid) \
            WHERE t.id = 2 AND d.generation = 2",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert_eq!(
        entries_id2.len(),
        1,
        "Should have generation 2 entry for id=2"
    );

    Ok(())
}

/// Test that generation purge removes entries correctly
#[tokio::test]
async fn test_generation_purge() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create test table
    ctx.query("create table test (id integer primary key, data text)", &[])
        .await?;

    ctx.query(
        "insert into test (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    // Wait for CDC to process the inserts before caching
    wait_for_cdc().await;

    // Create two cached queries
    let _res = ctx
        .simple_query("select id, data from test where id = 1")
        .await?;

    wait_cache_load().await;

    let _res = ctx
        .simple_query("select id, data from test where id = 2")
        .await?;

    wait_cache_load().await;

    // Execute both to record row access
    let _res = ctx
        .simple_query("select id, data from test where id = 1")
        .await?;
    let _res = ctx
        .simple_query("select id, data from test where id = 2")
        .await?;

    // Connect directly to cache db
    let cache_db = connect_cache_db(&ctx.dbs).await?;

    // Check entries before purge - row id=1 should have generation 1
    let entries_id1_before = cache_db
        .query(
            "SELECT t.id, t.ctid, d.generation FROM test t \
            JOIN pgcache_generation_dump() d
            ON d.table_oid = 'test'::regclass \
                AND d.pk_hash = pgcache_pk_hash(d.table_oid, t.ctid) \
            WHERE t.id = 1 AND d.generation = 1",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert_eq!(
        entries_id1_before.len(),
        1,
        "Should have generation 1 entry for id=1 before purge"
    );

    // Row id=2 should have generation 2
    let entries_id2_before = cache_db
        .query(
            "SELECT t.id, t.ctid, d.generation FROM test t \
            JOIN pgcache_generation_dump() d
            ON d.table_oid = 'test'::regclass \
                AND d.pk_hash = pgcache_pk_hash(d.table_oid, t.ctid) \
            WHERE t.id = 2 AND d.generation = 2",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert_eq!(
        entries_id2_before.len(),
        1,
        "Should have generation 2 entry for id=2 before purge"
    );

    // Purge generation 1 and below
    let purged: i64 = cache_db
        .query_one("SELECT pgcache_generation_purge_all(1)", &[])
        .await
        .map_err(Error::other)?
        .get(0);

    assert_eq!(purged, 1, "Should have purged exactly 1 entry");

    // Check entries after purge - row id=1 should have no generation 1 entry
    let entries_id1_after = cache_db
        .query(
            "SELECT t.id, t.ctid, d.generation FROM test t \
            JOIN pgcache_generation_dump() d
            ON d.table_oid = 'test'::regclass \
                AND d.pk_hash = pgcache_pk_hash(d.table_oid, t.ctid) \
            WHERE t.id = 1 AND d.generation = 1",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert!(
        entries_id1_after.is_empty(),
        "Generation 1 entry for id=1 should be purged"
    );

    // Row id=2 should still have generation 2
    let entries_id2_after = cache_db
        .query(
            "SELECT t.id, t.ctid, d.generation FROM test t \
            JOIN pgcache_generation_dump() d
            ON d.table_oid = 'test'::regclass \
                AND d.pk_hash = pgcache_pk_hash(d.table_oid, t.ctid) \
            WHERE t.id = 2 AND d.generation = 2",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert_eq!(
        entries_id2_after.len(),
        1,
        "Generation 2 entry for id=2 should remain after purge"
    );

    Ok(())
}

/// Test that generation tracking works with JOIN queries
#[tokio::test]
async fn test_generation_tracking_join() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create test tables
    ctx.query("create table test (id integer primary key, data text)", &[])
        .await?;

    ctx.query(
        "create table test_map (id serial primary key, test_id integer, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test (id, data) values (1, 'foo'), (2, 'bar')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_map (test_id, data) values (1, 'alpha'), (1, 'beta'), (2, 'gamma')",
        &[],
    )
    .await?;

    // Wait for CDC to process the inserts before caching
    wait_for_cdc().await;

    // Cached JOIN query
    let _res = ctx
        .simple_query(
            "select t.id, tm.data from test t join test_map tm on tm.test_id = t.id where t.id = 1",
        )
        .await?;

    wait_cache_load().await;

    // Execute to record row access
    let _res = ctx
        .simple_query(
            "select t.id, tm.data from test t join test_map tm on tm.test_id = t.id where t.id = 1",
        )
        .await?;

    // Connect directly to cache db
    let cache_db = connect_cache_db(&ctx.dbs).await?;

    // Check generation entry for test table row id=1
    let entries_test = cache_db
        .query(
            "SELECT t.id, t.ctid, d.generation FROM test t \
            JOIN pgcache_generation_dump() d
            ON d.table_oid = 'test'::regclass \
                AND d.pk_hash = pgcache_pk_hash(d.table_oid, t.ctid) \
            WHERE t.id = 1 AND d.generation = 1",
            &[],
        )
        .await
        .map_err(Error::other)?;

    assert_eq!(
        entries_test.len(),
        1,
        "Should have generation 1 entry for test.id=1"
    );

    // Check generation entries for test_map table rows with test_id=1
    let entries_test_map = cache_db
        .query(
            "SELECT tm.id, tm.ctid, d.generation FROM test_map tm \
            JOIN pgcache_generation_dump() d
            ON d.table_oid = 'test_map'::regclass \
                AND d.pk_hash = pgcache_pk_hash(d.table_oid, tm.ctid) \
            WHERE tm.test_id = 1 AND d.generation = 1",
            &[],
        )
        .await
        .map_err(Error::other)?;

    // Should have entries for both test_map rows with test_id=1 (alpha, beta)
    assert_eq!(
        entries_test_map.len(),
        2,
        "Should have generation 1 entries for both test_map rows with test_id=1"
    );

    Ok(())
}
