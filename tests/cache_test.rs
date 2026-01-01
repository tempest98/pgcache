#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

use std::io::Error;

use crate::util::{TestContext, assert_row_at, connect_cache_db, wait_cache_load, wait_for_cdc};

mod util;

#[tokio::test]
async fn test_cache_simple() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query("create table test (id integer primary key, data text)", &[])
        .await?;

    ctx.query(
        "insert into test (id, data) values (1, 'foo'), (2, 'bar')",
        &[],
    )
    .await?;

    let res = ctx
        .simple_query("select id, data from test where data = 'foo'")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("data", "foo")])?;

    wait_cache_load().await;

    let res = ctx
        .simple_query("select id, data from test where data = 'foo'")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("data", "foo")])?;

    ctx.origin_query(
        "insert into test (id, data) values (3, 'foo'), (4, 'bar')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let res = ctx
        .simple_query("select id, data from test where data = 'foo'")
        .await?;

    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1"), ("data", "foo")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("data", "foo")])?;

    let metrics = ctx.metrics()?;
    assert_eq!(metrics.queries_total, 5);
    assert_eq!(metrics.queries_cacheable, 3);
    assert_eq!(metrics.queries_uncacheable, 2);
    assert_eq!(metrics.queries_unsupported, 2);
    assert_eq!(metrics.queries_cache_hit, 2);
    assert_eq!(metrics.queries_cache_miss, 1);

    Ok(())
}

#[tokio::test]
async fn test_cache_join() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

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
        "insert into test_map (test_id, data) values \
        (1, 'foo'), \
        (1, 'bar'), \
        (1, 'baz'), \
        (2, 'foo'), \
        (2, 'bar'), \
        (2, 'baz')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let query_str = "select t.id, t.data as test_data, tm.test_id, tm.data as map_data \
        from test t join test_map tm on tm.test_id = t.id where t.id = 1 \
        order by tm.id;";

    // First query to populate cache
    let _ = ctx.simple_query(query_str).await?;

    wait_cache_load().await;

    // Second query should hit cache
    let res = ctx.simple_query(query_str).await?;

    assert_eq!(res.len(), 5);
    assert_row_at(
        &res,
        1,
        &[
            ("id", "1"),
            ("test_data", "foo"),
            ("test_id", "1"),
            ("map_data", "foo"),
        ],
    )?;
    assert_row_at(
        &res,
        2,
        &[
            ("id", "1"),
            ("test_data", "foo"),
            ("test_id", "1"),
            ("map_data", "bar"),
        ],
    )?;
    assert_row_at(
        &res,
        3,
        &[
            ("id", "1"),
            ("test_data", "foo"),
            ("test_id", "1"),
            ("map_data", "baz"),
        ],
    )?;

    // Trigger CDC events by modifying the test table
    ctx.origin_query("update test set id = 10 where id = 1", &[])
        .await?;

    ctx.origin_query("update test set id = 1 where id = 10", &[])
        .await?;

    wait_for_cdc().await;

    // Query after CDC should still return correct results
    let res = ctx.simple_query(query_str).await?;
    dbg!(&res);
    assert_eq!(res.len(), 5);
    assert_row_at(
        &res,
        1,
        &[
            ("id", "1"),
            ("test_data", "foo"),
            ("test_id", "1"),
            ("map_data", "foo"),
        ],
    )?;
    assert_row_at(
        &res,
        2,
        &[
            ("id", "1"),
            ("test_data", "foo"),
            ("test_id", "1"),
            ("map_data", "bar"),
        ],
    )?;
    assert_row_at(
        &res,
        3,
        &[
            ("id", "1"),
            ("test_data", "foo"),
            ("test_id", "1"),
            ("map_data", "baz"),
        ],
    )?;

    let metrics = ctx.metrics()?;
    assert_eq!(metrics.queries_total, 7);
    assert_eq!(metrics.queries_cacheable, 3);
    assert_eq!(metrics.queries_uncacheable, 4);
    assert_eq!(metrics.queries_unsupported, 4);
    assert_eq!(metrics.queries_invalid, 0);
    assert_eq!(metrics.queries_cache_hit, 1, "cache hits");
    assert_eq!(metrics.queries_cache_miss, 2, "cache misses");

    Ok(())
}

/// Test that indexes from the origin table are created on the cache table
#[tokio::test]
async fn test_cache_index_creation() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create table with multiple indexes on origin
    ctx.query(
        "CREATE TABLE test_indexed (
            id INTEGER PRIMARY KEY,
            email TEXT,
            name TEXT,
            created_at TIMESTAMP
        )",
        &[],
    )
    .await?;

    // Create various index types
    ctx.query("CREATE INDEX idx_email ON test_indexed (email)", &[])
        .await?;
    ctx.query("CREATE UNIQUE INDEX idx_name ON test_indexed (name)", &[])
        .await?;
    ctx.query(
        "CREATE INDEX idx_composite ON test_indexed (email, created_at)",
        &[],
    )
    .await?;
    ctx.query(
        "CREATE INDEX idx_email_hash ON test_indexed USING hash (email)",
        &[],
    )
    .await?;

    // Insert some data
    ctx.query(
        "INSERT INTO test_indexed (id, email, name, created_at) VALUES
         (1, 'alice@example.com', 'Alice', '2024-01-01'),
         (2, 'bob@example.com', 'Bob', '2024-01-02')",
        &[],
    )
    .await?;

    // Execute a cacheable query to trigger cache table creation
    let _ = ctx
        .simple_query("SELECT * FROM test_indexed WHERE id = 1")
        .await?;

    wait_cache_load().await;

    // Connect directly to the cache database to verify indexes
    let cache_db = connect_cache_db(&ctx.dbs).await?;

    // Query indexes using similar approach to query_table_indexes_get
    let rows = cache_db
        .query(
            r"
            SELECT
                i.relname AS index_name,
                ix.indisunique AS is_unique,
                am.amname AS method,
                array_agg(a.attname ORDER BY array_position(ix.indkey::int[], a.attnum::int)) AS columns,
                ix.indisprimary AS is_primary
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_am am ON am.oid = i.relam
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE t.relname = 'test_indexed'
            GROUP BY i.relname, ix.indisunique, am.amname, ix.indisprimary, ix.indkey
            ORDER BY i.relname
            ",
            &[],
        )
        .await
        .map_err(Error::other)?;

    // Build a list of (is_unique, method, columns, is_primary) for verification
    let indexes: Vec<(bool, String, Vec<String>, bool)> = rows
        .iter()
        .map(|r| {
            (
                r.get("is_unique"),
                r.get("method"),
                r.get("columns"),
                r.get("is_primary"),
            )
        })
        .collect();

    // Should have 5 indexes: 1 primary key + 4 non-pk indexes
    assert_eq!(
        indexes.len(),
        5,
        "Expected 5 indexes, found {}",
        indexes.len()
    );

    // Verify primary key index exists
    let pk_indexes: Vec<_> = indexes.iter().filter(|i| i.3).collect();
    assert_eq!(pk_indexes.len(), 1, "Expected exactly 1 primary key index");
    assert_eq!(
        pk_indexes[0].2,
        vec!["id"],
        "Primary key should be on 'id' column"
    );

    // Verify non-pk indexes
    let non_pk_indexes: Vec<_> = indexes.iter().filter(|i| !i.3).collect();
    assert_eq!(
        non_pk_indexes.len(),
        4,
        "Expected 4 non-primary-key indexes"
    );

    // Check for unique btree index on (name)
    let unique_name_idx = non_pk_indexes
        .iter()
        .find(|i| i.0 && i.1 == "btree" && i.2 == vec!["name"]);
    assert!(
        unique_name_idx.is_some(),
        "Missing unique btree index on (name)"
    );

    // Check for btree index on (email)
    let email_btree_idx = non_pk_indexes
        .iter()
        .find(|i| !i.0 && i.1 == "btree" && i.2 == vec!["email"]);
    assert!(email_btree_idx.is_some(), "Missing btree index on (email)");

    // Check for composite btree index on (email, created_at)
    let composite_idx = non_pk_indexes
        .iter()
        .find(|i| i.1 == "btree" && i.2 == vec!["email", "created_at"]);
    assert!(
        composite_idx.is_some(),
        "Missing composite btree index on (email, created_at)"
    );

    // Check for hash index on (email)
    let hash_idx = non_pk_indexes
        .iter()
        .find(|i| i.1 == "hash" && i.2 == vec!["email"]);
    assert!(hash_idx.is_some(), "Missing hash index on (email)");

    Ok(())
}
