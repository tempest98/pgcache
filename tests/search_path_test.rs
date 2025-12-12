#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

use std::io::Error;

use crate::util::{TestContext, assert_row_at, wait_cache_load, wait_for_cdc};

mod util;

/// Test that tables in custom schemas are correctly resolved via search_path.
/// Creates a table in a non-public schema, sets search_path, and verifies
/// unqualified queries resolve to the correct schema.
#[tokio::test]
async fn test_search_path_custom_schema() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Must be done BEFORE creating the schema/table so pgcache picks it up
    ctx.query("SET search_path TO myapp, public", &[]).await?;

    ctx.query("CREATE SCHEMA IF NOT EXISTS myapp", &[]).await?;
    ctx.query(
        "CREATE TABLE myapp.users (id integer primary key, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO myapp.users (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // First query with unqualified table name - should resolve to myapp.users via search_path
    let res = ctx
        .simple_query("SELECT id, name FROM users WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "Alice")])?;

    wait_cache_load().await;

    // Second query - same query, should hit cache
    let res = ctx
        .simple_query("SELECT id, name FROM users WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "Alice")])?;

    let metrics = ctx.metrics()?;
    // One cache hit expected (second query)
    assert_eq!(
        metrics.queries_cache_hit, 1,
        "Expected 1 cache hit, got {}",
        metrics.queries_cache_hit
    );

    Ok(())
}

/// Test that same table name in different schemas resolves correctly based on search_path order.
/// Creates tables with same name in two schemas, verifies search_path priority.
#[tokio::test]
async fn test_search_path_schema_priority() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create two schemas with same table name but different data
    ctx.origin_query("CREATE SCHEMA schema_a", &[]).await?;
    ctx.origin_query("CREATE SCHEMA schema_b", &[]).await?;

    ctx.origin_query(
        "CREATE TABLE schema_a.config (id integer primary key, value text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "CREATE TABLE schema_b.config (id integer primary key, value text)",
        &[],
    )
    .await?;

    ctx.origin_query(
        "INSERT INTO schema_a.config (id, value) VALUES (1, 'from_a')",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO schema_b.config (id, value) VALUES (1, 'from_b')",
        &[],
    )
    .await?;

    // Set search_path with schema_a first
    ctx.query("SET search_path TO schema_a, schema_b", &[])
        .await?;

    wait_for_cdc().await;

    // Query should resolve to schema_a.config (first in search_path)
    let res = ctx
        .simple_query("SELECT id, value FROM config WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_a")])?;

    wait_cache_load().await;

    // Cache hit should return same result
    let res = ctx
        .simple_query("SELECT id, value FROM config WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("value", "from_a")])?;

    let metrics = ctx.metrics()?;
    assert_eq!(metrics.queries_cache_hit, 1);

    Ok(())
}

/// Test that schema-qualified queries work regardless of search_path.
/// Even with search_path set to one schema, explicit qualification accesses other schema.
#[tokio::test]
async fn test_search_path_explicit_schema() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create schema and table
    ctx.origin_query("CREATE SCHEMA hidden", &[]).await?;
    ctx.origin_query(
        "CREATE TABLE hidden.secrets (id integer primary key, data text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO hidden.secrets (id, data) VALUES (1, 'secret_value')",
        &[],
    )
    .await?;

    // Set search_path to only public (hidden not included)
    ctx.query("SET search_path TO public", &[]).await?;

    wait_for_cdc().await;

    // Schema-qualified query should still work
    let res = ctx
        .simple_query("SELECT id, data FROM hidden.secrets WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("data", "secret_value")])?;

    wait_cache_load().await;

    // Cache hit with explicit schema
    let res = ctx
        .simple_query("SELECT id, data FROM hidden.secrets WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("data", "secret_value")])?;

    let metrics = ctx.metrics()?;
    assert_eq!(metrics.queries_cache_hit, 1);

    Ok(())
}

/// Test that the default search_path ("$user", public) works correctly.
/// With default search_path, tables in public schema should be accessible.
#[tokio::test]
async fn test_search_path_default() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create table in public schema (default)
    ctx.origin_query(
        "CREATE TABLE items (id integer primary key, name text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO items (id, name) VALUES (1, 'widget'), (2, 'gadget')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Default search_path should include public
    let res = ctx
        .simple_query("SELECT id, name FROM items WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "widget")])?;

    wait_cache_load().await;

    // Cache hit
    let res = ctx
        .simple_query("SELECT id, name FROM items WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "widget")])?;

    let metrics = ctx.metrics()?;
    assert_eq!(metrics.queries_cache_hit, 1);
    assert_eq!(metrics.queries_cache_miss, 1);

    Ok(())
}

/// Test cache invalidation works correctly with custom schemas.
/// CDC events from tables in non-public schemas should invalidate cache.
#[tokio::test]
async fn test_search_path_cache_invalidation() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create schema and table
    ctx.origin_query("CREATE SCHEMA app", &[]).await?;
    ctx.origin_query(
        "CREATE TABLE app.products (id integer primary key, price integer)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO app.products (id, price) VALUES (1, 100), (2, 200)",
        &[],
    )
    .await?;

    ctx.query("SET search_path TO app, public", &[]).await?;

    wait_for_cdc().await;

    // Initial query
    let res = ctx
        .simple_query("SELECT id, price FROM products WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("price", "100")])?;

    wait_cache_load().await;

    // Update via origin (simulating external change)
    ctx.origin_query("UPDATE app.products SET price = 150 WHERE id = 1", &[])
        .await?;

    wait_for_cdc().await;

    // Query should return updated value (cache invalidated by CDC)
    let res = ctx
        .simple_query("SELECT id, price FROM products WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("price", "150")])?;

    Ok(())
}

/// Test join queries across tables in custom schemas.
#[tokio::test]
async fn test_search_path_join() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create schema with related tables
    ctx.origin_query("CREATE SCHEMA store", &[]).await?;
    ctx.origin_query(
        "CREATE TABLE store.customers (id integer primary key, name text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "CREATE TABLE store.orders (id serial primary key, customer_id integer, total integer)",
        &[],
    )
    .await?;

    ctx.origin_query(
        "INSERT INTO store.customers (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO store.orders (customer_id, total) VALUES (1, 100), (1, 200), (2, 150)",
        &[],
    )
    .await?;

    ctx.query("SET search_path TO store, public", &[]).await?;

    wait_for_cdc().await;

    // Join query with unqualified table names
    let res = ctx
        .simple_query(
            "SELECT c.id, c.name, o.total FROM customers c \
             JOIN orders o ON o.customer_id = c.id \
             WHERE c.id = 1 ORDER BY o.id",
        )
        .await?;

    assert_eq!(res.len(), 4); // 2 rows + CommandComplete + ReadyForQuery
    assert_row_at(&res, 1, &[("id", "1"), ("name", "Alice"), ("total", "100")])?;
    assert_row_at(&res, 2, &[("id", "1"), ("name", "Alice"), ("total", "200")])?;

    wait_cache_load().await;

    // Cache hit
    let res = ctx
        .simple_query(
            "SELECT c.id, c.name, o.total FROM customers c \
             JOIN orders o ON o.customer_id = c.id \
             WHERE c.id = 1 ORDER BY o.id",
        )
        .await?;

    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "Alice"), ("total", "100")])?;

    let metrics = ctx.metrics()?;
    assert_eq!(metrics.queries_cache_hit, 1);

    Ok(())
}
