#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

use std::io::Error;

use crate::util::{TestContext, assert_row_at, metrics_delta, wait_cache_load, wait_for_cdc};

mod util;

/// Consolidated test for search_path resolution functionality.
/// Combines 6 individual tests into one to reduce setup overhead.
#[tokio::test]
async fn test_search_path() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    search_path_custom_schema(&mut ctx).await?;
    search_path_schema_priority(&mut ctx).await?;
    search_path_explicit_schema(&mut ctx).await?;
    search_path_default(&mut ctx).await?;
    search_path_cache_invalidation(&mut ctx).await?;
    search_path_join(&mut ctx).await?;

    Ok(())
}

/// Test that tables in custom schemas are correctly resolved via search_path.
/// Creates a table in a non-public schema, sets search_path, and verifies
/// unqualified queries resolve to the correct schema.
async fn search_path_custom_schema(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    // Must be done BEFORE creating the schema/table so pgcache picks it up
    ctx.query("SET search_path TO myapp_custom, public", &[])
        .await?;

    ctx.query("CREATE SCHEMA IF NOT EXISTS myapp_custom", &[])
        .await?;
    ctx.query(
        "CREATE TABLE myapp_custom.users (id integer primary key, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO myapp_custom.users (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
        &[],
    )
    .await?;

    // First query with unqualified table name - should resolve to myapp_custom.users via search_path
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

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(
        delta.queries_cache_hit, 1,
        "Expected 1 cache hit, got {}",
        delta.queries_cache_hit
    );

    Ok(())
}

/// Test that same table name in different schemas resolves correctly based on search_path order.
/// Creates tables with same name in two schemas, verifies search_path priority.
async fn search_path_schema_priority(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    // Create two schemas with same table name but different data
    ctx.origin_query("CREATE SCHEMA schema_a_priority", &[])
        .await?;
    ctx.origin_query("CREATE SCHEMA schema_b_priority", &[])
        .await?;

    ctx.origin_query(
        "CREATE TABLE schema_a_priority.config (id integer primary key, value text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "CREATE TABLE schema_b_priority.config (id integer primary key, value text)",
        &[],
    )
    .await?;

    ctx.origin_query(
        "INSERT INTO schema_a_priority.config (id, value) VALUES (1, 'from_a')",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO schema_b_priority.config (id, value) VALUES (1, 'from_b')",
        &[],
    )
    .await?;

    // Set search_path with schema_a_priority first
    ctx.query(
        "SET search_path TO schema_a_priority, schema_b_priority",
        &[],
    )
    .await?;

    // Query should resolve to schema_a_priority.config (first in search_path)
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

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.queries_cache_hit, 1);

    Ok(())
}

/// Test that schema-qualified queries work regardless of search_path.
/// Even with search_path set to one schema, explicit qualification accesses other schema.
async fn search_path_explicit_schema(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    // Create schema and table
    ctx.origin_query("CREATE SCHEMA hidden_explicit", &[])
        .await?;
    ctx.origin_query(
        "CREATE TABLE hidden_explicit.secrets (id integer primary key, data text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO hidden_explicit.secrets (id, data) VALUES (1, 'secret_value')",
        &[],
    )
    .await?;

    // Set search_path to only public (hidden_explicit not included)
    ctx.query("SET search_path TO public", &[]).await?;

    // Schema-qualified query should still work
    let res = ctx
        .simple_query("SELECT id, data FROM hidden_explicit.secrets WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("data", "secret_value")])?;

    wait_cache_load().await;

    // Cache hit with explicit schema
    let res = ctx
        .simple_query("SELECT id, data FROM hidden_explicit.secrets WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("data", "secret_value")])?;

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.queries_cache_hit, 1);

    Ok(())
}

/// Test that the default search_path ("$user", public) works correctly.
/// With default search_path, tables in public schema should be accessible.
async fn search_path_default(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    // Create table in public schema (default)
    ctx.origin_query(
        "CREATE TABLE items_default (id integer primary key, name text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO items_default (id, name) VALUES (1, 'widget'), (2, 'gadget')",
        &[],
    )
    .await?;

    // Default search_path should include public
    let res = ctx
        .simple_query("SELECT id, name FROM items_default WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "widget")])?;

    wait_cache_load().await;

    // Cache hit
    let res = ctx
        .simple_query("SELECT id, name FROM items_default WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("name", "widget")])?;

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.queries_cache_hit, 1);
    assert_eq!(delta.queries_cache_miss, 1);

    Ok(())
}

/// Test cache invalidation works correctly with custom schemas.
/// CDC events from tables in non-public schemas should invalidate cache.
async fn search_path_cache_invalidation(ctx: &mut TestContext) -> Result<(), Error> {
    // Create schema and table
    ctx.origin_query("CREATE SCHEMA app_invalidation", &[])
        .await?;
    ctx.origin_query(
        "CREATE TABLE app_invalidation.products (id integer primary key, price integer)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO app_invalidation.products (id, price) VALUES (1, 100), (2, 200)",
        &[],
    )
    .await?;

    ctx.query("SET search_path TO app_invalidation, public", &[])
        .await?;

    // Initial query
    let res = ctx
        .simple_query("SELECT id, price FROM products WHERE id = 1")
        .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("price", "100")])?;

    wait_cache_load().await;

    // Update via origin (simulating external change)
    ctx.origin_query(
        "UPDATE app_invalidation.products SET price = 150 WHERE id = 1",
        &[],
    )
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
async fn search_path_join(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    // Create schema with related tables
    ctx.origin_query("CREATE SCHEMA store_join", &[]).await?;
    ctx.origin_query(
        "CREATE TABLE store_join.customers (id integer primary key, name text)",
        &[],
    )
    .await?;
    ctx.origin_query(
        "CREATE TABLE store_join.orders (id serial primary key, customer_id integer, total integer)",
        &[],
    )
    .await?;

    ctx.origin_query(
        "INSERT INTO store_join.customers (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
        &[],
    )
    .await?;
    ctx.origin_query(
        "INSERT INTO store_join.orders (customer_id, total) VALUES (1, 100), (1, 200), (2, 150)",
        &[],
    )
    .await?;

    ctx.query("SET search_path TO store_join, public", &[])
        .await?;

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

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.queries_cache_hit, 1);

    Ok(())
}
