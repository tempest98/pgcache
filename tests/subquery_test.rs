#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

//! Subquery caching tests.
//!
//! Each test is independent with its own TestContext, database, and proxy instance.
//!
//! Subquery types covered:
//! - Derived tables (FROM subqueries) with CDC invalidation
//! - WHERE IN subqueries with CDC invalidation
//! - WHERE NOT IN subqueries (Exclusion semantics)
//! - Scalar subqueries in WHERE clause
//! - Nested subqueries (multiple levels)
//! - Multi-table subquery dependencies with CDC
//!
//! Not yet supported (correlated subqueries):
//! - WHERE EXISTS / NOT EXISTS with outer table references
//! - Scalar subqueries in SELECT list with outer table references
//! - LATERAL subqueries
//! - Correlated scalar subqueries in WHERE clause

use std::io::Error;

use crate::util::{TestContext, assert_row_at, metrics_delta, wait_cache_load, wait_for_cdc};

mod util;

// =============================================================================
// Derived Table (FROM subquery)
// =============================================================================

/// Test derived table (subquery in FROM clause) caching and CDC invalidation.
///
/// Pattern: SELECT ... FROM (SELECT ... FROM table WHERE ...) AS alias
///
/// CDC behavior for derived tables (Inclusion semantics):
/// - INSERT into subquery table → invalidates (set grows, outer result may grow)
/// - DELETE from subquery table → does NOT invalidate (set shrinks, cache tables updated in place)
/// - UPDATE on subquery table → invalidates (could go either way)
#[tokio::test]
async fn test_subquery_from_derived_table() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER, category TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO products (id, name, price, category) VALUES \
         (1, 'Widget', 100, 'gadgets'), \
         (2, 'Gizmo', 200, 'gadgets'), \
         (3, 'Thing', 50, 'tools'), \
         (4, 'Doodad', 150, 'gadgets')",
        &[],
    )
    .await?;

    // Wait for setup CDC events to be processed before caching —
    // INSERT events on subquery tables would trigger invalidation
    wait_for_cdc().await;

    let before = ctx.metrics().await?;

    let query = "SELECT name, price FROM (SELECT * FROM products WHERE category = 'gadgets') AS gadget_products ORDER BY price";

    // First query — cache miss, populates cache
    let res = ctx.simple_query(query).await?;

    assert_eq!(res.len(), 5); // 3 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("name", "Widget"), ("price", "100")])?;
    assert_row_at(&res, 2, &[("name", "Doodad"), ("price", "150")])?;
    assert_row_at(&res, 3, &[("name", "Gizmo"), ("price", "200")])?;

    wait_cache_load().await;

    // Second query — cache hit
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5);
    assert_row_at(&res, 1, &[("name", "Widget"), ("price", "100")])?;

    // --- CDC INSERT: derived table (Inclusion) → invalidates ---

    ctx.origin_query(
        "INSERT INTO products (id, name, price, category) VALUES (5, 'NewGadget', 75, 'gadgets')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // CDC INSERT on derived table (Inclusion) invalidates → cache miss
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 6); // 4 rows now
    assert_row_at(&res, 1, &[("name", "NewGadget"), ("price", "75")])?;
    assert_row_at(&res, 2, &[("name", "Widget"), ("price", "100")])?;

    wait_cache_load().await;

    // --- CDC DELETE: derived table (Inclusion) → does NOT invalidate ---

    ctx.origin_query("DELETE FROM products WHERE id = 2", &[])
        .await?;

    wait_for_cdc().await;

    // CDC DELETE on derived table (Inclusion) does NOT invalidate → cache hit
    // Row removed from cache table, query re-evaluates correctly
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5); // back to 3 rows (Gizmo gone)

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    assert_eq!(delta.queries_cacheable, 4, "cacheable queries");
    // miss, hit, miss (CDC INSERT invalidated), hit (CDC DELETE doesn't invalidate)
    assert_eq!(delta.queries_cache_miss, 2, "cache misses");
    assert_eq!(delta.queries_cache_hit, 2, "cache hits");

    Ok(())
}

// =============================================================================
// WHERE IN Subquery
// =============================================================================

/// Test WHERE IN subquery caching and CDC invalidation.
///
/// Pattern: SELECT ... FROM table WHERE column IN (SELECT column FROM other_table ...)
///
/// CDC behavior for IN subquery tables (Inclusion semantics):
/// - INSERT into subquery table → invalidates (IN set grows, outer result may grow)
/// - DELETE from subquery table → does NOT invalidate (IN set shrinks, cache updated in place)
#[tokio::test]
async fn test_subquery_where_in() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, tier TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO customers (id, name, tier) VALUES \
         (1, 'Alice', 'gold'), (2, 'Bob', 'silver'), (3, 'Charlie', 'gold')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO orders (id, customer_id, amount) VALUES \
         (10, 1, 500), (11, 1, 300), (12, 2, 100)",
        &[],
    )
    .await?;

    // Wait for setup CDC events to be processed before caching
    wait_for_cdc().await;

    let before = ctx.metrics().await?;

    // Query: customers who have placed orders
    let query =
        "SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders) ORDER BY name";

    // Cache miss
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // Alice, Bob
    assert_row_at(&res, 1, &[("name", "Alice")])?;
    assert_row_at(&res, 2, &[("name", "Bob")])?;

    wait_cache_load().await;

    // Cache hit
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);

    // --- CDC INSERT: IN subquery table (Inclusion) → invalidates ---

    ctx.origin_query(
        "INSERT INTO orders (id, customer_id, amount) VALUES (13, 3, 250)",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // CDC INSERT on IN subquery table (Inclusion) invalidates → cache miss
    // Charlie should now appear
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5); // Alice, Bob, Charlie
    assert_row_at(&res, 1, &[("name", "Alice")])?;
    assert_row_at(&res, 2, &[("name", "Bob")])?;
    assert_row_at(&res, 3, &[("name", "Charlie")])?;

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    assert_eq!(delta.queries_cacheable, 3, "cacheable IN subqueries");
    // miss, hit, miss (CDC INSERT invalidated)
    assert_eq!(delta.queries_cache_miss, 2, "cache misses");
    assert_eq!(delta.queries_cache_hit, 1, "cache hits");

    Ok(())
}

// =============================================================================
// WHERE NOT IN Subquery
// =============================================================================

/// Test WHERE NOT IN subquery caching.
///
/// Pattern: SELECT ... FROM table WHERE column NOT IN (SELECT column FROM other_table ...)
///
/// NOT IN has Exclusion semantics — INSERT into the subquery table does NOT
/// invalidate (the exclusion set grows, so the outer result can only shrink,
/// and the cache tables already reflect this).
#[tokio::test]
async fn test_subquery_where_not_in() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER, category TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO products (id, name, price, category) VALUES \
         (1, 'Widget', 100, 'gadgets'), \
         (2, 'Gizmo', 200, 'gadgets'), \
         (3, 'Thing', 50, 'tools'), \
         (4, 'Doodad', 150, 'gadgets')",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE sale_items (id INTEGER PRIMARY KEY, product_id INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO sale_items (id, product_id) VALUES (1, 1), (2, 3)",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let before = ctx.metrics().await?;

    // Query: products NOT on sale
    let query = "SELECT name FROM products WHERE id NOT IN (SELECT product_id FROM sale_items) ORDER BY name";

    let res = ctx.simple_query(query).await?;

    // Products 1 (Widget) and 3 (Thing) are on sale; remaining: Doodad, Gizmo
    assert_eq!(res.len(), 4); // 2 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("name", "Doodad")])?;
    assert_row_at(&res, 2, &[("name", "Gizmo")])?;

    wait_cache_load().await;

    let res_cached = ctx.simple_query(query).await?;
    assert_eq!(res.len(), res_cached.len(), "cache returns same results");

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    assert_eq!(delta.queries_cacheable, 2, "cacheable NOT IN subqueries");
    assert_eq!(delta.queries_cache_miss, 1, "cache misses");
    assert_eq!(delta.queries_cache_hit, 1, "cache hits");

    Ok(())
}

// =============================================================================
// Scalar Subquery in WHERE
// =============================================================================

/// Test scalar subquery in WHERE clause caching.
///
/// Pattern: SELECT ... FROM table WHERE column > (SELECT aggregate(...) FROM ...)
///
/// This scalar subquery is non-correlated (doesn't reference the outer table),
/// so it can be cached.
#[tokio::test]
async fn test_subquery_scalar_in_where() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER, category TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO products (id, name, price, category) VALUES \
         (1, 'Widget', 100, 'gadgets'), \
         (2, 'Gizmo', 200, 'gadgets'), \
         (3, 'Thing', 50, 'tools'), \
         (4, 'Doodad', 150, 'gadgets')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let before = ctx.metrics().await?;

    // Query: products with price above average
    // Average = (100 + 200 + 50 + 150) / 4 = 125
    let query = "SELECT name, price FROM products WHERE price > (SELECT AVG(price) FROM products) ORDER BY price";

    let res = ctx.simple_query(query).await?;

    // Doodad(150) and Gizmo(200) are above average
    assert_eq!(res.len(), 4); // 2 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("name", "Doodad"), ("price", "150")])?;
    assert_row_at(&res, 2, &[("name", "Gizmo"), ("price", "200")])?;

    wait_cache_load().await;

    let res_cached = ctx.simple_query(query).await?;
    assert_eq!(res.len(), res_cached.len(), "cache returns same results");

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    assert_eq!(
        delta.queries_cacheable, 2,
        "cacheable scalar WHERE subqueries"
    );
    assert_eq!(delta.queries_cache_miss, 1, "cache misses");
    assert_eq!(delta.queries_cache_hit, 1, "cache hits");

    Ok(())
}

// =============================================================================
// Nested Subqueries
// =============================================================================

/// Test nested subqueries caching.
///
/// Pattern: SELECT ... WHERE col IN (SELECT ... WHERE col IN (SELECT ...))
///
/// Subqueries can be nested multiple levels deep. The cache system tracks
/// table dependencies through all nesting levels.
#[tokio::test]
async fn test_subquery_nested() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE stores (id INTEGER PRIMARY KEY, region_id INTEGER, name TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, store_id INTEGER, name TEXT, price INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO regions (id, name) VALUES (1, 'East'), (2, 'West')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO stores (id, region_id, name) VALUES \
         (10, 1, 'NYC'), (11, 1, 'Boston'), (12, 2, 'LA')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO products (id, store_id, name, price) VALUES \
         (100, 10, 'Widget', 25), (101, 11, 'Gadget', 50), \
         (102, 12, 'Gizmo', 30), (103, 10, 'Doohickey', 15)",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let before = ctx.metrics().await?;

    // Nested IN subqueries across 3 tables:
    // find products in stores that belong to the 'East' region
    let query = "SELECT name FROM products \
                 WHERE store_id IN ( \
                     SELECT id FROM stores \
                     WHERE region_id IN ( \
                         SELECT id FROM regions WHERE name = 'East' \
                     ) \
                 ) \
                 ORDER BY name";

    let res = ctx.simple_query(query).await?;

    // East region (id=1) → stores NYC(10), Boston(11) → products Widget, Gadget, Doohickey
    assert_eq!(res.len(), 5); // 3 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("name", "Doohickey")])?;
    assert_row_at(&res, 2, &[("name", "Gadget")])?;
    assert_row_at(&res, 3, &[("name", "Widget")])?;

    wait_cache_load().await;

    let res_cached = ctx.simple_query(query).await?;
    assert_eq!(res.len(), res_cached.len(), "cache returns same results");

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    assert_eq!(delta.queries_cacheable, 2, "cacheable nested subqueries");
    assert_eq!(delta.queries_cache_miss, 1, "cache misses");
    assert_eq!(delta.queries_cache_hit, 1, "cache hits");

    Ok(())
}

// =============================================================================
// Double-Nested Subquery CDC Tests
// =============================================================================

/// Test double-nested IN subqueries with CDC invalidation at each level.
///
/// Pattern: SELECT ... WHERE col IN (SELECT ... WHERE col IN (SELECT ...))
///
/// All three subquery levels are Inclusion:
/// - Outer: products (Direct)
/// - Middle: stores (Subquery/Inclusion)
/// - Inner: regions (Subquery/Inclusion)
///
/// CDC INSERT at any level should invalidate (Inclusion = growth invalidates).
/// CDC DELETE at inner levels should NOT invalidate (Inclusion = shrinkage safe).
#[tokio::test]
async fn test_subquery_nested_in_in_cdc() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE stores (id INTEGER PRIMARY KEY, region_id INTEGER, name TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, store_id INTEGER, name TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO regions (id, name) VALUES (1, 'East'), (2, 'West')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO stores (id, region_id, name) VALUES \
         (10, 1, 'NYC'), (11, 1, 'Boston'), (12, 2, 'LA')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO products (id, store_id, name) VALUES \
         (100, 10, 'Widget'), (101, 11, 'Gadget'), \
         (102, 12, 'Gizmo'), (103, 10, 'Doohickey')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let before = ctx.metrics().await?;

    // products in stores in East region
    let query = "SELECT name FROM products \
                 WHERE store_id IN ( \
                     SELECT id FROM stores \
                     WHERE region_id IN ( \
                         SELECT id FROM regions WHERE name = 'East' \
                     ) \
                 ) \
                 ORDER BY name";

    // --- Initial cache miss ---
    let res = ctx.simple_query(query).await?;
    // East(1) → NYC(10), Boston(11) → Widget, Gadget, Doohickey
    assert_eq!(res.len(), 5);
    assert_row_at(&res, 1, &[("name", "Doohickey")])?;
    assert_row_at(&res, 2, &[("name", "Gadget")])?;
    assert_row_at(&res, 3, &[("name", "Widget")])?;

    wait_cache_load().await;

    // --- Cache hit ---
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5);

    // --- CDC INSERT at innermost level (regions): Inclusion → invalidate ---
    // Add a new store in the East region.

    // --- CDC INSERT at middle level (stores): Inclusion → invalidate ---
    ctx.origin_query(
        "INSERT INTO stores (id, region_id, name) VALUES (13, 1, 'Philly')",
        &[],
    )
    .await?;

    ctx.origin_query(
        "INSERT INTO products (id, store_id, name) VALUES (104, 13, 'Thingamajig')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Stores insert (Inclusion) should invalidate
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 6); // Doohickey, Gadget, Thingamajig, Widget
    assert_row_at(&res, 1, &[("name", "Doohickey")])?;
    assert_row_at(&res, 2, &[("name", "Gadget")])?;
    assert_row_at(&res, 3, &[("name", "Thingamajig")])?;
    assert_row_at(&res, 4, &[("name", "Widget")])?;

    wait_cache_load().await;

    // --- CDC DELETE at middle level (stores): Inclusion → no invalidation ---
    // Remove Philly store — shrinkage is safe for Inclusion
    ctx.origin_query("DELETE FROM stores WHERE id = 13", &[])
        .await?;

    wait_for_cdc().await;

    // Cache hit — Inclusion DELETE does not invalidate.
    // Thingamajig's store is gone, so Thingamajig won't be included in results.
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5); // back to Doohickey, Gadget, Widget
    assert_row_at(&res, 1, &[("name", "Doohickey")])?;
    assert_row_at(&res, 2, &[("name", "Gadget")])?;
    assert_row_at(&res, 3, &[("name", "Widget")])?;

    // --- CDC INSERT at innermost level (regions): Inclusion → invalidate ---
    // Update an existing region to become 'East'
    ctx.origin_query("UPDATE regions SET name = 'East' WHERE id = 2", &[])
        .await?;

    wait_for_cdc().await;

    // Now both regions are 'East', so LA(12) is included → Gizmo appears
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 6); // Doohickey, Gadget, Gizmo, Widget
    assert_row_at(&res, 1, &[("name", "Doohickey")])?;
    assert_row_at(&res, 2, &[("name", "Gadget")])?;
    assert_row_at(&res, 3, &[("name", "Gizmo")])?;
    assert_row_at(&res, 4, &[("name", "Widget")])?;

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    assert_eq!(delta.queries_cacheable, 5, "cacheable queries");
    // miss, hit, miss (stores INSERT), hit (stores DELETE), miss (regions UPDATE)
    assert_eq!(delta.queries_cache_miss, 3, "cache misses");
    assert_eq!(delta.queries_cache_hit, 2, "cache hits");

    Ok(())
}

/// Test NOT IN nested inside IN with CDC invalidation.
///
/// Pattern: SELECT ... WHERE col IN (SELECT ... WHERE col NOT IN (SELECT ...))
///
/// Source types:
/// - Outer: products (Direct)
/// - Middle: stores via IN (Subquery/Inclusion)
/// - Inner: excluded_regions via NOT IN (Subquery/Exclusion)
///
/// The inner NOT IN has Exclusion semantics:
/// - INSERT into excluded_regions → does NOT invalidate (exclusion set grows, result can only shrink)
/// - DELETE from excluded_regions → invalidates (exclusion set shrinks, result may grow)
#[tokio::test]
async fn test_subquery_nested_not_in_inside_in() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE excluded_regions (id INTEGER PRIMARY KEY, region_id INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE stores (id INTEGER PRIMARY KEY, region_id INTEGER, name TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, store_id INTEGER, name TEXT)",
        &[],
    )
    .await?;

    // Regions: East(1), West(2), South(3)
    ctx.query(
        "INSERT INTO regions (id, name) VALUES (1, 'East'), (2, 'West'), (3, 'South')",
        &[],
    )
    .await?;

    // Exclude West(2) — only East and South are non-excluded
    ctx.query(
        "INSERT INTO excluded_regions (id, region_id) VALUES (1, 2)",
        &[],
    )
    .await?;

    // Stores: NYC(East), LA(West), Miami(South)
    ctx.query(
        "INSERT INTO stores (id, region_id, name) VALUES \
         (10, 1, 'NYC'), (11, 2, 'LA'), (12, 3, 'Miami')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO products (id, store_id, name) VALUES \
         (100, 10, 'Widget'), (101, 11, 'Gizmo'), (102, 12, 'Gadget')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let before = ctx.metrics().await?;

    // Products in stores whose region is NOT excluded
    // stores WHERE region_id IN (regions WHERE id NOT IN (excluded_regions))
    let query = "SELECT name FROM products \
                 WHERE store_id IN ( \
                     SELECT id FROM stores \
                     WHERE region_id IN ( \
                         SELECT id FROM regions \
                         WHERE id NOT IN (SELECT region_id FROM excluded_regions) \
                     ) \
                 ) \
                 ORDER BY name";

    // Non-excluded regions: East(1), South(3) → stores NYC(10), Miami(12) → Widget, Gadget
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("name", "Gadget")])?;
    assert_row_at(&res, 2, &[("name", "Widget")])?;

    wait_cache_load().await;

    // --- Cache hit ---
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);

    // --- CDC INSERT into excluded_regions: Exclusion → no invalidation ---
    // Exclude South(3) too — exclusion set grows, result can only shrink
    ctx.origin_query(
        "INSERT INTO excluded_regions (id, region_id) VALUES (2, 3)",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Exclusion INSERT does not invalidate → cache hit
    // But cache tables are updated: South is now excluded, so
    // Miami/Gadget should be removed from cache in-place.
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3); // Only Widget remains
    assert_row_at(&res, 1, &[("name", "Widget")])?;

    // --- CDC DELETE from excluded_regions: Exclusion → invalidates ---
    // Un-exclude West(2) — exclusion set shrinks, result may grow
    ctx.origin_query("DELETE FROM excluded_regions WHERE region_id = 2", &[])
        .await?;

    wait_for_cdc().await;

    // Exclusion DELETE invalidates → cache miss
    // Non-excluded: East(1), West(2) → NYC, LA → Widget, Gizmo
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("name", "Gizmo")])?;
    assert_row_at(&res, 2, &[("name", "Widget")])?;

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    assert_eq!(delta.queries_cacheable, 4, "cacheable queries");
    // miss, hit, hit (exclusion INSERT), miss (exclusion DELETE)
    assert_eq!(delta.queries_cache_miss, 2, "cache misses");
    assert_eq!(delta.queries_cache_hit, 2, "cache hits");

    Ok(())
}

/// Test IN nested inside NOT IN with CDC invalidation.
///
/// Pattern: SELECT ... WHERE col NOT IN (SELECT ... WHERE col IN (SELECT ...))
///
/// Source types:
/// - Outer: products (Direct)
/// - Middle: blacklist via NOT IN (Subquery/Exclusion)
/// - Inner: categories via IN (inherits negated=true → Subquery/Exclusion)
///
/// The NOT IN wrapping flips the inner IN's semantics:
/// - Inner IN under NOT IN → negated=true → Exclusion
/// - So INSERT into inner table does NOT invalidate (same as Exclusion)
/// - DELETE from inner table → invalidates (Exclusion shrinkage)
#[tokio::test]
async fn test_subquery_nested_in_inside_not_in() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE blacklisted_products (id INTEGER PRIMARY KEY, product_id INTEGER, category_id INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;

    // Categories: Electronics(1), Toys(2), Books(3)
    ctx.query(
        "INSERT INTO categories (id, name) VALUES (1, 'Electronics'), (2, 'Toys'), (3, 'Books')",
        &[],
    )
    .await?;

    // Blacklisted products in 'restricted' categories (Electronics)
    // Product 100 is blacklisted because it's in category Electronics
    ctx.query(
        "INSERT INTO blacklisted_products (id, product_id, category_id) VALUES \
         (1, 100, 1), (2, 102, 1)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO products (id, name) VALUES \
         (100, 'Laptop'), (101, 'Teddy Bear'), (102, 'Phone'), (103, 'Novel')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let before = ctx.metrics().await?;

    // Products NOT in the blacklist for Electronics category
    // NOT IN (blacklisted WHERE category_id IN (categories WHERE name = 'Electronics'))
    let query = "SELECT name FROM products \
                 WHERE id NOT IN ( \
                     SELECT product_id FROM blacklisted_products \
                     WHERE category_id IN ( \
                         SELECT id FROM categories WHERE name = 'Electronics' \
                     ) \
                 ) \
                 ORDER BY name";

    // Blacklisted for Electronics: product 100 (Laptop), 102 (Phone)
    // Remaining: Novel, Teddy Bear
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("name", "Novel")])?;
    assert_row_at(&res, 2, &[("name", "Teddy Bear")])?;

    wait_cache_load().await;

    // --- Cache hit ---
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);

    // --- CDC INSERT into blacklisted_products: Exclusion → no invalidation ---
    // Add product 101 (Teddy Bear) to Electronics blacklist
    // The NOT IN exclusion set grows → result can only shrink → safe
    ctx.origin_query(
        "INSERT INTO blacklisted_products (id, product_id, category_id) VALUES (3, 101, 1)",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Exclusion INSERT: no invalidation → cache hit
    // But Teddy Bear should be removed from cache in-place
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3); // Only Novel remains
    assert_row_at(&res, 1, &[("name", "Novel")])?;

    // --- CDC DELETE from blacklisted_products: Exclusion → invalidates ---
    // Remove Laptop(100) from blacklist — exclusion set shrinks
    ctx.origin_query(
        "DELETE FROM blacklisted_products WHERE product_id = 100",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Exclusion DELETE invalidates → cache miss
    // Blacklisted for Electronics: 101 (Teddy Bear), 102 (Phone)
    // Remaining: Laptop, Novel
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("name", "Laptop")])?;
    assert_row_at(&res, 2, &[("name", "Novel")])?;

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    assert_eq!(delta.queries_cacheable, 4, "cacheable queries");
    // miss, hit, hit (exclusion INSERT), miss (exclusion DELETE)
    assert_eq!(delta.queries_cache_miss, 2, "cache misses");
    assert_eq!(delta.queries_cache_hit, 2, "cache hits");

    Ok(())
}

// =============================================================================
// Multi-Table Subquery Dependencies
// =============================================================================

/// Test subquery with multiple table dependencies.
///
/// When a subquery references multiple tables, cache invalidation must
/// track dependencies on ALL referenced tables.
#[tokio::test]
async fn test_subquery_multi_table_dependency() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, category_id INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE inventory (id INTEGER PRIMARY KEY, item_id INTEGER, quantity INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO categories (id, name, active) VALUES (1, 'Electronics', true), (2, 'Furniture', false)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO items (id, name, category_id) VALUES \
         (1, 'Laptop', 1), (2, 'Phone', 1), (3, 'Chair', 2)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO inventory (id, item_id, quantity) VALUES \
         (1, 1, 10), (2, 2, 5), (3, 3, 20)",
        &[],
    )
    .await?;

    // Wait for setup CDC events to be processed before caching
    wait_for_cdc().await;

    let before = ctx.metrics().await?;

    // Get items in active categories that have inventory
    let query = "SELECT i.name, inv.quantity \
                 FROM items i \
                 JOIN inventory inv ON i.id = inv.item_id \
                 WHERE i.category_id IN ( \
                     SELECT c.id FROM categories c WHERE c.active = true \
                 ) \
                 ORDER BY i.name";

    let res = ctx.simple_query(query).await?;

    // Only Electronics items (Laptop, Phone)
    assert_eq!(res.len(), 4); // 2 rows
    assert_row_at(&res, 1, &[("name", "Laptop"), ("quantity", "10")])?;
    assert_row_at(&res, 2, &[("name", "Phone"), ("quantity", "5")])?;

    wait_cache_load().await;

    // Cache hit
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);

    // Now change category active status via CDC
    ctx.origin_query("UPDATE categories SET active = true WHERE id = 2", &[])
        .await?;

    wait_for_cdc().await;

    // Chair should now appear (Furniture is now active)
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5); // 3 rows now
    assert_row_at(&res, 1, &[("name", "Chair"), ("quantity", "20")])?;

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    // All 3 SELECT queries should be cacheable
    assert_eq!(
        delta.queries_cacheable, 3,
        "cacheable multi-table subqueries"
    );
    // First miss, second hit, third miss (CDC UPDATE on subquery table invalidates)
    assert_eq!(delta.queries_cache_miss, 2, "cache misses");
    assert_eq!(delta.queries_cache_hit, 1, "cache hits");

    Ok(())
}

// =============================================================================
// Constraint Filtering Tests
// =============================================================================

/// Test that derived table (FROM subquery) constraint filtering skips
/// invalidation for CDC events that don't match the inner WHERE clause.
///
/// With per-UpdateQuery constraints, the inner SELECT's WHERE clause
/// produces constraints on the inner table's UpdateQuery. For derived
/// tables (Subquery/Inclusion source), `row_constraints_match` checks
/// these constraints before proceeding to directional invalidation logic.
///
/// Before per-UpdateQuery constraints: inner tables had no constraints,
/// so every INSERT triggered directional invalidation.
/// After: non-matching rows are filtered by the inner WHERE constraint.
#[tokio::test]
async fn test_subquery_derived_table_constraint_filter() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE appliances (id INTEGER PRIMARY KEY, name TEXT, price INTEGER, dept TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO appliances (id, name, price, dept) VALUES \
         (1, 'Blender', 80, 'kitchen'), \
         (2, 'Toaster', 40, 'kitchen'), \
         (3, 'Drill', 120, 'workshop'), \
         (4, 'Mixer', 60, 'kitchen')",
        &[],
    )
    .await?;

    // Wait for setup CDC events to settle before caching
    wait_for_cdc().await;

    let before = ctx.metrics().await?;

    // Derived table with inner predicate: dept = 'kitchen'
    // The inner SELECT produces constraint {appliances.dept = 'kitchen'} on
    // the UpdateQuery for the appliances table (source = Subquery(Inclusion)).
    let query = "SELECT name, price \
                 FROM (SELECT * FROM appliances WHERE dept = 'kitchen') AS kitchen_items \
                 ORDER BY price";

    // First query — cache miss, populates cache
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5); // 3 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("name", "Toaster"), ("price", "40")])?;
    assert_row_at(&res, 2, &[("name", "Mixer"), ("price", "60")])?;
    assert_row_at(&res, 3, &[("name", "Blender"), ("price", "80")])?;

    wait_cache_load().await;

    // Second query — cache hit
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5);

    // --- Non-matching INSERT: dept = 'workshop' does NOT match constraint ---
    // row_constraints_match returns false → row_uncached_invalidation_check returns false
    // → no invalidation → cache hit
    ctx.origin_query(
        "INSERT INTO appliances (id, name, price, dept) VALUES (5, 'Wrench', 30, 'workshop')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Cache hit — non-matching INSERT was filtered by constraint
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5); // still 3 kitchen rows

    // --- Matching INSERT: dept = 'kitchen' DOES match constraint ---
    // row_constraints_match returns true → Subquery(Inclusion) + Insert → invalidate
    ctx.origin_query(
        "INSERT INTO appliances (id, name, price, dept) VALUES (6, 'Kettle', 50, 'kitchen')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Cache miss — matching INSERT triggered invalidation
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 6); // 4 kitchen rows now
    assert_row_at(&res, 1, &[("name", "Toaster"), ("price", "40")])?;
    assert_row_at(&res, 2, &[("name", "Kettle"), ("price", "50")])?;
    assert_row_at(&res, 3, &[("name", "Mixer"), ("price", "60")])?;
    assert_row_at(&res, 4, &[("name", "Blender"), ("price", "80")])?;

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    assert_eq!(delta.queries_cacheable, 4, "cacheable queries");
    // miss, hit, hit (non-matching filtered), miss (matching invalidated)
    assert_eq!(delta.queries_cache_miss, 2, "cache misses");
    assert_eq!(delta.queries_cache_hit, 2, "cache hits");

    Ok(())
}

/// Test that IN subquery constraint filtering skips invalidation for
/// CDC events on the inner table that don't match the inner WHERE clause.
///
/// With per-UpdateQuery constraints, the inner SELECT's WHERE clause
/// produces constraints on the inner table's UpdateQuery. For IN
/// subqueries (Subquery/Inclusion source), `row_constraints_match`
/// checks these constraints before directional invalidation logic.
///
/// Before per-UpdateQuery constraints: inner tables had empty constraints,
/// so all CDC events passed through to directional logic.
/// After: inner tables have their own WHERE constraints, filtering
/// irrelevant CDC events.
#[tokio::test]
async fn test_subquery_where_in_constraint_filter() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO departments (id, name, active) VALUES \
         (1, 'Engineering', true), (2, 'Marketing', true), (3, 'Sales', false)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO employees (id, name, dept_id) VALUES \
         (10, 'Alice', 1), (11, 'Bob', 2), (12, 'Charlie', 3)",
        &[],
    )
    .await?;

    // Wait for setup CDC events to settle
    wait_for_cdc().await;

    let before = ctx.metrics().await?;

    // IN subquery with inner predicate: active = true
    // The inner SELECT produces constraint {departments.active = true} on
    // the departments UpdateQuery (source = Subquery(Inclusion)).
    let query = "SELECT e.name FROM employees e \
                 WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.active = true) \
                 ORDER BY e.name";

    // First query — cache miss
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // Alice, Bob + RowDesc + CommandComplete
    assert_row_at(&res, 1, &[("name", "Alice")])?;
    assert_row_at(&res, 2, &[("name", "Bob")])?;

    wait_cache_load().await;

    // Second query — cache hit
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);

    // --- Non-matching INSERT on inner table: active = false ---
    // row_constraints_match returns false → no invalidation
    ctx.origin_query(
        "INSERT INTO departments (id, name, active) VALUES (4, 'Finance', false)",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Cache hit — non-matching INSERT on inner table was filtered
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // still Alice, Bob

    // --- Matching UPDATE on inner table: active changes to true ---
    // New data has active = true which matches constraint.
    // Row was NOT in cache (was false) → uncached path → constraint matches →
    // Subquery(Inclusion) + Update → invalidate.
    // Charlie's department (Sales, id=3) becomes active.
    ctx.origin_query("UPDATE departments SET active = true WHERE id = 3", &[])
        .await?;

    wait_for_cdc().await;

    // Cache miss — matching UPDATE triggered invalidation
    // Charlie now appears (Sales dept is active)
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5); // Alice, Bob, Charlie
    assert_row_at(&res, 1, &[("name", "Alice")])?;
    assert_row_at(&res, 2, &[("name", "Bob")])?;
    assert_row_at(&res, 3, &[("name", "Charlie")])?;

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    assert_eq!(delta.queries_cacheable, 4, "cacheable queries");
    // miss, hit, hit (non-matching filtered), miss (matching invalidated)
    assert_eq!(delta.queries_cache_miss, 2, "cache misses");
    assert_eq!(delta.queries_cache_hit, 2, "cache hits");

    Ok(())
}

// =============================================================================
// Correlated Subquery Tests (not yet supported for caching)
// =============================================================================

// Correlated subqueries reference columns from the outer query, which makes
// them impossible to resolve independently. These patterns are correctly
// detected and rejected at resolution time (CorrelatedSubqueryNotSupported).
// The proxy passes them through to origin and returns correct results,
// but they cannot be cached.
//
// Enable these tests when correlated subquery support is added:
//
// - WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.ref)
// - WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.ref)
// - SELECT col, (SELECT COUNT(*) FROM t2 WHERE t2.ref = t1.ref) FROM t1
// - SELECT ... WHERE col > (SELECT AVG(col) FROM t2 WHERE t2.ref = t1.ref)
// - LATERAL (SELECT ... WHERE subquery.ref = table.ref)
// - CASE WHEN EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.ref) THEN ...
