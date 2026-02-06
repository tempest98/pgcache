#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

//! Subquery caching tests (TDD)
//!
//! These tests document the expected behavior for caching queries that contain subqueries.
//! Initially these tests will fail because subqueries are not yet supported for caching.
//! Once subquery support is implemented, all tests should pass.
//!
//! Subquery types covered:
//! - Derived tables (FROM subqueries): SELECT * FROM (SELECT ...) AS alias
//! - WHERE IN subqueries: WHERE id IN (SELECT ...)
//! - WHERE EXISTS subqueries: WHERE EXISTS (SELECT ...)
//! - WHERE NOT IN / NOT EXISTS
//! - Scalar subqueries in SELECT list
//! - Scalar subqueries in WHERE clause
//! - Correlated subqueries (reference outer query)
//! - Nested subqueries

use std::io::Error;

use crate::util::{TestContext, assert_row_at, metrics_delta, wait_cache_load, wait_for_cdc};

mod util;

/// Consolidated test for subquery caching support.
#[tokio::test]
async fn test_subqueries() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    subquery_from_derived_table(&mut ctx).await?;
    subquery_from_derived_table_cdc(&mut ctx).await?;
    subquery_where_in(&mut ctx).await?;
    subquery_where_in_cdc(&mut ctx).await?;
    subquery_where_exists(&mut ctx).await?;
    subquery_where_not_in(&mut ctx).await?;
    subquery_where_not_exists(&mut ctx).await?;
    subquery_scalar_in_select(&mut ctx).await?;
    subquery_scalar_in_where(&mut ctx).await?;
    // subquery_correlated(&mut ctx).await?; // enable when correlated queries are supported
    subquery_nested(&mut ctx).await?;

    Ok(())
}

// =============================================================================
// Derived Table (FROM subquery) Tests
// =============================================================================

/// Test derived table (subquery in FROM clause) caching.
///
/// Pattern: SELECT ... FROM (SELECT ... FROM table WHERE ...) AS alias
///
/// This is one of the most common subquery patterns - creating an inline view
/// that filters or transforms data before the outer query operates on it.
async fn subquery_from_derived_table(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    // Create test table
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

    // Query with derived table (subquery in FROM)
    let query = "SELECT name, price FROM (SELECT * FROM products WHERE category = 'gadgets') AS gadget_products ORDER BY price";

    // First query - cache miss, populates cache
    let res = ctx.simple_query(query).await?;

    // Should have 3 gadget products
    assert_eq!(res.len(), 5); // 3 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("name", "Widget"), ("price", "100")])?;
    assert_row_at(&res, 2, &[("name", "Doodad"), ("price", "150")])?;
    assert_row_at(&res, 3, &[("name", "Gizmo"), ("price", "200")])?;

    wait_cache_load().await;

    // Second query - should hit cache
    let res = ctx.simple_query(query).await?;

    assert_eq!(res.len(), 5);
    assert_row_at(&res, 1, &[("name", "Widget"), ("price", "100")])?;

    // Verify metrics
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    // 2 setup queries (CREATE + INSERT) + 2 SELECT queries = 4 total
    assert_eq!(delta.queries_total, 4, "total queries");
    // Setup queries are uncacheable (DDL/DML via extended protocol)
    assert_eq!(delta.queries_uncacheable, 2, "uncacheable setup queries");
    assert_eq!(delta.queries_unsupported, 2, "unsupported setup queries");
    // Both SELECT queries with derived tables should be cacheable
    assert_eq!(delta.queries_cacheable, 2, "cacheable queries");
    // First: miss, second: hit
    assert_eq!(delta.queries_cache_miss, 1, "cache misses");
    assert_eq!(delta.queries_cache_hit, 1, "cache hits");

    Ok(())
}

/// Test derived table with CDC updates.
///
/// When the underlying table changes, queries using derived tables
/// that reference that table should see the updated results.
async fn subquery_from_derived_table_cdc(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    let query = "SELECT name, price FROM (SELECT * FROM products WHERE category = 'gadgets') AS gadget_products ORDER BY price";

    // Add new gadget via origin (CDC)
    ctx.origin_query(
        "INSERT INTO products (id, name, price, category) VALUES (5, 'NewGadget', 75, 'gadgets')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Query should include the new gadget
    let res = ctx.simple_query(query).await?;

    assert_eq!(res.len(), 6); // 4 rows now
    assert_row_at(&res, 1, &[("name", "NewGadget"), ("price", "75")])?;
    assert_row_at(&res, 2, &[("name", "Widget"), ("price", "100")])?;

    // Delete a gadget
    ctx.origin_query("DELETE FROM products WHERE id = 2", &[])
        .await?;

    wait_for_cdc().await;

    // Gizmo should be gone
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5); // back to 3 rows

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    // CDC updates cache in place
    assert_eq!(delta.queries_cacheable, 2, "cacheable queries");
    assert_eq!(delta.queries_cache_hit, 2, "cache hits after CDC");

    Ok(())
}

// =============================================================================
// WHERE IN Subquery Tests
// =============================================================================

/// Test WHERE IN subquery caching.
///
/// Pattern: SELECT ... FROM table WHERE column IN (SELECT column FROM other_table ...)
///
/// This pattern filters rows based on whether a value exists in the result
/// of another query. Common for finding related records.
async fn subquery_where_in(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    // Create related tables
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

    // Query: customers who have placed orders
    let query =
        "SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders) ORDER BY name";

    let res = ctx.simple_query(query).await?;

    // Alice and Bob have orders, Charlie does not
    assert_eq!(res.len(), 4); // 2 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("name", "Alice")])?;
    assert_row_at(&res, 2, &[("name", "Bob")])?;

    wait_cache_load().await;

    // Cache hit
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    assert_eq!(delta.queries_cacheable, 2, "cacheable IN subqueries");
    assert_eq!(delta.queries_cache_miss, 1, "cache misses");
    assert_eq!(delta.queries_cache_hit, 1, "cache hits");

    Ok(())
}

/// Test WHERE IN subquery with CDC updates.
///
/// When either the outer table or the subquery table changes,
/// the cache should reflect the updated relationship.
async fn subquery_where_in_cdc(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    let query =
        "SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders) ORDER BY name";

    // Charlie places an order via CDC
    ctx.origin_query(
        "INSERT INTO orders (id, customer_id, amount) VALUES (13, 3, 250)",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Charlie should now appear
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5); // 3 rows now
    assert_row_at(&res, 3, &[("name", "Charlie")])?;

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    // Cache hit (CDC updates in place)
    assert_eq!(delta.queries_cache_hit, 1, "cache hit after CDC");

    Ok(())
}

// =============================================================================
// WHERE EXISTS Subquery Tests
// =============================================================================

/// Test WHERE EXISTS subquery caching.
///
/// Pattern: SELECT ... FROM table WHERE EXISTS (SELECT 1 FROM other_table WHERE ...)
///
/// EXISTS tests for the presence of rows. It's often more efficient than IN
/// for correlated subqueries and handles NULL values differently.
async fn subquery_where_exists(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    // Query: customers who have orders > 200
    let query = "SELECT name FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id AND amount > 200) ORDER BY name";

    let res = ctx.simple_query(query).await?;

    // Only Alice has orders > 200 (500 and 300)
    assert_eq!(res.len(), 3); // 1 row + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("name", "Alice")])?;

    wait_cache_load().await;

    // Cache hit
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3);

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    assert_eq!(delta.queries_cacheable, 2, "cacheable EXISTS subqueries");
    assert_eq!(delta.queries_cache_miss, 1, "cache misses");
    assert_eq!(delta.queries_cache_hit, 1, "cache hits");

    Ok(())
}

// =============================================================================
// WHERE NOT IN / NOT EXISTS Tests
// =============================================================================

/// Test WHERE NOT IN subquery caching.
///
/// Pattern: SELECT ... FROM table WHERE column NOT IN (SELECT column FROM other_table ...)
///
/// NOT IN excludes rows where the value appears in the subquery result.
/// Note: NOT IN has special NULL handling that can be surprising.
async fn subquery_where_not_in(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    // Create a table for products on sale
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

    // Query: products NOT on sale
    let query = "SELECT name FROM products WHERE id NOT IN (SELECT product_id FROM sale_items) ORDER BY name";

    let res = ctx.simple_query(query).await?;

    // Products 1 (Widget) and 3 (Thing) are on sale
    // Remaining should be Doodad, NewGadget (from earlier test)
    // Note: Gizmo was deleted in earlier test
    assert_eq!(res.len(), 4); // 2 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("name", "Doodad")])?;
    assert_row_at(&res, 2, &[("name", "NewGadget")])?;

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

/// Test WHERE NOT EXISTS subquery caching.
///
/// Pattern: SELECT ... FROM table WHERE NOT EXISTS (SELECT 1 FROM other_table WHERE ...)
///
/// NOT EXISTS is often preferred over NOT IN because it handles NULLs better
/// and can be more efficient for correlated subqueries.
async fn subquery_where_not_exists(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    // Query: customers without any orders
    // After previous tests: Alice, Bob, Charlie all have orders now
    // So we need to add a new customer without orders
    ctx.origin_query(
        "INSERT INTO customers (id, name, tier) VALUES (4, 'Diana', 'bronze')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let query = "SELECT name FROM customers WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id) ORDER BY name";

    let res = ctx.simple_query(query).await?;

    // Only Diana has no orders
    assert_eq!(res.len(), 3); // 1 row + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("name", "Diana")])?;

    wait_cache_load().await;

    let res_cached = ctx.simple_query(query).await?;
    assert_eq!(res.len(), res_cached.len(), "cache returns same results");

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    assert_eq!(
        delta.queries_cacheable, 2,
        "cacheable NOT EXISTS subqueries"
    );
    assert_eq!(delta.queries_cache_miss, 1, "cache misses");
    assert_eq!(delta.queries_cache_hit, 1, "cache hits");

    Ok(())
}

// =============================================================================
// Scalar Subquery Tests
// =============================================================================

/// Test scalar subquery in SELECT list caching.
///
/// Pattern: SELECT column, (SELECT aggregate(...) FROM ... WHERE ...) AS alias FROM table
///
/// Scalar subqueries return a single value and can appear in the SELECT list.
/// They're commonly used for computed columns like counts or aggregates.
async fn subquery_scalar_in_select(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    // Query with scalar subquery in SELECT
    let query = "SELECT name, (SELECT COUNT(*) FROM orders WHERE orders.customer_id = customers.id) AS order_count FROM customers ORDER BY name";

    let res = ctx.simple_query(query).await?;

    // All 4 customers with their order counts
    // Alice: 2 orders, Bob: 1 order, Charlie: 1 order, Diana: 0 orders
    assert_eq!(res.len(), 6); // 4 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("name", "Alice"), ("order_count", "2")])?;
    assert_row_at(&res, 2, &[("name", "Bob"), ("order_count", "1")])?;
    assert_row_at(&res, 3, &[("name", "Charlie"), ("order_count", "1")])?;
    assert_row_at(&res, 4, &[("name", "Diana"), ("order_count", "0")])?;

    wait_cache_load().await;

    // Cache hit
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 6);

    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);

    assert_eq!(delta.queries_cacheable, 2, "cacheable scalar subqueries");
    assert_eq!(delta.queries_cache_miss, 1, "cache misses");
    assert_eq!(delta.queries_cache_hit, 1, "cache hits");

    Ok(())
}

/// Test scalar subquery in WHERE clause caching.
///
/// Pattern: SELECT ... FROM table WHERE column > (SELECT aggregate(...) FROM ...)
///
/// Scalar subqueries in WHERE are used for dynamic filtering based on
/// computed values like averages, maximums, etc.
async fn subquery_scalar_in_where(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    // Query: products with price above average
    // Current products after earlier tests: Widget(100), Thing(50), Doodad(150), NewGadget(75)
    // Average = (100 + 50 + 150 + 75) / 4 = 93.75
    let query = "SELECT name, price FROM products WHERE price > (SELECT AVG(price) FROM products) ORDER BY price";

    let res = ctx.simple_query(query).await?;

    // Widget(100) and Doodad(150) are above average
    assert_eq!(res.len(), 4); // 2 rows + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("name", "Widget"), ("price", "100")])?;
    assert_row_at(&res, 2, &[("name", "Doodad"), ("price", "150")])?;

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
// Correlated Subquery Tests
// =============================================================================

// enable when correlated queries are supported
// /// Test correlated subquery caching.
// ///
// /// Pattern: SELECT ... FROM table t1 WHERE column > (SELECT agg(...) FROM table t2 WHERE t2.ref = t1.ref)
// ///
// /// Correlated subqueries reference columns from the outer query.
// /// They're conceptually executed once per row of the outer query.
// async fn subquery_correlated(ctx: &mut TestContext) -> Result<(), Error> {
//     let before = ctx.metrics().await?;

//     // Create employee table for self-join correlated subquery
//     ctx.query(
//         "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, department TEXT, salary INTEGER)",
//         &[],
//     )
//     .await?;

//     ctx.query(
//         "INSERT INTO employees (id, name, department, salary) VALUES \
//          (1, 'Alice', 'eng', 100000), \
//          (2, 'Bob', 'eng', 90000), \
//          (3, 'Charlie', 'sales', 80000), \
//          (4, 'Diana', 'sales', 85000)",
//         &[],
//     )
//     .await?;

//     // Query: employees who earn more than the average in their department
//     // eng avg = 95000, sales avg = 82500
//     let query = "SELECT name, department, salary FROM employees e1 \
//                  WHERE salary > (SELECT AVG(salary) FROM employees e2 WHERE e2.department = e1.department) \
//                  ORDER BY name";

//     let res = ctx.simple_query(query).await?;

//     // Alice (100k > 95k eng avg) and Diana (85k > 82.5k sales avg)
//     assert_eq!(res.len(), 4); // 2 rows + RowDescription + CommandComplete
//     assert_row_at(
//         &res,
//         1,
//         &[
//             ("name", "Alice"),
//             ("department", "eng"),
//             ("salary", "100000"),
//         ],
//     )?;
//     assert_row_at(
//         &res,
//         2,
//         &[
//             ("name", "Diana"),
//             ("department", "sales"),
//             ("salary", "85000"),
//         ],
//     )?;

//     wait_cache_load().await;

//     // Cache hit
//     let res = ctx.simple_query(query).await?;
//     assert_eq!(res.len(), 4);

//     let after = ctx.metrics().await?;
//     let delta = metrics_delta(&before, &after);

//     assert_eq!(
//         delta.queries_cacheable, 2,
//         "cacheable correlated subqueries"
//     );
//     assert_eq!(delta.queries_cache_miss, 1, "cache misses");
//     assert_eq!(delta.queries_cache_hit, 1, "cache hits");

//     Ok(())
// }

// =============================================================================
// Nested Subquery Tests
// =============================================================================

/// Test nested subqueries caching.
///
/// Pattern: SELECT ... WHERE column IN (SELECT ... WHERE value > (SELECT ... FROM (SELECT ...) AS ...))
///
/// Subqueries can be nested multiple levels deep. The cache system needs to
/// track table dependencies through all nesting levels.
async fn subquery_nested(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;

    // Query with nested subqueries:
    // Find customers whose total order amount is above the average total order amount
    let query = "SELECT name FROM customers \
                 WHERE id IN ( \
                     SELECT customer_id FROM orders \
                     GROUP BY customer_id \
                     HAVING SUM(amount) > ( \
                         SELECT AVG(total) FROM ( \
                             SELECT SUM(amount) as total FROM orders GROUP BY customer_id \
                         ) AS totals \
                     ) \
                 ) \
                 ORDER BY name";

    let res = ctx.simple_query(query).await?;

    // Order totals: Alice=800, Bob=100, Charlie=250
    // Average total = (800 + 100 + 250) / 3 = 383.33
    // Only Alice is above average
    assert_eq!(res.len(), 3); // 1 row + RowDescription + CommandComplete
    assert_row_at(&res, 1, &[("name", "Alice")])?;

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
// Additional Subquery Pattern Tests
// =============================================================================

// enable when correlated queries are supported
// /// Test LATERAL subquery caching.
// ///
// /// Pattern: SELECT ... FROM table, LATERAL (SELECT ... WHERE subquery.ref = table.ref)
// ///
// /// LATERAL subqueries can reference columns from preceding FROM items.
// /// This is similar to correlated subqueries but in the FROM clause.
// #[tokio::test]
// async fn test_subquery_lateral() -> Result<(), Error> {
//     let mut ctx = TestContext::setup().await?;

//     ctx.query(
//         "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)",
//         &[],
//     )
//     .await?;

//     ctx.query(
//         "CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary INTEGER)",
//         &[],
//     )
//     .await?;

//     ctx.query(
//         "INSERT INTO departments (id, name) VALUES (1, 'Engineering'), (2, 'Sales')",
//         &[],
//     )
//     .await?;

//     ctx.query(
//         "INSERT INTO emp (id, name, dept_id, salary) VALUES \
//          (1, 'Alice', 1, 100000), (2, 'Bob', 1, 90000), \
//          (3, 'Charlie', 2, 80000), (4, 'Diana', 2, 85000)",
//         &[],
//     )
//     .await?;

//     let before = ctx.metrics().await?;

//     // LATERAL subquery: Get top earner per department
//     let query = "SELECT d.name as dept, e.name as employee, e.salary \
//                  FROM departments d, \
//                  LATERAL (SELECT * FROM emp WHERE emp.dept_id = d.id ORDER BY salary DESC LIMIT 1) e \
//                  ORDER BY d.name";

//     let res = ctx.simple_query(query).await?;

//     // Top earners: Alice (Engineering), Diana (Sales)
//     assert_eq!(res.len(), 4); // 2 rows + RowDescription + CommandComplete
//     assert_row_at(
//         &res,
//         1,
//         &[
//             ("dept", "Engineering"),
//             ("employee", "Alice"),
//             ("salary", "100000"),
//         ],
//     )?;
//     assert_row_at(
//         &res,
//         2,
//         &[
//             ("dept", "Sales"),
//             ("employee", "Diana"),
//             ("salary", "85000"),
//         ],
//     )?;

//     wait_cache_load().await;

//     let res_cached = ctx.simple_query(query).await?;
//     assert_eq!(res.len(), res_cached.len(), "cache returns same results");

//     let after = ctx.metrics().await?;
//     let delta = metrics_delta(&before, &after);

//     // Note: LATERAL queries may have additional complexity due to LIMIT in subquery
//     // This test documents expected behavior for when LATERAL support is added
//     assert_eq!(delta.queries_cacheable, 2, "cacheable LATERAL subqueries");
//     assert_eq!(delta.queries_cache_miss, 1, "cache misses");
//     assert_eq!(delta.queries_cache_hit, 1, "cache hits");

//     Ok(())
// }

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

    // Query with subquery referencing multiple tables
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
    // First miss, then hits (CDC updates in place)
    assert_eq!(delta.queries_cache_miss, 1, "cache misses");
    assert_eq!(delta.queries_cache_hit, 2, "cache hits");

    Ok(())
}

// enable when correlated queries are supported
// /// Test subquery in CASE expression.
// ///
// /// Pattern: SELECT CASE WHEN EXISTS (SELECT ...) THEN ... ELSE ... END FROM table
// ///
// /// Subqueries can appear within CASE expressions for conditional logic.
// #[tokio::test]
// async fn test_subquery_in_case() -> Result<(), Error> {
//     let mut ctx = TestContext::setup().await?;

//     ctx.query(
//         "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
//         &[],
//     )
//     .await?;

//     ctx.query(
//         "CREATE TABLE user_orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)",
//         &[],
//     )
//     .await?;

//     ctx.query(
//         "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
//         &[],
//     )
//     .await?;

//     ctx.query(
//         "INSERT INTO user_orders (id, user_id, amount) VALUES (1, 1, 100)",
//         &[],
//     )
//     .await?;

//     let before = ctx.metrics().await?;

//     // CASE with EXISTS subquery
//     let query = "SELECT name, \
//                  CASE WHEN EXISTS (SELECT 1 FROM user_orders WHERE user_orders.user_id = users.id) \
//                       THEN 'has_orders' \
//                       ELSE 'no_orders' \
//                  END AS status \
//                  FROM users \
//                  ORDER BY name";

//     let res = ctx.simple_query(query).await?;

//     assert_eq!(res.len(), 4); // 2 rows
//     assert_row_at(&res, 1, &[("name", "Alice"), ("status", "has_orders")])?;
//     assert_row_at(&res, 2, &[("name", "Bob"), ("status", "no_orders")])?;

//     wait_cache_load().await;

//     let res_cached = ctx.simple_query(query).await?;
//     assert_eq!(res.len(), res_cached.len(), "cache returns same results");

//     let after = ctx.metrics().await?;
//     let delta = metrics_delta(&before, &after);

//     assert_eq!(delta.queries_cacheable, 2, "cacheable CASE subqueries");
//     assert_eq!(delta.queries_cache_miss, 1, "cache misses");
//     assert_eq!(delta.queries_cache_hit, 1, "cache hits");

//     Ok(())
// }
