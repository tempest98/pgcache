#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

//! Correlated subquery caching tests (EXISTS, NOT EXISTS, scalar).
//!
//! Correlated subqueries reference columns from the outer query. pgcache
//! decorrelates them into JOINs for update query generation so that CDC
//! invalidation can track dependencies on the inner tables.
//!
//! Decorrelation transforms:
//! - EXISTS → INNER JOIN + DISTINCT (semi-join)
//! - NOT EXISTS → LEFT JOIN + IS NULL (anti-join)
//! - Scalar subquery → LEFT JOIN + derived table (aggregate or lookup)
//!
//! These tests verify:
//! - Correlated queries are cached and served correctly
//! - CDC events on inner tables trigger appropriate invalidation
//! - CDC events on outer tables trigger appropriate invalidation
//! - EXISTS with GROUP BY/HAVING in inner query (stripped for update queries)
//! - Residual predicates on inner tables filter CDC events correctly
//! - Scalar subqueries in SELECT list and WHERE clause

use std::io::Error;

use crate::util::{
    TestContext, assert_cache_hit, assert_cache_miss, assert_row_at, wait_cache_load, wait_for_cdc,
};

mod util;

// =============================================================================
// EXISTS — Basic Caching and CDC Invalidation
// =============================================================================

/// Test correlated EXISTS subquery caching and CDC invalidation.
///
/// Pattern: SELECT ... FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.ref)
///
/// Decorrelated form (for update queries):
///   SELECT DISTINCT t1.* FROM t1 JOIN t2 ON t2.ref = t1.ref
///
/// CDC behavior after decorrelation:
/// - INSERT into inner table → invalidates (new correlation match may appear)
/// - DELETE from inner table → invalidates (correlation match may disappear)
/// - UPDATE on inner table correlation column → invalidates
/// - INSERT into outer table → does NOT invalidate (new row evaluated at serve time)
#[tokio::test]
async fn test_correlated_exists_basic() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, emp_id INTEGER, status TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO employees (id, name, dept_id) VALUES \
         (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 10)",
        &[],
    )
    .await?;

    // Only Alice and Charlie have orders
    ctx.query(
        "INSERT INTO orders (id, emp_id, status) VALUES \
         (100, 1, 'shipped'), (101, 3, 'pending')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Employees who have at least one order
    let query = "SELECT e.name FROM employees e \
                 WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id) \
                 ORDER BY e.name";

    // --- Cache miss, populates cache ---
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // Alice, Charlie + RowDesc + CommandComplete
    assert_row_at(&res, 1, &[("name", "Alice")])?;
    assert_row_at(&res, 2, &[("name", "Charlie")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // --- Cache hit ---
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // --- CDC INSERT on inner table (orders) → invalidates ---
    // Add an order for Bob — he should now appear in results
    ctx.origin_query(
        "INSERT INTO orders (id, emp_id, status) VALUES (102, 2, 'shipped')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5); // Alice, Bob, Charlie
    assert_row_at(&res, 1, &[("name", "Alice")])?;
    assert_row_at(&res, 2, &[("name", "Bob")])?;
    assert_row_at(&res, 3, &[("name", "Charlie")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // --- CDC DELETE on inner table (orders) → in-place removal, cache hit ---
    // Direct source: row removed from cache table, serve-time EXISTS
    // re-evaluates. INNER JOIN DELETE can only shrink the result.
    ctx.origin_query("DELETE FROM orders WHERE id = 101", &[])
        .await?;

    wait_for_cdc().await;

    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // Alice, Bob
    assert_row_at(&res, 1, &[("name", "Alice")])?;
    assert_row_at(&res, 2, &[("name", "Bob")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

// =============================================================================
// NOT EXISTS — Basic Caching and CDC Invalidation
// =============================================================================

/// Test correlated NOT EXISTS subquery caching and CDC invalidation.
///
/// Pattern: SELECT ... FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.ref)
///
/// Decorrelated form (for update queries):
///   SELECT t1.* FROM t1 LEFT JOIN t2 ON t2.ref = t1.ref WHERE t2.ref IS NULL
///
/// CDC behavior after decorrelation:
/// - INSERT into inner table → invalidates (match may appear, removing outer row from result)
/// - DELETE from inner table → invalidates (match may disappear, adding outer row to result)
/// - UPDATE on inner table → invalidates
#[tokio::test]
async fn test_correlated_not_exists_basic() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO departments (id, name) VALUES \
         (1, 'Engineering'), (2, 'Marketing'), (3, 'Sales')",
        &[],
    )
    .await?;

    // Engineering and Sales have employees; Marketing is empty
    ctx.query(
        "INSERT INTO employees (id, name, dept_id) VALUES \
         (10, 'Alice', 1), (11, 'Bob', 3)",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Departments with no employees
    let query = "SELECT d.name FROM departments d \
                 WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id) \
                 ORDER BY d.name";

    // --- Cache miss ---
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3); // Marketing + RowDesc + CommandComplete
    assert_row_at(&res, 1, &[("name", "Marketing")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // --- Cache hit ---
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // --- CDC INSERT on inner table (employees) → invalidates ---
    // Add employee to Marketing — it should disappear from results
    ctx.origin_query(
        "INSERT INTO employees (id, name, dept_id) VALUES (12, 'Carol', 2)",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 2); // no departments without employees + RowDesc + CommandComplete
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // --- CDC DELETE on inner table (employees) → invalidates ---
    // Remove Bob from Sales — Sales should reappear
    ctx.origin_query("DELETE FROM employees WHERE id = 11", &[])
        .await?;

    wait_for_cdc().await;

    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3); // Sales + RowDesc + CommandComplete
    assert_row_at(&res, 1, &[("name", "Sales")])?;
    let _m = assert_cache_miss(&mut ctx, m).await?;

    Ok(())
}

// =============================================================================
// EXISTS with Residual Predicates — Constraint Filtering
// =============================================================================

/// Test EXISTS with residual predicates (non-correlation WHERE conditions).
///
/// Pattern: SELECT ... FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.ref AND t2.col = 'val')
///
/// The residual predicate (t2.col = 'val') produces constraints on the inner
/// table's update query. CDC events that don't match the constraint should NOT
/// trigger invalidation.
#[tokio::test]
async fn test_correlated_exists_residual_constraint_filter() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, emp_id INTEGER, status TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO employees (id, name) VALUES \
         (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
        &[],
    )
    .await?;

    // Only Alice has a 'shipped' order
    ctx.query(
        "INSERT INTO orders (id, emp_id, status) VALUES \
         (100, 1, 'shipped'), (101, 2, 'pending')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Employees with at least one shipped order
    let query = "SELECT e.name FROM employees e \
                 WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id AND o.status = 'shipped') \
                 ORDER BY e.name";

    // --- Cache miss ---
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3); // Alice + RowDesc + CommandComplete
    assert_row_at(&res, 1, &[("name", "Alice")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // --- Cache hit ---
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // --- Non-matching INSERT: status = 'pending' does NOT match constraint ---
    ctx.origin_query(
        "INSERT INTO orders (id, emp_id, status) VALUES (102, 3, 'pending')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Cache hit — 'pending' doesn't match the 'shipped' constraint
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3); // still just Alice
    let m = assert_cache_hit(&mut ctx, m).await?;

    // --- Matching INSERT: status = 'shipped' DOES match constraint ---
    ctx.origin_query(
        "INSERT INTO orders (id, emp_id, status) VALUES (103, 3, 'shipped')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Cache miss — matching INSERT triggered invalidation
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // Alice, Charlie
    assert_row_at(&res, 1, &[("name", "Alice")])?;
    assert_row_at(&res, 2, &[("name", "Charlie")])?;
    let _m = assert_cache_miss(&mut ctx, m).await?;

    Ok(())
}

// =============================================================================
// NOT EXISTS with Residual Predicates
// =============================================================================

/// Test NOT EXISTS with residual predicates — residual goes into JOIN ON clause.
///
/// Pattern: SELECT ... FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.ref AND t2.col = 'val')
///
/// For NOT EXISTS, residual predicates go in the ON clause (not outer WHERE)
/// to preserve LEFT JOIN semantics. A department is included if no employees
/// match the full ON condition (correlation + residual).
#[tokio::test]
async fn test_correlated_not_exists_residual_predicates() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, status TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO departments (id, name) VALUES \
         (1, 'Engineering'), (2, 'Marketing'), (3, 'Sales')",
        &[],
    )
    .await?;

    // Engineering has an active employee; Marketing has an inactive one; Sales has none
    ctx.query(
        "INSERT INTO employees (id, name, dept_id, status) VALUES \
         (10, 'Alice', 1, 'active'), (11, 'Bob', 2, 'inactive')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Departments with no ACTIVE employees
    let query = "SELECT d.name FROM departments d \
                 WHERE NOT EXISTS ( \
                     SELECT 1 FROM employees e \
                     WHERE e.dept_id = d.id AND e.status = 'active' \
                 ) \
                 ORDER BY d.name";

    // --- Cache miss ---
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    // Marketing (has only inactive), Sales (has no employees)
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("name", "Marketing")])?;
    assert_row_at(&res, 2, &[("name", "Sales")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // --- Cache hit ---
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // --- INSERT active employee into Sales → Sales should disappear ---
    ctx.origin_query(
        "INSERT INTO employees (id, name, dept_id, status) VALUES (12, 'Carol', 3, 'active')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3); // only Marketing
    assert_row_at(&res, 1, &[("name", "Marketing")])?;
    let _m = assert_cache_miss(&mut ctx, m).await?;

    Ok(())
}

// =============================================================================
// EXISTS with Inner GROUP BY — Stripped for Update Queries
// =============================================================================

/// Test correlated EXISTS with GROUP BY in the inner subquery.
///
/// GROUP BY/HAVING are stripped from the inner subquery for update query
/// generation (conservative, over-invalidation is safe for EXISTS).
/// The cached query still returns correct results because population uses
/// the original query.
///
/// Pattern: SELECT ... FROM t1
///          WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.ref = t1.ref GROUP BY t2.col HAVING count(*) > 1)
#[tokio::test]
async fn test_correlated_exists_with_group_by() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, emp_id INTEGER, status TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO employees (id, name) VALUES \
         (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
        &[],
    )
    .await?;

    // Alice has 2 orders, Bob has 1, Charlie has 0
    ctx.query(
        "INSERT INTO orders (id, emp_id, status) VALUES \
         (100, 1, 'shipped'), (101, 1, 'pending'), (102, 2, 'shipped')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Employees with more than 1 order
    let query = "SELECT e.name FROM employees e \
                 WHERE EXISTS ( \
                     SELECT 1 FROM orders o \
                     WHERE o.emp_id = e.id \
                     GROUP BY o.emp_id \
                     HAVING count(*) > 1 \
                 ) \
                 ORDER BY e.name";

    // --- Cache miss ---
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3); // Alice + RowDesc + CommandComplete
    assert_row_at(&res, 1, &[("name", "Alice")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // --- Cache hit ---
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // --- CDC INSERT giving Bob a second order → Bob should appear ---
    // GROUP BY is stripped for update queries, so any orders INSERT triggers
    // invalidation (conservative). The re-populated result correctly applies
    // the HAVING filter.
    ctx.origin_query(
        "INSERT INTO orders (id, emp_id, status) VALUES (103, 2, 'pending')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // Alice, Bob
    assert_row_at(&res, 1, &[("name", "Alice")])?;
    assert_row_at(&res, 2, &[("name", "Bob")])?;
    let _m = assert_cache_miss(&mut ctx, m).await?;

    Ok(())
}

// =============================================================================
// EXISTS — CDC on Outer Table
// =============================================================================

/// Test that CDC events on the outer table correctly affect cached results.
///
/// After decorrelation, the outer table is the left side of the JOIN (Direct source).
/// For Direct source, CDC UPDATEs are applied in-place to cached rows — the cache
/// self-corrects without full invalidation, so the next query is a cache HIT.
#[tokio::test]
async fn test_correlated_exists_outer_table_cdc() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, emp_id INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO employees (id, name, active) VALUES \
         (1, 'Alice', 1), (2, 'Bob', 1), (3, 'Charlie', 0)",
        &[],
    )
    .await?;

    // Alice and Bob have orders
    ctx.query(
        "INSERT INTO orders (id, emp_id) VALUES (100, 1), (101, 2)",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Active employees who have orders
    let query = "SELECT e.name FROM employees e \
                 WHERE e.active = 1 \
                 AND EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id) \
                 ORDER BY e.name";

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // Alice, Bob
    assert_row_at(&res, 1, &[("name", "Alice")])?;
    assert_row_at(&res, 2, &[("name", "Bob")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // --- CDC UPDATE on outer table: deactivate Bob ---
    // Direct source: row updated in-place in cache, no full invalidation needed.
    ctx.origin_query("UPDATE employees SET active = 0 WHERE id = 2", &[])
        .await?;

    wait_for_cdc().await;

    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3); // only Alice
    assert_row_at(&res, 1, &[("name", "Alice")])?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

// =============================================================================
// EXISTS with Multiple Tables in Inner Subquery
// =============================================================================

/// Test EXISTS where the inner subquery joins multiple tables.
///
/// Pattern: SELECT ... FROM t1
///          WHERE EXISTS (SELECT 1 FROM t2 JOIN t3 ON ... WHERE t2.ref = t1.ref)
///
/// CDC should track dependencies on all inner tables (t2 and t3).
#[tokio::test]
async fn test_correlated_exists_inner_join() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, emp_id INTEGER, customer_id INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, tier TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO employees (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO customers (id, name, tier) VALUES \
         (10, 'Acme', 'gold'), (11, 'Beta', 'silver')",
        &[],
    )
    .await?;

    // Alice has an order with gold customer, Bob has one with silver
    ctx.query(
        "INSERT INTO orders (id, emp_id, customer_id) VALUES (100, 1, 10), (101, 2, 11)",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Employees who have orders from gold-tier customers
    let query = "SELECT e.name FROM employees e \
                 WHERE EXISTS ( \
                     SELECT 1 FROM orders o \
                     JOIN customers c ON c.id = o.customer_id \
                     WHERE o.emp_id = e.id AND c.tier = 'gold' \
                 ) \
                 ORDER BY e.name";

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3); // Alice
    assert_row_at(&res, 1, &[("name", "Alice")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // --- CDC UPDATE on inner joined table (customers): upgrade Beta to gold ---
    ctx.origin_query("UPDATE customers SET tier = 'gold' WHERE id = 11", &[])
        .await?;

    wait_for_cdc().await;

    // Bob should now appear (his customer is now gold)
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // Alice, Bob
    assert_row_at(&res, 1, &[("name", "Alice")])?;
    assert_row_at(&res, 2, &[("name", "Bob")])?;
    let _m = assert_cache_miss(&mut ctx, m).await?;

    Ok(())
}

// =============================================================================
// Scalar Subquery in SELECT List — Aggregate (COUNT)
// =============================================================================

/// Test correlated scalar subquery with aggregate in SELECT list.
///
/// Pattern: SELECT e.name, (SELECT COUNT(*) FROM orders o WHERE o.emp_id = e.id) FROM employees e
///
/// Decorrelated form (transient, for update queries / population branches):
///   SELECT e.name, _dc1._ds1
///   FROM employees e
///   LEFT JOIN (SELECT emp_id, COUNT(*) AS _ds1 FROM orders GROUP BY emp_id) _dc1
///     ON _dc1.emp_id = e.id
///
/// The original (correlated) form is stored for serving — PostgreSQL evaluates
/// COUNT(*) against the cache database, returning 0 (not NULL) for employees
/// with no orders.
///
/// CDC behavior:
/// - INSERT/DELETE on orders → invalidates (count changes)
/// - UPDATE on outer table → applied in-place (Direct source)
#[tokio::test]
async fn test_correlated_scalar_select_count() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, emp_id INTEGER, status TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO employees (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
        &[],
    )
    .await?;

    // Alice has 2 orders, Bob has 1, Charlie has 0
    ctx.query(
        "INSERT INTO orders (id, emp_id, status) VALUES \
         (100, 1, 'shipped'), (101, 1, 'pending'), (102, 2, 'shipped')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let query = "SELECT e.name, \
                     (SELECT count(*) FROM orders o WHERE o.emp_id = e.id) AS order_count \
                 FROM employees e \
                 ORDER BY e.name";

    // --- Cache miss, populates cache ---
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5); // 3 rows + RowDesc + CommandComplete
    assert_row_at(&res, 1, &[("name", "Alice"), ("order_count", "2")])?;
    assert_row_at(&res, 2, &[("name", "Bob"), ("order_count", "1")])?;
    assert_row_at(&res, 3, &[("name", "Charlie"), ("order_count", "0")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // --- Cache hit ---
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5);
    assert_row_at(&res, 1, &[("name", "Alice"), ("order_count", "2")])?;
    assert_row_at(&res, 2, &[("name", "Bob"), ("order_count", "1")])?;
    assert_row_at(&res, 3, &[("name", "Charlie"), ("order_count", "0")])?;
    let m = assert_cache_hit(&mut ctx, m).await?;

    // --- CDC INSERT on inner table → invalidates ---
    ctx.origin_query(
        "INSERT INTO orders (id, emp_id, status) VALUES (103, 3, 'pending')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5);
    assert_row_at(&res, 1, &[("name", "Alice"), ("order_count", "2")])?;
    assert_row_at(&res, 2, &[("name", "Bob"), ("order_count", "1")])?;
    assert_row_at(&res, 3, &[("name", "Charlie"), ("order_count", "1")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // --- CDC DELETE on inner table → invalidates ---
    ctx.origin_query("DELETE FROM orders WHERE id = 100", &[])
        .await?;

    wait_for_cdc().await;

    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5);
    assert_row_at(&res, 1, &[("name", "Alice"), ("order_count", "1")])?;
    assert_row_at(&res, 2, &[("name", "Bob"), ("order_count", "1")])?;
    assert_row_at(&res, 3, &[("name", "Charlie"), ("order_count", "1")])?;
    let _m = assert_cache_miss(&mut ctx, m).await?;

    Ok(())
}

// =============================================================================
// Scalar Subquery in SELECT List — Non-Aggregate Lookup
// =============================================================================

/// Test correlated scalar subquery without aggregate (direct lookup).
///
/// Pattern: SELECT e.name, (SELECT d.name FROM departments d WHERE d.id = e.dept_id) FROM employees e
///
/// No GROUP BY in the derived table since there's no aggregate.
/// CDC on departments should invalidate.
#[tokio::test]
async fn test_correlated_scalar_select_lookup() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO departments (id, name) VALUES (1, 'Engineering'), (2, 'Marketing')",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO employees (id, name, dept_id) VALUES \
         (10, 'Alice', 1), (11, 'Bob', 2)",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let query = "SELECT e.name, \
                     (SELECT d.name FROM departments d WHERE d.id = e.dept_id) AS dept_name \
                 FROM employees e \
                 ORDER BY e.name";

    // --- Cache miss ---
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // 2 rows + RowDesc + CommandComplete
    assert_row_at(&res, 1, &[("name", "Alice"), ("dept_name", "Engineering")])?;
    assert_row_at(&res, 2, &[("name", "Bob"), ("dept_name", "Marketing")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // --- Cache hit ---
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // --- CDC UPDATE on inner table (departments) → invalidates ---
    ctx.origin_query(
        "UPDATE departments SET name = 'Eng' WHERE id = 1",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("name", "Alice"), ("dept_name", "Eng")])?;
    assert_row_at(&res, 2, &[("name", "Bob"), ("dept_name", "Marketing")])?;
    let _m = assert_cache_miss(&mut ctx, m).await?;

    Ok(())
}

// =============================================================================
// Scalar Subquery in WHERE Clause — Aggregate Comparison
// =============================================================================

/// Test correlated scalar subquery in WHERE clause (comparison with aggregate).
///
/// Pattern: SELECT ... FROM departments d
///          WHERE d.budget > (SELECT avg(d2.budget) FROM departments d2 WHERE d2.location = d.location)
///
/// Self-join: same table referenced in both inner and outer with different aliases.
/// CDC on departments should invalidate (affects both sides of the comparison).
#[tokio::test]
async fn test_correlated_scalar_where_aggregate() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT, location TEXT, budget INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO departments (id, name, location, budget) VALUES \
         (1, 'Engineering', 'NYC', 100), \
         (2, 'Marketing', 'NYC', 60), \
         (3, 'Sales', 'SF', 90), \
         (4, 'Support', 'SF', 50)",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Departments with budget above their location's average
    // NYC avg = 80: Engineering (100) above, Marketing (60) below
    // SF avg = 70: Sales (90) above, Support (50) below
    let query = "SELECT d.name FROM departments d \
                 WHERE d.budget > (SELECT avg(d2.budget) FROM departments d2 WHERE d2.location = d.location) \
                 ORDER BY d.name";

    // --- Cache miss ---
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // Engineering, Sales + RowDesc + CommandComplete
    assert_row_at(&res, 1, &[("name", "Engineering")])?;
    assert_row_at(&res, 2, &[("name", "Sales")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // --- Cache hit ---
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // --- CDC UPDATE: increase Marketing budget to shift NYC average ---
    // New NYC avg = (100 + 120) / 2 = 110, Engineering (100) now BELOW average
    ctx.origin_query(
        "UPDATE departments SET budget = 120 WHERE id = 2",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let res = ctx.simple_query(query).await?;
    // Now only Marketing (120 > 110) and Sales (90 > 70)
    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("name", "Marketing")])?;
    assert_row_at(&res, 2, &[("name", "Sales")])?;
    let _m = assert_cache_miss(&mut ctx, m).await?;

    Ok(())
}

// =============================================================================
// Scalar Subquery in WHERE Clause — Non-Aggregate Lookup
// =============================================================================

/// Test correlated scalar subquery in WHERE clause without aggregate.
///
/// Pattern: SELECT ... FROM employees e
///          WHERE e.name = (SELECT d.name FROM departments d WHERE d.id = e.dept_id)
///
/// Non-aggregate lookup in WHERE: finds employees whose name matches
/// their department name.
#[tokio::test]
async fn test_correlated_scalar_where_lookup() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO departments (id, name) VALUES (1, 'Alice'), (2, 'Marketing')",
        &[],
    )
    .await?;

    // Alice's name matches her department; Bob's does not
    ctx.query(
        "INSERT INTO employees (id, name, dept_id) VALUES \
         (10, 'Alice', 1), (11, 'Bob', 2)",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let query = "SELECT e.name FROM employees e \
                 WHERE e.name = (SELECT d.name FROM departments d WHERE d.id = e.dept_id) \
                 ORDER BY e.name";

    // --- Cache miss ---
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3); // Alice + RowDesc + CommandComplete
    assert_row_at(&res, 1, &[("name", "Alice")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // --- Cache hit ---
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 3);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // --- CDC UPDATE: rename department 2 to 'Bob' → Bob should now match ---
    ctx.origin_query("UPDATE departments SET name = 'Bob' WHERE id = 2", &[])
        .await?;

    wait_for_cdc().await;

    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4); // Alice, Bob
    assert_row_at(&res, 1, &[("name", "Alice")])?;
    assert_row_at(&res, 2, &[("name", "Bob")])?;
    let _m = assert_cache_miss(&mut ctx, m).await?;

    Ok(())
}

// =============================================================================
// Scalar Subquery — Mixed with EXISTS
// =============================================================================

/// Test a query with both a scalar subquery in SELECT and a correlated EXISTS in WHERE.
///
/// Both decorrelation types coexist in the same query: scalar → LEFT JOIN with
/// derived table, EXISTS → INNER JOIN + DISTINCT.
#[tokio::test]
async fn test_correlated_scalar_and_exists_combined() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, emp_id INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "CREATE TABLE projects (id INTEGER PRIMARY KEY, dept_id INTEGER)",
        &[],
    )
    .await?;

    ctx.query(
        "INSERT INTO employees (id, name, dept_id) VALUES \
         (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 10)",
        &[],
    )
    .await?;

    // Alice has 2 orders, Charlie has 1, Bob has 0
    ctx.query(
        "INSERT INTO orders (id, emp_id) VALUES (100, 1), (101, 1), (102, 3)",
        &[],
    )
    .await?;

    // Dept 10 has a project, dept 20 does not
    ctx.query(
        "INSERT INTO projects (id, dept_id) VALUES (200, 10)",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    // Employees in departments that have projects, with their order count
    let query = "SELECT e.name, \
                     (SELECT count(*) FROM orders o WHERE o.emp_id = e.id) AS order_count \
                 FROM employees e \
                 WHERE EXISTS (SELECT 1 FROM projects p WHERE p.dept_id = e.dept_id) \
                 ORDER BY e.name";

    // --- Cache miss ---
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(query).await?;
    // Dept 10: Alice (2 orders), Charlie (1 order). Bob excluded (dept 20 has no projects).
    assert_eq!(res.len(), 4); // 2 rows + RowDesc + CommandComplete
    assert_row_at(&res, 1, &[("name", "Alice"), ("order_count", "2")])?;
    assert_row_at(&res, 2, &[("name", "Charlie"), ("order_count", "1")])?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // --- Cache hit ---
    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 4);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // --- CDC INSERT project for dept 20 → Bob should appear ---
    ctx.origin_query(
        "INSERT INTO projects (id, dept_id) VALUES (201, 20)",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let res = ctx.simple_query(query).await?;
    assert_eq!(res.len(), 5); // Alice, Bob, Charlie
    assert_row_at(&res, 1, &[("name", "Alice"), ("order_count", "2")])?;
    assert_row_at(&res, 2, &[("name", "Bob"), ("order_count", "0")])?;
    assert_row_at(&res, 3, &[("name", "Charlie"), ("order_count", "1")])?;
    let _m = assert_cache_miss(&mut ctx, m).await?;

    Ok(())
}
