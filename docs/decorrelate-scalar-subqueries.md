# Scalar Correlated Subquery Decorrelation

## Context

pgcache decorrelates correlated subqueries for CDC invalidation (update queries need flat JOINs with all tables visible). EXISTS/NOT EXISTS decorrelation is implemented. Scalar subqueries are currently:

- **SELECT list**: Rejected in `select_node_decorrelate()` at the `column_expr_has_correlation()` check (`"correlated subquery in SELECT list"`)
- **WHERE clause**: Silently pass through via the catch-all arm in `select_node_decorrelate()`'s conjunct loop (latent bug — inner branch extracted for population can't run standalone)

This change makes both positions decorrelatable, enabling caching of queries like:
```sql
SELECT e.id, (SELECT COUNT(*) FROM orders o WHERE o.emp_id = e.id) FROM employees e
SELECT * FROM employees WHERE salary > (SELECT AVG(salary) FROM employees e2 WHERE e2.dept_id = employees.dept_id)
```

## Design: Transient Decorrelation at Registration Time

Scalar subquery decorrelation uses LEFT JOIN + derived table, consistent with how
EXISTS/NOT EXISTS decorrelation already works. Three resolved forms are involved:

1. **Original resolved** (`CachedQuery.resolved`) — stored permanently, used by the worker
   to serve cached results. Contains the correlated subquery as-is, so PostgreSQL evaluates
   it correctly against the cache database (e.g., `COUNT(*)` returns 0 not NULL).
2. **Decorrelated resolved** — transient, produced by `query_expr_decorrelate()` at
   registration time. Used to extract population branches and generate update queries,
   then discarded.
3. **Update queries** — derived from the decorrelated form via `query_table_update_queries()`,
   stored in `Cache.update_queries` for CDC invalidation.

### Why population works with the decorrelated form

For `SELECT e.id, (SELECT COUNT(*) FROM orders o WHERE o.emp_id = e.id) FROM employees e`,
decorrelated to:
```sql
SELECT e.id, _dc1._ds1
FROM employees e
LEFT JOIN (SELECT emp_id, COUNT(*) AS _ds1 FROM orders GROUP BY emp_id) _dc1
  ON _dc1.emp_id = e.id
```

`select_nodes()` extracts two branches:

1. **Outer branch** — `direct_table_nodes()` returns `employees` (derived table subquery skipped).
   Population replaces SELECT with employee columns → all employee rows cached.
2. **Derived table inner branch** — `SELECT emp_id, COUNT(*) FROM orders GROUP BY emp_id`.
   `direct_table_nodes()` returns `orders`. `resolved_select_node_replace` replaces SELECT
   with order columns **and strips GROUP BY** → `SELECT DISTINCT orders.* FROM orders`.
   All order rows cached.

### NULL vs 0 correctness

The NULL vs 0 difference (LEFT JOIN produces NULL where `COUNT(*)` returns 0) never
arises because:

- **Serving** uses the original resolved form with the correlated subquery — PostgreSQL
  evaluates `COUNT(*)` correctly against the cache database
- **Population** stores raw table rows — aggregates are stripped by `resolved_select_node_replace`
- The decorrelated form (where NULL would replace 0) is never executed as-is

### Fit with existing `query_register` flow

`query_register` already follows this pattern: decorrelation produces a transient result
used for branch extraction and update query generation, while the original resolved form
is stored in `CachedQuery.resolved` for serving. Scalar decorrelation slots into this
existing flow without changes to `writer/query.rs`.

## Implementation

### Phase 1: Scalar Decorrelation Infrastructure

**File**: `decorrelate.rs`

**1a. `DecorrelateState`** — counter for unique derived table aliases (`_dc1`, `_dc2`...) and scalar column aliases (`_ds1`, `_ds2`...).

**1b. Aggregate function set from catalog** — load aggregate function names from `pg_proc` at startup (similar to `function_volatility_map_load()` which queries `pg_proc` for scalar function volatility). Query `pg_proc WHERE prokind = 'a'` to build a `HashSet<String>` of aggregate function names. This set is passed through `DecorrelateState` and used by `column_expr_has_aggregate()` to decide whether to add GROUP BY to the derived table.

**1c. `column_expr_has_aggregate()`** — check if a column expression contains any function call whose name is in the aggregate function set. Used to decide whether to add GROUP BY to the derived table.

**1d. `scalar_inner_prepare()`** — validate and clean inner query:
- Must be simple SELECT with exactly one output column
- Must have WHERE clause (needed for correlation extraction)
- Strip LIMIT and ORDER BY (irrelevant for derived table)
- **Reject**: non-aggregate inner query WITH LIMIT (can't safely strip LIMIT without aggregate deduplication)

**1e. `subquery_scalar_decorrelate()`** — core function, builds derived table + LEFT JOIN:

Input: inner query, outer_refs, state
Output: `(ResolvedTableSource, ResolvedWhereExpr, ResolvedColumnNode)` — derived table, join condition, scalar column ref

Steps:
1. Call `scalar_inner_prepare()`
2. Call `where_clause_correlation_partition()` (reuse existing)
3. Build derived table SELECT: correlation key columns + scalar expression (aliased `_dsN`)
4. If inner has aggregate (checked via catalog set) → GROUP BY on correlation key columns; otherwise skip
5. WHERE: residual only (correlation predicates removed)
6. Wrap as `ResolvedTableSource::Subquery(ResolvedTableSubqueryNode { query, alias: "_dcN" })`
7. Build JOIN ON condition: `_dcN.inner_col = outer_col` for each correlation predicate
8. Build scalar column ref: `ResolvedColumnNode { table: "_dcN", table_alias: Some("_dcN"), column: "_dsN", column_metadata: synthetic TEXT }`

JOIN ON references use synthetic `ResolvedColumnNode` with the derived table alias for inner columns:
```rust
ResolvedColumnNode {
    schema: "".into(),
    table: derived_alias.into(),
    table_alias: Some(derived_alias.into()),
    column: predicate.inner_column.column.clone(),
    column_metadata: predicate.inner_column.column_metadata.clone(),
}
```

For non-aggregate inner queries, the derived table LEFT JOIN could theoretically produce multiple rows per outer row if the correlation key isn't unique. This is harmless — the decorrelated form is transient (never executed), so row multiplication doesn't affect population (raw table rows) or update queries (table references only).

### Phase 2: SELECT List Decorrelation

**File**: `decorrelate.rs` — modify `select_node_decorrelate()`

Replace the `column_expr_has_correlation()` rejection block with decorrelation. For each correlated `ResolvedColumnExpr::Subquery(query, outer_refs)` in SELECT columns:

1. Call `subquery_scalar_decorrelate(inner_query, outer_refs, state)`
2. Replace column expr with `ResolvedColumnExpr::Column(scalar_ref)`
3. LEFT JOIN derived table to `current_select.from`
4. Set `transformed = true`
5. If decorrelation fails → return `Err(NonDecorrelatable)`

Multiple scalar subqueries in the same SELECT produce chained LEFT JOINs.

### Phase 3: WHERE Clause Decorrelation

**File**: `decorrelate.rs`

**3a. `conjunct_scalar_decorrelate()`** — recursively walk a WHERE expression, find `Subquery { sublink_type: Expr, outer_refs (non-empty) }`, decorrelate each, replace with `Column(scalar_ref)`, add LEFT JOINs to `select.from`.

Pattern match:
- `Subquery { Expr, non-empty outer_refs }` → decorrelate, replace with Column
- `Binary` → recurse into both sides
- `Unary` → recurse into inner
- Everything else → return unchanged

**3b. Narrow the correlated non-EXISTS rejection arm** — the existing arm that rejects `Subquery { outer_refs non-empty }` with `"correlated non-EXISTS subquery (IN/ALL/scalar)"` should be narrowed to only reject `IN` (`SubLinkType::Any`) and `ALL` (`SubLinkType::All`). Scalar subqueries (`SubLinkType::Expr`) should fall through to the catch-all for decorrelation.

**3c. Integration in conjunct loop** — in `select_node_decorrelate()`, modify the catch-all arm:

```rust
_ => {
    if where_expr_has_correlation(&conjunct) {
        // Try scalar decorrelation for embedded subqueries
        let (new_conjunct, was_transformed) =
            conjunct_scalar_decorrelate(&conjunct, &mut current_select, state)?;
        remaining_conjuncts.push(new_conjunct);
        transformed |= was_transformed;
    } else {
        remaining_conjuncts.push(conjunct);
    }
}
```

This replaces the current catch-all which silently passed correlated WHERE scalar subqueries through.

**Note**: Correlated subqueries in HAVING remain rejected — out of scope for this change.

### Phase 4: Thread `DecorrelateState`

- `query_expr_decorrelate()` creates `DecorrelateState::new()` and calls `query_expr_decorrelate_inner()`
- `select_node_decorrelate()` accepts `&mut DecorrelateState`
- `subquery_scalar_decorrelate()` accepts `&mut DecorrelateState`
- `conjunct_scalar_decorrelate()` accepts `&mut DecorrelateState`

### Tests

**SELECT list** (decorrelate.rs):
- COUNT(*) with single correlation predicate
- AVG aggregate with residual WHERE predicates
- Multiple correlation predicates
- Multiple scalar subqueries in same SELECT
- Non-aggregate lookup (no GROUP BY in derived table)
- Non-aggregate with LIMIT → rejected
- Mixed: scalar + EXISTS in same query
- Update existing `test_correlated_scalar_subquery_rejected` → now succeeds

**WHERE clause** (decorrelate.rs):
- `salary > (SELECT AVG(salary) WHERE dept = e.dept)`
- `id = (SELECT MAX(id) WHERE ...)`
- Multiple scalar subqueries in different Binary exprs

**Self-join / alias handling** (decorrelate.rs):
- Same table in inner and outer with different aliases: `SELECT e.id, (SELECT COUNT(*) FROM employees e2 WHERE e2.manager_id = e.id) FROM employees e`

**SetOp with scalar subqueries** (decorrelate.rs):
- UNION ALL where both branches have scalar subqueries — verifies `DecorrelateState` counters produce unique aliases across branches (`_dc1`/`_ds1` in left, `_dc2`/`_ds2` in right)

### Verification

```bash
cd pgcache && cargo test --lib
cd pgcache && cargo clippy -- -D warnings
```
