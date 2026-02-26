# ADR-015: Scalar Correlated Subquery Decorrelation

## Status
Accepted

## Context
ADR-014 introduced decorrelation for EXISTS/NOT EXISTS correlated subqueries. Scalar correlated subqueries — those returning a single value in the SELECT list or WHERE clause — remained unsupported. SELECT-list scalars were rejected outright; WHERE-clause scalars silently passed through without proper branch extraction (a latent bug where the inner branch couldn't run standalone).

Scalar subqueries are common in analytics queries: `SELECT e.id, (SELECT COUNT(*) FROM orders o WHERE o.emp_id = e.id) FROM employees e` or `SELECT * FROM employees WHERE salary > (SELECT AVG(salary) FROM employees e2 WHERE e2.dept_id = employees.dept_id)`. Without decorrelation, inner tables are invisible to CDC and population.

Unlike EXISTS/NOT EXISTS which are boolean tests, scalar subqueries return a computed value (often an aggregate). They can't be flattened into a simple JOIN — the inner query must be wrapped in a derived table that preserves the scalar result while exposing its tables at the outer scope.

## Decision
Decorrelate scalar correlated subqueries into LEFT JOIN with a derived table. The derived table contains the correlation key columns plus the scalar expression (aliased `_dsN`), with GROUP BY on the correlation key when the expression contains an aggregate. The subquery reference is replaced with a column reference to the derived table's scalar column.

- **LEFT JOIN preserves outer rows**: Unlike EXISTS (INNER JOIN), scalar subqueries should not filter — an employee with no orders should still appear. LEFT JOIN ensures all outer rows survive
- **Aggregate detection via `pg_proc`**: Whether the derived table needs GROUP BY depends on whether the scalar expression contains an aggregate. Aggregate functions are loaded from `pg_proc WHERE prokind = 'a'` at startup into a `HashSet`, matching the existing pattern for function volatility classification
- **Non-aggregate subqueries skip GROUP BY**: Simple lookups don't need deduplication. Potential row multiplication from non-unique correlation keys is harmless since the decorrelated form is transient
- **Unique aliases via `DecorrelateState`**: Counter-based aliases (`_dc1`, `_dc2` for derived tables; `_ds1`, `_ds2` for scalar columns) ensure uniqueness across multiple decorrelations and SetOp branches
- **Both SELECT list and WHERE clause**: `select_columns_decorrelate()` handles SELECT-list scalars; `conjunct_scalar_decorrelate()` recursively walks WHERE expressions to find scalar subqueries embedded in binary/unary expressions (e.g., `salary > (SELECT ...)`)

Three resolved forms are involved, consistent with EXISTS/NOT EXISTS: the original is preserved for serving (PostgreSQL evaluates `COUNT(*)` correctly, returning 0 not NULL), the decorrelated form drives population and update query generation, and update queries are stored for CDC. The NULL-vs-0 difference from LEFT JOIN never matters because the decorrelated form is never executed.

## Rationale
**Completes correlated subquery support**: Together with ADR-014, all common correlated subquery patterns (EXISTS, NOT EXISTS, scalar) are now cacheable. Only IN/ALL remain unsupported.

**Fixes a latent bug**: WHERE-clause scalar subqueries previously passed through silently, producing inner branches that couldn't run standalone. Decorrelation ensures proper branch extraction.

**Catalog-based aggregate detection**: Hardcoding aggregate names would miss user-defined aggregates. Querying `pg_proc` matches the existing function volatility pattern and handles all aggregate types.

**Derived table pattern is standard**: Wrapping a correlated scalar into a derived table with GROUP BY on correlation keys is the standard relational approach, producing exactly one row per key value.

## Consequences

### Positive
- Scalar correlated subqueries in SELECT list and WHERE clause are now cacheable
- Fixes silent pass-through bug for WHERE-clause scalar subqueries
- Multiple scalar subqueries in the same query produce correctly chained LEFT JOINs with unique aliases
- Inner tables get `Subquery(SubqueryKind::Scalar)` source — any CDC change triggers invalidation
- Works transparently with SetOp queries

### Negative
- Requires a startup query to `pg_proc` for aggregate function detection (minimal cost, similar queries already made)
- Only equality correlation predicates are supported
- Correlated scalar subqueries in HAVING are rejected
- Non-aggregate subqueries with LIMIT are rejected (can't safely strip LIMIT without aggregate deduplication)

## Implementation Notes
`scalar_inner_prepare()` validates the inner query (single output column, has WHERE, rejects non-aggregate with LIMIT). `subquery_scalar_decorrelate()` builds the derived table, JOIN condition, and scalar column reference. `DecorrelateState` threads mutable alias counters through all decorrelation functions, shared with EXISTS/NOT EXISTS decorrelation to ensure globally unique aliases.
