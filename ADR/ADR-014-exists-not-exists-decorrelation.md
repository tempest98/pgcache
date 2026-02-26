# ADR-014: EXISTS/NOT EXISTS Correlated Subquery Decorrelation

## Status
Accepted

## Context
pgcache needs visibility into all tables referenced by a cached query so CDC events on any table can trigger cache invalidation. Correlated subqueries hide inner tables inside a separate scope — `SELECT * FROM employees e WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)` references both `employees` and `orders`, but `orders` is invisible to the outer query's table iteration. Without decorrelation, pgcache would either miss CDC events on inner tables (stale cache) or reject these queries as uncacheable.

The standard approach is decorrelation: rewriting correlated subqueries into equivalent JOIN forms where all tables appear at the same scope level. EXISTS and NOT EXISTS have well-known JOIN equivalences (semi-join and anti-join), but the rewriting must handle DISTINCT semantics, residual predicate placement, and the interaction with `UpdateQuerySource` classification for CDC.

An alternative was to walk the subquery tree to extract table references without rewriting, but this would not produce the flat query structure needed by `query_table_update_queries()` to generate per-table update queries with correct source classification.

## Decision
Decorrelate correlated EXISTS into INNER JOIN + DISTINCT and NOT EXISTS into LEFT JOIN + IS NULL. Decorrelation is transient — it runs at registration time to extract population branches and generate update queries, then the decorrelated form is discarded. The original resolved AST is preserved in `CachedQuery.resolved` for serving.

- **EXISTS → INNER JOIN + DISTINCT**: Correlation predicates become the JOIN ON condition. Residual inner predicates merge into the outer WHERE. DISTINCT preserves semi-join semantics (multiple matching inner rows must not duplicate outer rows). GROUP BY, HAVING, and LIMIT on the inner query are stripped — EXISTS is a boolean check, so these don't change the answer, and stripping produces conservative (over-invalidation) CDC behavior
- **NOT EXISTS → LEFT JOIN + IS NULL**: Both correlation and residual predicates go into the ON clause (not outer WHERE) to preserve LEFT JOIN semantics. An IS NULL check on an inner correlation column is added to the outer WHERE for anti-join filtering. NOT EXISTS with GROUP BY or HAVING is rejected — the anti-join pattern would under-invalidate because it tests for zero matching rows, while the original tests for no rows surviving the filter
- Only equality correlation predicates are supported (e.g., `o.emp_id = e.id`). Non-equality correlations and correlated subqueries inside OR expressions are rejected as non-decorrelatable
- The resulting JOIN types feed directly into `UpdateQuerySource` classification: INNER JOIN tables get `FromClause`, LEFT JOIN optional-side tables get `OuterJoinOptional` or `OuterJoinTerminal` via the existing `outer_join_optional_tables()` analysis

## Rationale
**Enables caching of a common query pattern**: EXISTS/NOT EXISTS subqueries are widely used for membership checks and filtering. Rejecting them would leave a significant class of queries uncacheable.

**Transient transformation preserves correctness**: The original resolved form with the correlated subquery intact is used for serving — PostgreSQL evaluates it correctly against the cache database. The decorrelated form only drives population and CDC, then is discarded.

**CDC source classification falls out naturally**: The JOIN types produced by decorrelation map directly to existing `UpdateQuerySource` variants without special-casing. EXISTS inner tables get `FromClause` (DELETE can only shrink the result), NOT EXISTS inner tables get `OuterJoinOptional`/`OuterJoinTerminal` (changes can expand the result, requiring invalidation).

**Standard relational algebra**: Semi-join and anti-join are well-established transformations with well-understood correctness properties.

## Consequences

### Positive
- Correlated EXISTS and NOT EXISTS queries are now cacheable
- CDC correctly tracks inner tables and invalidates on changes
- Population extracts all referenced tables across outer and inner scopes
- Existing `UpdateQuerySource` classification handles both patterns without modification

### Negative
- DISTINCT for EXISTS adds deduplication overhead at serve time even when unnecessary
- NOT EXISTS with GROUP BY or HAVING is rejected
- Only equality correlation predicates are supported
- Correlated subqueries inside OR are rejected (AND-conjunct splitting cannot isolate them)

## Implementation Notes
`where_clause_correlation_partition()` splits inner WHERE into equality predicates matching `outer_refs` (correlation) and residual predicates. Inner FROM sources are combined via cross-join chaining when multiple tables are present. `correlated_exists_inner_prepare()` strips GROUP BY/HAVING/LIMIT for EXISTS; `correlated_not_exists_inner_prepare()` strips only LIMIT and rejects GROUP BY/HAVING.
