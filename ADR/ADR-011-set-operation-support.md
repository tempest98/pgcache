# ADR-011: Set Operation Support (UNION/INTERSECT/EXCEPT)

## Status
Accepted

## Context
pgcache needed to support SQL set operations (UNION, INTERSECT, EXCEPT, and their ALL variants) for caching. Set operations combine results from multiple SELECT statements, which introduces complexity:

1. **Multiple table sources**: A UNION query like `SELECT id FROM users WHERE tenant_id = 1 UNION SELECT id FROM admins WHERE tenant_id = 1` references two different tables that must each be populated and tracked for CDC invalidation.

2. **ORDER BY resolution**: PostgreSQL resolves ORDER BY in set operations against the first SELECT's output column names (unqualified), not against table columns like in simple SELECT queries.

3. **Cache population and CDC**: Each branch of a set operation may reference different tables with different columns, requiring branch-specific processing rather than treating the query as a monolithic unit.

## Decision
Process set operations by extracting and handling each SELECT branch independently:

- Add `select_branches()` method to both AST `QueryExpr` and `ResolvedQueryExpr` to extract all SELECT nodes from a query tree
- During cache population, iterate through branches and populate each table independently using the branch's WHERE clause
- For CDC updates, generate branch-specific update queries rather than preserving the UNION structure
- Handle ORDER BY in set operations by converting column references to unqualified identifiers (new `ResolvedColumnExpr::Identifier` variant)

## Rationale
**Branch-based processing matches PostgreSQL semantics**: Each SELECT in a set operation is evaluated independently, then combined. Processing branches separately mirrors this behavior and ensures correct table filtering.

**Enables correct CDC handling**: When a row changes in the `users` table, the branch referencing `users` is checked. If a branch requires invalidation, the entire parent query is invalidated since set operation results cannot be incrementally recomputed.

**Simplifies update query generation**: Branch-specific update queries avoid the complexity of preserving UNION structure in predicate-modified queries.

**Reuses existing infrastructure**: The branch-based approach leverages existing single-SELECT processing code paths, minimizing new code and risk.

## Consequences

### Positive
- UNION, INTERSECT, EXCEPT, and their ALL variants are now cacheable
- CDC correctly updates cached results when underlying tables change
- Consistent with how PostgreSQL evaluates set operations

### Negative
- Additional AST traversal overhead to extract branches (minimal in practice)
- `ResolvedColumnExpr::Identifier` variant adds a case to handle in various match arms
- Set operation queries with many branches will have proportionally more cache table entries
- If any branch triggers invalidation, the entire query is invalidated

## Implementation Notes
The `select_branches()` methods recursively traverse the query tree, collecting SELECT nodes from both sides of set operations. The `ResolvedColumnExpr::Identifier` variant deparses without table qualification, which PostgreSQL requires for set operation ORDER BY clauses.
