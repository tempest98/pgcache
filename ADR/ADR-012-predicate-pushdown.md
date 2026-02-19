# ADR-012: Predicate Pushdown into FROM Subqueries

## Status
Accepted

## Context
When pgcache caches a query like `SELECT * FROM (SELECT ... UNION SELECT ...) sub WHERE sub.a < 100`, the outer WHERE predicate is evaluated after the entire subquery materializes. This means all rows from both UNION branches are fetched from origin during cache population and stored in cache, even though the predicate will filter most of them out. For UNION subqueries with large base tables, this is expensive both at population time and at serve time.

The transform needed to operate at the resolved level (post-resolution), where column references are fully qualified and SELECT * is expanded, so that column remapping across UNION branches could be done by position.

An alternative was operating at the AST level (pre-resolution), but resolved types have fully qualified column metadata that makes position-based remapping straightforward and unambiguous.

## Decision
Apply predicate pushdown as a resolved-level transform between resolution and all downstream consumers (population, CDC update queries, worker serve):

- Split outer WHERE into AND-conjuncts and evaluate each independently for pushdown eligibility
- A conjunct is pushable if all column references target the subquery alias, it contains no subquery expressions, and the target branch is safe (no GROUP BY, HAVING, or window functions)
- Column remapping uses position: outer column name maps to a position in the subquery's output column list, then to the corresponding column expression in each branch's SELECT list
- Only columns actually referenced by the predicate need to map to simple column expressions; non-referenced positions (literals, functions, etc.) are ignored
- For set operations, the predicate is pushed into both branches recursively; if either branch rejects, the conjunct stays in the outer WHERE
- The entry point also recurses into top-level SetOp branches, so nested structures like `(SELECT ... FROM (subquery) WHERE ...) UNION ALL (SELECT ... FROM (subquery) WHERE ...)` benefit from pushdown at each level

## Rationale
**Reduces rows fetched from origin**: Pushed predicates are included in the SQL sent to origin during population, so the database applies index scans and filtering before returning rows.

**Reduces cache storage and scan time**: Fewer rows cached means less memory and faster serve-time scans for cache hits.

**Single integration point**: Inserting the transform after resolution means population, CDC update queries, and worker serve all automatically benefit without changes.

**Position-based remapping handles column aliasing**: UNION branches can have different column names (e.g., `t1.a` vs `t2.c`). Remapping by position in the output column list handles this correctly.

**Conjunct-level granularity**: Each AND-conjunct is evaluated independently, so partial pushdown works — pushable conjuncts are pushed, others remain in the outer WHERE.

## Consequences

### Positive
- Significant performance improvement for queries with outer WHERE on UNION/subquery FROM sources, especially with selective predicates like date ranges
- Works transparently with existing cache pipeline — no changes needed to population, CDC, or worker code
- Handles column aliasing across UNION branches correctly via position-based remapping
- Gracefully falls back to no-op for queries that don't match the pattern

### Negative
- Only pushes into a single FROM subquery source (multiple FROM sources won't benefit, though these don't arise for cached queries today)
- Predicates referencing columns that map to non-column expressions (computed columns, literals) in any branch cannot be pushed
- Clones the branch SelectNode when pushing (acceptable since this runs once at registration, not per query)
