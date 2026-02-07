# QueryConstraints Analysis

## Background

`QueryConstraints` was implemented before subquery and union support was added. This document examines what is currently extracted, how it is used, and whether the structure is sufficient for subqueries, unions, and the planned outer join support.

## What Is Currently Extracted

`analyze_query_constraints()` in `constraints.rs` takes a `ResolvedSelectNode` and extracts three things. Constraints are stored per `UpdateQuery`, so each table's update query carries constraints derived from its own branch-specific resolved SELECT.

1. **Column constraints** — `column = literal` equalities from the WHERE clause and JOIN ON conditions
2. **Column equivalences** — `column = column` equalities from WHERE and JOIN ON conditions
3. **Table constraints** — column constraints reorganized by table name for CDC lookup

The extraction walks AND-connected equality comparisons only. It stops at OR, subqueries, functions, unary ops, IN, BETWEEN, ranges, IS NULL, etc. A fixpoint propagation step transitively pushes constants through equivalences:

```
WHERE a.id = 1 AND a.id = b.id  →  a.id = 1, b.id = 1
```

## How Constraints Are Used

Constraints serve one purpose: **CDC invalidation filtering** in `cache/writer/cdc.rs`.

### row_constraints_match()

Checks if a CDC row's column values match an update query's constraints for that row's table. If any constraint doesn't match, the row is irrelevant to this update query — skip invalidation.

### row_uncached_invalidation_check()

For rows not currently in cache:

- **Direct single-table queries**: Never invalidate for uncached rows (no join partners can change).
- **Direct multi-table queries**: If the table has WHERE-level constraints, use `row_constraints_match()` to decide. If no constraints exist, check whether all join columns are PK columns — if so and PK didn't change, the row's join membership is stable, skip invalidation.
- **Subquery tables**: Check `row_constraints_match()` first, then apply directional logic based on `SubqueryKind` (Inclusion/Exclusion/Scalar).

### row_cached_invalidation_check()

For UPDATE events where the row IS cached:

- Subquery tables: always invalidate (column changes could shift set membership).
- Direct tables: check if any join column changed AND `row_constraints_match()` returns true.

### table_join_columns()

Returns column names involved in join conditions for a given table. Used to determine if a column change affects join membership.

## Subqueries

### Current behavior

`analyze_equality_expr()` skips `ResolvedWhereExpr::Subquery` — inner subquery predicates don't leak into outer constraints. Outer-level equalities adjacent to subqueries are still extracted:

```sql
-- Extracts: users.tenant_id = 1
-- Does NOT extract: active.status = 'active'
SELECT * FROM users
WHERE id IN (SELECT id FROM active WHERE status = 'active')
  AND tenant_id = 1
```

This is correct and intentional. Subquery tables get their own `UpdateQuerySource::Subquery(kind)` with separate CDC semantics (Inclusion/Exclusion/Scalar), so inner constraints don't need to participate in the outer query's `QueryConstraints`.

### Inner table constraints

Since constraints are stored per `UpdateQuery`, each subquery inner table's update query carries constraints derived from the inner SELECT's WHERE clause. For example, `SELECT * FROM users WHERE id IN (SELECT user_id FROM active WHERE tenant_id = 5)` produces an update query for `active` with constraint `tenant_id = 5`. CDC events on `active` with a different `tenant_id` are correctly skipped.

### Is the current structure sufficient for subqueries?

**Yes.** Per-update-query constraints give each subquery table its own constraint set. The inner subquery path works through `SubqueryKind` directional logic with constraint filtering.

## UNION / Set Operations

### Current behavior

Since constraints are stored per `UpdateQuery`, and each UNION branch produces its own update query with a simple SELECT `resolved`, each branch gets its own constraints. For `SELECT * FROM t WHERE id = 1 UNION ALL SELECT * FROM t WHERE id = 2`, the table `t` gets two update queries — one with constraint `id = 1` and one with `id = 2`. A CDC row matches if it matches any branch's constraints, which is the correct behavior.

### Is the current structure sufficient for unions?

**Yes.** Per-update-query constraints give each UNION branch its own constraint set, providing accurate filtering.

## LEFT / RIGHT Joins

See also: `docs/left-right-join-support.md` for the full design.

### Terminal optional-side tables (planned first step)

**No constraint changes needed.** Propagating `a.id = 1` through `a.id = b.id` to `b.id = 1` is valid even for LEFT JOIN:

- The constraint is only used to filter CDC events on `b`
- If `b.id != 1`, the row cannot match the join condition regardless of join type
- Skipping it is correct whether the join is INNER or LEFT

Equivalences and `table_join_columns()` work identically for outer joins — the join condition structure is the same, only the semantics of non-matching rows differ (NULL-padding vs exclusion), which is handled at retrieval time by the cache database's LEFT JOIN execution.

### Non-terminal optional-side tables (future work)

Propagation becomes semantically questionable. For a non-terminal LEFT JOIN:

```sql
SELECT * FROM a
  LEFT JOIN b ON a.id = b.id
  JOIN c ON b.val = c.val
WHERE a.id = 1
```

Propagating `a.id = 1` → `b.id = 1` is still safe for CDC filtering on `b`. But propagating further through `b.val = c.val` could be wrong because `b.val` might be NULL when there's no match.

However, the design doc plans conservative re-population for non-terminal cases, sidestepping this issue entirely. The constraint system doesn't need to handle it.

### Is the current structure sufficient for outer joins?

**Yes**, for the terminal case. No changes needed — the existing propagation, equivalences, and `table_join_columns()` all work correctly.

For the non-terminal case (future), constraints would not be the mechanism used (conservative re-population instead), so no structural changes are needed.

## Other Observations

### table_constraints keyed by name only

`table_constraints` is `HashMap<String, Vec<(String, LiteralValue)>>` keyed by unqualified table name. The underlying `ResolvedColumnNode` carries schema, but this is lost at the `table_constraints` level. If two tables in different schemas share a name, their constraints would collide. Worth fixing if cross-schema queries become common.

### Self-join deduplication

Self-joins (same table, different aliases) deduplicate to a single constraint because `ResolvedColumnNode::PartialEq` excludes `table_alias`. This means `SELECT * FROM t a JOIN t b ON a.id = b.id WHERE a.id = 1` produces one constraint (`t.id = 1`), not two. This is correct behavior for CDC purposes — the table only has one set of rows regardless of how many times it's aliased.

### Single equality join condition restriction

The cacheability gate only accepts single-equality join conditions (`ON a.id = b.id`). Multi-condition joins like `ON a.x = b.x AND a.y = b.y` are rejected at cacheability, so the constraint system never sees them. If multi-condition joins are supported in the future, `analyze_equality_expr` already handles AND-connected equalities and would extract both equivalences without changes.

### Conflict detection

The propagation TODO at `constraints.rs:184` notes that conflicting constraints (`a = 1 AND a = 2`) are not detected. Such queries produce empty results but the system does not optimize for this — it would process CDC events normally. Low priority since these queries are rare in practice.

## Summary

| Feature | Constraints Sufficient? | Notes |
|---------|------------------------|-------|
| Single SELECT + WHERE | Yes | Core use case, well tested |
| INNER JOINs | Yes | Equivalences + propagation work correctly |
| Subqueries (outer constraints) | Yes | Inner predicates correctly skipped |
| Subqueries (inner constraints) | Yes | Per-update-query constraints extract inner predicates |
| UNION / set operations | Yes | Per-update-query constraints give each branch its own set |
| LEFT JOIN (terminal) | Yes | No changes needed |
| LEFT JOIN (non-terminal) | N/A | Conservative re-population planned instead |
| Multi-condition joins | N/A | Currently rejected at cacheability gate |
