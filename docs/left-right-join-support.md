# LEFT and RIGHT JOIN Support

## Status: Implemented

## Background

pgcache previously supported only INNER JOINs for cached queries. The cacheability gate in `cache/query.rs` (`is_supported_join`) rejected any join where `join_type != JoinType::Inner`. LEFT and RIGHT JOINs are common in application queries, and rejecting them forced those queries through the pass-through path to origin on every execution.

The parsing and resolution layers already handled all join types. The AST (`JoinType::Left`, `JoinType::Right`), parser, and resolver all accept and represent outer joins. `ResolvedJoinNode::deparse` correctly emits `LEFT JOIN` / `RIGHT JOIN` syntax.

## How the Cache Works

pgcache stores per-table data, not materialized join results. Each origin table referenced by a cached query gets its own cache table mirroring the origin schema. When serving a cached query, pgcache deparses the resolved query and executes it against the cache database, running the original JOINs against the cached per-table data.

This architecture naturally supports outer join retrieval — PostgreSQL handles LEFT/RIGHT JOIN semantics when executing against the cache tables. The challenge is keeping the per-table cache data correct under CDC events.

## CDC Semantics: INNER vs Outer

With INNER JOINs, a row either participates in the result or doesn't. CDC handling is symmetric.

Outer joins introduce asymmetry between the **preserved side** (always contributes rows) and the **optional side** (contributes data when matched, NULLs otherwise):

- **INSERT on the optional side**: Adds the row to the optional-side cache table. The LEFT JOIN at retrieval fills in what was previously NULL-padded. The preserved-side result set is unchanged.

- **DELETE on the optional side**: Removes the row from the optional-side cache table. The LEFT JOIN at retrieval produces NULL-padded columns for the preserved-side row.

- **UPDATE on the optional side**: Combination of the above. If join columns change, the row may start or stop matching, handled by the existing upsert-then-delete-stale mechanism.

These operations are safe **when the optional-side table is terminal** — CDC can handle them in place without query invalidation.

## The Chained Join Problem

When the optional-side table feeds into downstream joins or predicates, changes on the optional side can alter which rows from other tables should be in cache.

### Downstream join dependency

```sql
SELECT * FROM a
  LEFT JOIN b ON a.id = b.a_id
  JOIN c ON b.val = c.val
```

Before a B row is inserted: B is NULL-padded, `b.val` is NULL, the INNER JOIN with C matches nothing for that A row. After: `b.val` has a real value, the JOIN with C could now match — but the relevant C rows were never populated into cache because that join path was inactive at population time.

### WHERE clause dependency

```sql
SELECT * FROM a
  LEFT JOIN b ON a.id = b.a_id
WHERE b.status = 'active'
```

Before B insert: `b.status` is NULL, WHERE filters it out. After: it might match, but the cache wasn't populated with that expectation.

### Chained outer join dependency

```sql
SELECT * FROM a
  LEFT JOIN b ON a.id = b.a_id
  LEFT JOIN c ON b.x = c.x
```

Before B insert: `(A, NULL, NULL)`. After: should be `(A, B, C)` if a matching C row exists, but cache only has `(A, B, NULL)` because C was never fetched for that join path.

## Terminal Table Definition

An optional-side table is **terminal** when its columns do not appear in:

1. Any other join condition (not its own ON clause)
2. The WHERE clause

GROUP BY, HAVING, and the SELECT list are excluded from the check. Population queries strip GROUP BY and HAVING — the row set before aggregation is stored, and these clauses are re-evaluated at retrieval time against cached rows. Similarly, SELECT expressions don't affect result set membership.

If a table is terminal, changes to it cannot cascade into affecting other tables' membership in the result set.

## Implementation

### Approach

All LEFT and RIGHT JOINs are cacheable. FULL OUTER JOINs are rejected. The terminal vs non-terminal distinction determines CDC behavior, not cacheability:

- **Terminal** optional-side tables (`OuterJoinTerminal`): CDC INSERT/DELETE handled in place — the preserved side already has the row, changes here only affect which values fill the NULL-padded columns
- **Non-terminal** optional-side tables (`OuterJoinOptional`): CDC events trigger full query invalidation (conservative but correct)
- **Preserved side**: Remains `Direct` — INSERT triggers invalidation because optional-side rows matching the new preserved-side row may not be in cache

### AST deparse fix

`JoinNode::deparse` in `ast.rs` was hardcoding `" JOIN"` without the type qualifier. Fixed to emit `" LEFT JOIN"` / `" RIGHT JOIN"` / `" FULL JOIN"` to match the resolved-level deparse. This is used by `query_table_update_queries` in `transform.rs` for generating CDC check queries.

### Cacheability gate

`is_supported_join` in `cache/query.rs` changed from rejecting all non-Inner joins to only rejecting Full:

```rust
if join.join_type == JoinType::Full {
    return Err(CacheabilityError::UnsupportedFrom);
}
```

### AND of equalities in join conditions

Previously only single equality join conditions were accepted. An `ON a.id = b.id AND a.tenant = b.tenant` condition parsed as a `Binary { op: And, ... }` which was rejected. Added a recursive helper `join_condition_is_valid` that accepts equalities and AND-of-equalities.

### Terminality analysis

Added `outer_join_optional_tables(select: &ResolvedSelectNode) -> (HashSet<String>, HashSet<String>)` in `cache/query.rs`. Returns `(terminal, non_terminal)` sets. Operates on the resolved AST where `ResolvedColumnNode.table` always carries the real table name (not alias), eliminating alias ambiguity. Called during update query generation, not during cacheability checking.

Two-pass algorithm:

**Pass 1** — Collect real table names from `ResolvedColumnNode` references in the WHERE clause into a `non_terminal_refs` set.

**Pass 2** — Walk the resolved join tree top-down via `resolved_join_terminality_walk`, threading `non_terminal_refs` (which accumulates ancestor join condition refs at each level):
- At each outer join, collect ALL optional-side table names into `all_optional`
- Check if any optional-side table appears in `non_terminal_refs` — if so, add to `non_terminal`
- Before recursing into children, merge current join condition's column table refs into `non_terminal_refs`
- After the walk: `terminal = all_optional - non_terminal`

The current join's own ON condition is NOT in `non_terminal_refs` at check time (merged only for children), correctly excluding a join's own condition from making it non-terminal.

Helpers:
- `resolved_column_table_refs_collect(expr: &ResolvedWhereExpr, ...)` — walks the expression tree collecting real table names from `ResolvedColumnNode`s
- `resolved_source_table_names_collect(source: &ResolvedTableSource, ...)` — recursively collects real table names from `ResolvedTableNode`s

### Update query source tagging

`UpdateQuerySource` in `cache/types.rs`:

```rust
pub enum UpdateQuerySource {
    Direct,
    Subquery(SubqueryKind),
    OuterJoinTerminal,
    OuterJoinOptional,
}
```

`query_table_update_queries` in `transform.rs` takes the `ResolvedQueryExpr` as an additional parameter. Resolved branches are paired with AST branches by position. For each table, the real table name is checked against the terminal and non-terminal sets:
- In `non_terminal` → `OuterJoinOptional`
- In `terminal` → `OuterJoinTerminal`
- Neither → original source (`Direct` or `Subquery`)

### CDC invalidation

In `cache/writer/cdc.rs`:

**`row_uncached_invalidation_check`** (row not currently in cache):
- `OuterJoinTerminal`: Returns `false` — no invalidation needed. The preserved side already has the row; changes on the terminal optional side only affect NULL-padded columns. The row is added to cache in place via update query execution.
- `OuterJoinOptional`: Uses constraint matching — if the row matches constraints, invalidate the query.

**`row_cached_invalidation_check`** (row currently in cache):
- `OuterJoinTerminal`: Falls through to `Direct` logic — checks join column changes.
- `OuterJoinOptional`: Always returns `true` — any change to a cached non-terminal optional-side row could cascade.

After invalidation, the query is removed from `cached_queries`. The next client request for the same query triggers re-registration and re-population.

### Population: NULL-padded row skipping

LEFT/RIGHT JOINs produce NULL-padded rows for the optional side when there's no matching row. During population, these phantom rows have NULL primary keys. `population_stream` in `cache/writer/population.rs` detects rows where any primary key column is NULL and skips them — a NULL PK indicates the row doesn't exist on the optional side.

### Constraint propagation

`constraints.rs` propagates equality constraints through join equivalences uniformly. For both terminal and non-terminal cases, propagation in both directions is safe. No changes were needed.

## Files Changed

| File | Changes |
|------|---------|
| `src/query/ast.rs` | Fix `JoinNode::deparse` to emit join type qualifier |
| `src/cache/query.rs` | Allow Left/Right in `is_supported_join`, AND-of-equalities via `join_condition_is_valid`, `outer_join_optional_tables` function with helpers, tests |
| `src/cache/types.rs` | Add `OuterJoinTerminal` and `OuterJoinOptional` variants to `UpdateQuerySource` |
| `src/query/transform.rs` | Tag optional-side tables with `OuterJoinTerminal` or `OuterJoinOptional`, tests |
| `src/cache/writer/cdc.rs` | Handle `OuterJoinTerminal` and `OuterJoinOptional` in invalidation check functions |
| `src/cache/writer/population.rs` | Skip NULL-padded phantom rows (NULL PK) during population |

## Future Work

- **FULL OUTER JOIN**: Both sides are optional. Terminal check would need to apply to both sides. Less common in practice — currently rejected at cacheability gate.
