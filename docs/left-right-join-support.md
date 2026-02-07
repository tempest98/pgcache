# LEFT and RIGHT JOIN Support

## Background

pgcache currently supports only INNER JOINs for cached queries. The cacheability gate in `cache/query.rs` (`is_supported_join`) rejects any join where `join_type != JoinType::Inner`. LEFT and RIGHT JOINs are common in application queries, and rejecting them forces those queries through the pass-through path to origin on every execution.

The parsing and resolution layers already handle all join types. The AST (`JoinType::Left`, `JoinType::Right`), parser, and resolver all accept and represent outer joins. `ResolvedJoinNode::deparse` correctly emits `LEFT JOIN` / `RIGHT JOIN` syntax. The work is in cacheability analysis and CDC invalidation.

## How the Cache Works

pgcache stores per-table data, not materialized join results. Each origin table referenced by a cached query gets its own cache table mirroring the origin schema. When serving a cached query, pgcache deparses the resolved query and executes it against the cache database, running the original JOINs against the cached per-table data.

This architecture naturally supports outer join retrieval — PostgreSQL handles LEFT/RIGHT JOIN semantics when executing against the cache tables. The challenge is keeping the per-table cache data correct under CDC events.

## CDC Semantics: INNER vs Outer

With INNER JOINs, a row either participates in the result or doesn't. CDC handling is symmetric.

Outer joins introduce asymmetry between the **preserved side** (always contributes rows) and the **optional side** (contributes data when matched, NULLs otherwise):

- **INSERT on the optional side**: Adds the row to the optional-side cache table. The LEFT JOIN at retrieval fills in what was previously NULL-padded. The preserved-side result set is unchanged.

- **DELETE on the optional side**: Removes the row from the optional-side cache table. The LEFT JOIN at retrieval produces NULL-padded columns for the preserved-side row.

- **UPDATE on the optional side**: Combination of the above. If join columns change, the row may start or stop matching, handled by the existing upsert-then-delete-stale mechanism.

These operations are safe **when the optional-side table is terminal**.

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
3. GROUP BY columns
4. HAVING clause

The SELECT list is fine — selecting columns from the optional side doesn't affect result set membership.

If a table is terminal, changes to it cannot cascade into affecting other tables' membership in the result set. The existing CDC mechanisms work correctly because:
- The preserved-side result set never changes
- The optional-side data is either present or NULL-padded, determined at retrieval time by the LEFT JOIN against cache tables
- No downstream join or filter depends on the optional-side values

## Strategy

Support LEFT and RIGHT JOINs where the optional-side table is terminal. Reject outer joins where the optional side feeds into downstream joins or predicates (pass-through).

RIGHT JOINs can be normalized to LEFT JOINs by swapping sides. This simplifies all downstream logic to only reason about LEFT JOINs.

### Terminal table check

The resolved AST supports this check directly. `ResolvedColumnNode` carries the real table name (not alias), and `nodes::<ResolvedColumnNode>()` extracts all column references from any subtree.

For each outer join in the join tree:

1. Collect the set of table names on the optional side (right side for LEFT JOIN)
2. Verify none of those table names appear in column references in the WHERE clause, any ancestor or sibling join condition, GROUP BY, or HAVING

Two-pass approach:
1. Collect all column references from WHERE, GROUP BY, and HAVING into a set of table names
2. Walk the join tree recursively. At each outer join, collect the optional-side table names and check for overlap. Additionally check ancestor join conditions by threading down the set of tables referenced by join conditions above the current node.

### CDC invalidation

No changes to the core CDC handlers. The existing mechanisms work for terminal optional-side tables:

- `handle_insert`: The upsert mechanism adds the row to the optional-side cache table if it matches. The LEFT JOIN at retrieval fills in previously NULL columns.
- `handle_delete`: Removes the row from the optional-side cache table. The LEFT JOIN at retrieval produces NULLs.
- `handle_update`: Upsert + conditional delete. Correct for the same reasons.

The invalidation checks need review:

- `row_uncached_invalidation_check`: The "PK didn't change and all join columns are PK" stability optimization assumes INNER JOIN semantics. For the optional side of a LEFT JOIN, a row not being in cache means the preserved side gets NULL-padded, which is handled correctly by retrieval-time LEFT JOIN. The existing logic should work as-is for terminal optional-side tables but needs verification.

- `row_cached_invalidation_check`: Checks if join columns changed. Applies equally to outer joins.

### Constraint propagation

`constraints.rs` propagates equality constraints through join equivalences uniformly. For the terminal case, propagation in both directions is safe. No changes needed.

### AST deparse fix

`JoinNode::deparse` in `ast.rs` hardcodes `" JOIN"` without the type qualifier. This must emit `" LEFT JOIN"` / `" RIGHT JOIN"` to match the resolved-level deparse. This is used by `query_table_update_queries` in `transform.rs` for generating CDC check queries.

## Files to Change

- `src/cache/query.rs`: Relax `is_supported_join`, add terminal table check
- `src/query/ast.rs`: Fix `JoinNode::deparse` to emit join type qualifier

## Files to Review

- `src/cache/writer/cdc.rs`: Confirm invalidation checks are correct for terminal outer joins
- `src/query/constraints.rs`: Confirm propagation is safe for terminal case
- `src/cache/writer/population.rs`: Confirm per-table fetching works
- `src/query/transform.rs`: Confirm update query generation works with outer join syntax

## Future Work

- **Non-terminal outer joins**: Could support with a conservative invalidation strategy — any CDC event on the optional side that changes join-relevant columns triggers full re-population. Correct but pessimistic.
- **FULL OUTER JOIN**: Both sides are optional. Terminal check would need to apply to both sides. Less common in practice.
