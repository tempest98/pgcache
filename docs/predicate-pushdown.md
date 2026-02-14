# Predicate Pushdown into FROM Subqueries

## Context

When pgcache encounters a query like:
```sql
SELECT * FROM (
    SELECT a, b FROM t1 UNION SELECT c AS a, d AS b FROM t2 WHERE d > 10
) sub WHERE a < 100
```
the outer `WHERE a < 100` is evaluated after the entire UNION materializes. Pushing it into each branch reduces the rows fetched from origin during cache population and the rows scanned when serving from cache. This is especially valuable for UNION subqueries where each branch independently benefits from the filter.

The transform operates at the resolved level (post-resolution), where column references are fully qualified and SELECT * is expanded.

## Files to Create/Modify

| File | Action |
|---|---|
| `pgcache/src/query/pushdown.rs` | **Create** — all pushdown logic + unit tests |
| `pgcache/src/query/mod.rs` | **Modify** — add `pub mod pushdown;` |
| `pgcache/src/cache/writer/query.rs` | **Modify** — call `predicate_pushdown_apply` after resolution (line ~113) |

## Algorithm

Given an outer `ResolvedSelectNode` whose single FROM source is a `Subquery(sub)`:

1. Split outer WHERE into AND-conjuncts (flatten nested AND tree)
2. Build column name→position map from subquery's output columns (leftmost SELECT for set ops, via `derived_table_columns_extract` logic)
3. For each conjunct, check:
   - All `ResolvedColumnNode` refs have `schema == ""` and `table == sub.alias`
   - No `ResolvedWhereExpr::Subquery` variant anywhere in the predicate (could reference outer scope)
4. For pushable conjuncts, attempt to remap into the subquery body:
   - **SELECT body**: check branch is safe (no GROUP BY, HAVING, window fns), map each column ref by position to the branch's SELECT list column. Only push if the target position has a simple `ResolvedColumnExpr::Column`. AND remapped predicate into the branch's existing WHERE.
   - **SetOp body**: recursively push into both `left` and `right`. If either branch rejects, skip the entire conjunct.
   - **Values body**: skip.
5. Remove successfully pushed conjuncts from outer WHERE. Reconstruct remaining conjuncts with AND.

Column remapping uses position: outer column name "a" → position 0 in output map → position 0 in each branch's SELECT columns → that branch's actual `ResolvedColumnNode`.

**Important**: `ResolvedWhereExpr::nodes::<ResolvedColumnNode>()` traverses into `Subquery` children, which would pick up inner scope columns. Must check for `Subquery` variant presence before using `nodes()` for alias checking.

## Functions

All in `pushdown.rs`:

```
predicate_pushdown_apply(ResolvedQueryExpr) -> ResolvedQueryExpr          // entry point
select_node_pushdown_apply(ResolvedSelectNode) -> ResolvedSelectNode      // per-SELECT logic
where_expr_conjuncts_split(ResolvedWhereExpr) -> Vec<ResolvedWhereExpr>   // flatten AND tree
where_expr_conjuncts_join(Vec<ResolvedWhereExpr>) -> Option<ResolvedWhereExpr>  // rebuild AND tree
predicate_targets_subquery(&ResolvedWhereExpr, alias: &str) -> bool       // all cols target alias?
predicate_has_subquery(&ResolvedWhereExpr) -> bool                        // contains Subquery variant?
subquery_output_column_names(&ResolvedQueryExpr) -> Vec<String>           // name→position from leftmost SELECT
branch_pushdown_is_safe(&ResolvedSelectNode) -> bool                      // no GROUP BY/HAVING/window fns
predicate_push_into_query(&ResolvedQueryExpr, predicate, positions) -> Option<ResolvedQueryExpr>  // push into SELECT or recurse SetOp
where_expr_columns_remap(&ResolvedWhereExpr, positions, branch_columns) -> Option<ResolvedWhereExpr>  // remap column refs
```

## Edge Cases

- **Subquery has LIMIT**: skip pushdown (changes row set before filter)
- **Predicate contains OR**: pushed as a whole unit if all column refs target the alias
- **All conjuncts pushed**: outer WHERE becomes `None`
- **Non-column SELECT expr** (arithmetic, function) at target position: skip that conjunct
- **Multiple FROM sources**: only push into subqueries (real tables don't need pushdown). Currently cacheability allows only single FROM source, so multiple FROMs won't arise for cached queries, but handle gracefully.

## Integration Point

In `cache/writer/query.rs`, line ~113, after resolution:
```rust
let resolved = query_expr_resolve(&base_query, &self.cache.tables, search_path)?;
let resolved = predicate_pushdown_apply(resolved);  // <-- add this
```

All downstream consumers (population, worker serve, CDC update queries) automatically benefit.

## Tests

Unit tests in `pushdown.rs` `mod tests`, using the existing pattern from `resolved.rs`:
- `resolve_query()` helper: parse SQL → AST → resolve with test table metadata
- Apply pushdown, deparse result, assert SQL matches expected

Key test cases:
1. UNION with pushdown into both branches
2. UNION with column alias remapping (branch 2 has different column names)
3. Existing WHERE in branch — pushed predicate ANDed with existing
4. Non-column expr at target position — no pushdown
5. GROUP BY in branch — no pushdown
6. Window function in branch — no pushdown
7. Multiple AND conjuncts — all pushed
8. Mixed references (if multiple FROM sources existed) — only matching pushed
9. Plain subquery (no set op) — pushed directly
10. No subquery in FROM — returned unchanged
11. Predicate contains subquery expr — not pushed
12. Subquery with LIMIT — not pushed
13. Conjuncts split/join roundtrip

## Verification

```bash
cargo test --lib pushdown     # unit tests
cargo clippy -- -D warnings   # lint
cargo test --lib               # full unit test suite (no regressions)
```
