# Skip Invalidation for FromClause Source DELETE on Multi-Table Queries

## Context

After decorrelating correlated EXISTS subqueries into INNER JOINs, CDC DELETE events on joined tables trigger full invalidation even though the cache can serve correctly without it. The row is already removed from the cache table by `cache_delete_sql`, and serve-time re-evaluation handles correctness. Full invalidation forces unnecessary re-population.

**Current behavior**: DELETE on orders (FromClause source, multi-table JOIN, no LIMIT) → cache MISS (invalidation + re-population).

**Desired behavior**: DELETE on orders → cache HIT (row removed in-place, serve-time re-evaluates).

## Root Cause

In `row_uncached_invalidation_check` (`src/cache/writer/cdc.rs:443`), for FromClause source DELETE on a multi-table query:

1. `has_limit && Delete` check passes (no LIMIT) — OK
2. `is_single_table()` returns false (JOIN) — continues
3. No table-specific constraints → calls `join_membership_unchanged`
4. `join_membership_unchanged` receives `key_data=None` (DELETE has no before/after) → returns `false`
5. `!false` = `true` → **invalidates**

The function can't prove join membership is unchanged because there's no "new row" to compare. But for DELETE, this proof isn't needed — removing a row from an INNER JOIN can only shrink the result set, never expand it.

## Safety Analysis

**INNER JOIN (EXISTS decorrelation)**: DELETE can only shrink result. Safe to skip invalidation.

**LEFT JOIN optional side (NOT EXISTS decorrelation)**: DELETE could expand result (shrinks exclusion set). But these tables get `OuterJoinOptional` or `OuterJoinTerminal` source, NOT `FromClause`. The optimization doesn't affect them.

**LIMIT queries**: Already handled — `has_limit && Delete` returns `true` before we reach the new code. Window-shift risk remains correctly covered.

Source assignment verified in `query_table_update_queries` (`src/query/update.rs:44-55`): outer join optional-side tables are reclassified from `FromClause` to `OuterJoinOptional`/`OuterJoinTerminal`. Only INNER JOIN tables retain `FromClause`.

## Change

### `src/cache/writer/cdc.rs` — `row_uncached_invalidation_check`

Add early return for FromClause source DELETE (non-LIMIT):

```rust
UpdateQuerySource::FromClause => {
    // DELETE on a limited query's table: cached result may have fewer
    // rows than the LIMIT window. Invalidate to trigger re-population.
    if update_query.has_limit && operation == CdcOperation::Delete {
        return true;
    }

    // DELETE on FromClause source: the row is already removed from the cache
    // table. For INNER JOIN (the only join type that gets FromClause source),
    // removing a row can only shrink the result set, never expand it.
    // Serve-time re-evaluation handles correctness.
    if operation == CdcOperation::Delete {
        return false;
    }

    // ... rest of existing INSERT/UPDATE logic unchanged ...
```

### `tests/correlated_subquery_test.rs` — `test_correlated_exists_basic`

Change the DELETE assertion from `assert_cache_miss` to `assert_cache_hit`:

```rust
// --- CDC DELETE on inner table (orders) → in-place removal, cache hit ---
// FromClause source: row removed from cache table, serve-time EXISTS
// re-evaluates. INNER JOIN DELETE can only shrink the result.
```

## Verification

1. `cargo test --lib` — all unit tests pass
2. `cargo test --test correlated_subquery_test -- --test-threads=1` — all 7 integration tests pass
3. `cargo test --test subquery_test -- --test-threads=1` — existing subquery tests still pass (especially Inclusion DELETE = cache hit, Exclusion DELETE = cache miss)
4. `cargo clippy --tests --benches -- -D warnings` — clean
5. Manually verify no other integration tests assert `cache_miss` for FromClause source DELETE on multi-table non-LIMIT queries (search for patterns)
