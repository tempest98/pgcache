# Constraint-Based Invalidation Optimization

## Key Insight

**Invalidation only matters when the result set could GROW.**

The UPDATE query mechanism already handles:
- Rows leaving the result set (removes them from cache)
- Rows being modified while staying in result set (updates them in cache)

Invalidation is needed when:
- A new row joins the result set
- An existing row that wasn't in the result set now qualifies

## Example Query

```sql
SELECT * FROM test t JOIN test_map tm ON tm.test_id = t.id WHERE t.id = 1
```

### Constraints Extracted

```rust
QueryConstraints {
    column_constraints: {
        test.id = 1,           // From WHERE clause
        test_map.test_id = 1   // Propagated through JOIN equivalence
    },
    equivalences: {
        ColumnEquivalence { left: test_map.test_id, right: test.id }
    },
    table_constraints: {
        "test" -> [("id", 1)],
        "test_map" -> [("test_id", 1)]
    }
}
```

## Invalidation Behavior (Optimized)

| Scenario | Invalidation | Reason |
|----------|--------------|--------|
| INSERT test_map (test_id=1) | ✅ Yes | Matches constraints, will appear in results |
| INSERT test_map (test_id=5) | ❌ No | Doesn't match constraint, won't appear |
| UPDATE test_map: test_id 1→2 | ❌ No | Leaving result set (UPDATE handles removal) |
| UPDATE test_map: test_id 5→1 | ✅ Yes | Entering result set (was 5, now 1) |
| UPDATE test_map: name only | ❌ No | JOIN key unchanged |
| UPDATE test: id 1→2 | ❌ No | Leaving result set (UPDATE handles removal) |

### Implementation Status

✅ **IMPLEMENTED** - Both INSERT and UPDATE optimizations are now active in pgcache/src/cache/query_cache.rs:728-876

### How It Works

#### INSERT Optimization
When a new row is inserted, we check if it matches ALL constraints for the table. If it doesn't match, we skip invalidation because the row won't appear in query results.

#### UPDATE Optimization
When a JOIN column changes, we use a key insight: **if the column changed, the old value was different from the new value**.

Logic:
1. Column didn't change → skip (row already in result set, UPDATE handles it)
2. Column changed + new value doesn't match constraints → skip (row leaving/staying out)
3. Column changed + new value matches ALL constraints → invalidate (row entering result set)

In case #3: Since the column changed, old value ≠ new value. If new value matches constraint but old value was different, then old value didn't match. Therefore the row is entering the result set.

## Optimization Strategy

### For INSERTs

Check if the new row will actually appear in results:

```rust
if new_row_values_match_all_constraints_for_this_table {
    invalidate(); // Row will appear in results
} else {
    skip(); // Row won't appear, UPDATE mechanism handles it
}
```

### For UPDATEs on JOIN Columns

Compare old and new values against constraints:

```rust
let old_matched = old_value_matches_constraint;
let new_matched = new_value_matches_constraint;

match (old_matched, new_matched) {
    (false, true) => invalidate(),  // Row entering result set
    (true, false) => skip(),        // Row leaving result set (UPDATE mechanism handles)
    (false, false) => skip(),       // Row never in result set
    (true, true) => skip(),         // Row staying in result set (UPDATE mechanism handles)
}
```

## Implementation Notes

The constraint analysis performed during query registration provides the necessary information to determine whether a CDC event will cause a row to enter the result set. This allows us to skip unnecessary invalidations while maintaining cache correctness.
