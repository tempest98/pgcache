# Inequality Constraint Extraction for CDC Invalidation

## Context

The constraint system in `constraints.rs` extracts column constraints from WHERE clauses to optimize CDC invalidation. Currently it only extracts **equality** constraints (`column = value`). Inequality operators (`<`, `<=`, `>`, `>=`, `!=`) are silently ignored.

This matters for **multi-table (JOIN) queries**: when a CDC event matches equality constraints but fails inequality constraints, the system triggers a full query invalidation and re-population cycle unnecessarily. Single-table queries are unaffected — they never invalidate on INSERT, and the update query's EXISTS check handles filtering correctly.

**Goal**: Extract and check inequality constraints alongside equality constraints, reducing unnecessary invalidations for multi-table queries.

## Current State

The constraint system has three layers:

1. **Extraction** (`analyze_equality_expr`): Walks AND-connected WHERE/JOIN expressions, extracting `column = value` constraints and `column = column` equivalences. Ignores everything else.
2. **Propagation** (`propagate_constraints`): Fixpoint iteration pushing constraints through equivalences (e.g., `a.id = 1` + `a.id = b.id` → `b.id = 1`).
3. **CDC matching** (`row_constraints_match` in `cdc.rs`): For each constraint, compares the CDC row's column value using `LiteralValue::matches()` (equality-only).

The comparison logic for all operators already exists in `where_value_compare_string()` in `evaluate.rs` — it handles `=`, `!=`, `<`, `<=`, `>`, `>=` for all `LiteralValue` types (String, Integer, Float, Boolean). This function will be reused.

## Changes

### 1. Add `BinaryOp::op_flip()` — `src/query/ast.rs:255`

Add to the existing `impl BinaryOp` block. Flips comparison operators for `value op column` → `column op' value` normalization during extraction:

- `Equal` ↔ `Equal`, `NotEqual` ↔ `NotEqual`
- `LessThan` ↔ `GreaterThan`, `LessThanOrEqual` ↔ `GreaterThanOrEqual`
- Returns `None` for non-comparison ops (`And`, `Or`, `Like`, etc.)

### 2. Make `where_value_compare_string` public — `src/query/evaluate.rs:86`

Change `fn` to `pub fn`. Already handles all comparison operators for all `LiteralValue` types. No other changes needed.

### 3. Add `op` field to `ColumnConstraint` — `src/query/constraints.rs:10`

```rust
pub struct ColumnConstraint {
    pub column: ResolvedColumnNode,
    pub op: BinaryOp,
    pub value: LiteralValue,
}
```

### 4. Change `QueryConstraints` fields — `src/query/constraints.rs:32`

- `column_constraints`: `HashMap<ResolvedColumnNode, LiteralValue>` → `Vec<ColumnConstraint>`
  - Multiple constraints per column now possible (e.g., `amount > 100 AND amount < 500`)
  - Only `.len()` and `.is_empty()` are used externally — both work on `Vec`
- `table_constraints`: `HashMap<String, Vec<(String, LiteralValue)>>` → `HashMap<String, Vec<(String, BinaryOp, LiteralValue)>>`

### 5. Rename and extend `analyze_equality_expr` → `analyze_constraint_expr` — `src/query/constraints.rs:62`

- Accept all comparison operators via helper `is_constraint_op()` matching `Equal`, `NotEqual`, `LessThan`, `LessThanOrEqual`, `GreaterThan`, `GreaterThanOrEqual`
- For `column op value`: push constraint with `op`
- For `value op column`: push constraint with `op.op_flip()`
- `column = column` equivalences: still equality-only (inequality between two columns like `a.id > b.id` is not a useful equivalence)
- `And`: recurse both sides (unchanged)
- Everything else: ignored (unchanged)
- Change first parameter from `&mut HashSet<ColumnConstraint>` to `&mut Vec<ColumnConstraint>`

### 6. Rename `collect_query_equalities` → `collect_query_constraints` — `src/query/constraints.rs:131`

Update signature to return `Vec<ColumnConstraint>` instead of `HashSet<ColumnConstraint>`. Update `collect_from_table_source` parameter similarly.

### 7. Update `propagate_constraints` — `src/query/constraints.rs:151`

Change from `HashMap<ResolvedColumnNode, LiteralValue>` to `Vec<ColumnConstraint>`. Fixpoint iteration: for each equivalence, if one side has a constraint, propagate `(other_column, same_op, same_value)` to the other side. Use `constraints.contains()` to avoid duplicates — O(n) but n is tiny (single-digit constraints in typical queries).

Inequality constraints propagate through equality equivalences correctly: if `a.id = b.id` and `a.id > 5`, then `b.id > 5` follows.

### 8. Update `analyze_query_constraints` — `src/query/constraints.rs:197`

Wire through the new types. Build `table_constraints` with `(column, op, value)` tuples.

### 9. Update `row_constraints_match` — `src/cache/writer/cdc.rs:346`

- Destructure `(column_name, op, constraint_value)` from `table_constraints`
- For `Some(row_str)`: call `where_value_compare_string(constraint_value, row_str, *op)` (reusing the existing comparison engine from `evaluate.rs`)
- For `None` (NULL): only `Equal` + `LiteralValue::Null` returns true; all other ops return false (SQL NULL semantics)
- Add import: `use crate::query::evaluate::where_value_compare_string`

### 10. Update existing tests — `src/query/constraints.rs` tests

All `table_constraints` tuple assertions change from `(String, LiteralValue)` to `(String, BinaryOp, LiteralValue)`. For existing equality-only tests, add `BinaryOp::Equal` to each tuple. The `.len()` assertions remain unchanged.

Tests that use `.any()` closures need `(col, op, val)` destructuring with `*op == BinaryOp::Equal`.

### 11. Add new inequality tests — `src/query/constraints.rs` tests

- Simple inequality: `WHERE id > 5` → extracts `(id, GreaterThan, 5)`
- Multiple inequalities on same column: `WHERE id > 5 AND id < 100` → 2 constraints
- Reversed operand: `WHERE 5 < id` → stored as `(id, GreaterThan, 5)` via `op_flip()`
- NotEqual: `WHERE name != 'deleted'` → extracts `(name, NotEqual, 'deleted')`
- Mixed equality + inequality: `WHERE id = 1 AND name != 'deleted'` → 2 constraints
- Inequality propagation through JOIN: `a JOIN b ON a.id = b.id WHERE a.id > 5` → both tables get `id > 5`
- OR prevents extraction: `WHERE id > 5 OR id < 2` → 0 constraints

## Files Modified

| File | Nature of change |
|------|-----------------|
| `src/query/ast.rs` | Add `BinaryOp::op_flip()` method |
| `src/query/evaluate.rs` | Make `where_value_compare_string` public |
| `src/query/constraints.rs` | Core rework: data structures, extraction, propagation, tests |
| `src/cache/writer/cdc.rs` | Operator-aware `row_constraints_match` + new import |

## Design Notes

### Self-join deduplication

`ResolvedColumnNode::PartialEq` excludes `table_alias`, so `t1.id` and `t2.id` in `SELECT * FROM t t1 JOIN t t2 ON t1.id = t2.id` resolve to the same node. The `Vec::contains()` check during propagation correctly deduplicates these, preserving the existing behavior where `column_constraints.len() == 1` for this case.

### Correctness vs performance

This change is purely a performance optimization. Cache correctness is already maintained by the SQL-level EXISTS check in the update queries. The constraint system only determines whether to *skip* unnecessary invalidation work. False negatives (failing to skip) cause extra work; false positives (incorrectly skipping) would cause stale data. The implementation is conservative: only AND-connected, column-vs-literal comparisons are extracted.

### What this does NOT include

- **BETWEEN parsing**: `parse.rs` does not handle `AExprKind::AexprBetween`. This is a separate change.
- **IN list constraints**: `column IN (1, 2, 3)` is not extracted. Would require a different constraint representation.
- **LIKE/pattern constraints**: Not useful for CDC row filtering.

## Verification

1. `cargo check` — compile check
2. `cargo test` — all existing tests pass with updated assertions
3. `cargo clippy -- -D warnings` — no warnings
4. New inequality tests pass
