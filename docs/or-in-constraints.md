# OR and IN Constraint Extraction for CDC Invalidation

## Context

The constraint system in `constraints.rs` extracts column constraints from WHERE clauses to optimize CDC invalidation. After the inequality constraints work, it handles AND-connected comparisons (`=`, `!=`, `<`, `<=`, `>`, `>=`) and BETWEEN. However, OR expressions and IN lists are silently ignored.

This means queries like `WHERE status IN ('active', 'pending')` or `WHERE id = 1 OR id = 2` produce no constraints, causing unnecessary invalidation for every CDC event.

**Goal**: Extract disjunctive constraints from OR expressions and IN lists, enabling CDC filtering for these common patterns.

## Current State

The extraction function `analyze_constraint_expr` handles:
- Comparison operators (`column op value`) → `ColumnConstraint`
- AND → recurse both sides
- BETWEEN / BETWEEN SYMMETRIC → two inequality constraints
- Everything else (OR, IN, subqueries, etc.) → ignored

All extracted constraints are AND-connected in a flat `HashSet<ColumnConstraint>`. The CDC matching in `row_constraints_match` requires ALL constraints to match.

## Key Design Constraint: Cross-Table OR

OR branches that span multiple tables cannot be used for per-table CDC filtering. Consider:

```sql
SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id
WHERE o.status = 'urgent' OR c.vip = true
```

A CDC event on `orders` with `status = 'normal'` — the local branch fails, but the row could still be in the result set if the joined customer has `vip = true`. We can't evaluate the remote branch from a single table's CDC event.

**Rule**: An OR is only extractable when ALL branches reference the **same table**. If any branch references a different table (or is opaque), the entire OR is skipped.

## Changes

### 1. Add `ConstraintClause` enum — `src/query/constraints.rs`

```rust
/// A clause in the constraint formula. All clauses are AND-connected.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ConstraintClause {
    /// A single comparison (column op value)
    Single(ColumnConstraint),
    /// A disjunction — at least one must match (OR / IN semantics)
    AnyOf(Vec<ColumnConstraint>),
}
```

### 2. Add `TableConstraint` enum — `src/query/constraints.rs`

The table-level representation used by CDC matching:

```rust
/// A constraint clause organized for per-table CDC matching
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    /// A single comparison
    Comparison(String, BinaryOp, LiteralValue),
    /// At least one must match
    AnyOf(Vec<(String, BinaryOp, LiteralValue)>),
}
```

### 3. Change `QueryConstraints` fields — `src/query/constraints.rs`

- `column_constraints`: `HashSet<ColumnConstraint>` → `Vec<ConstraintClause>`
  - Now holds both simple constraints and disjunctive clauses
  - All clauses are AND-connected at the top level
  - `Vec` because `ConstraintClause::AnyOf` doesn't have a useful `Hash`
- `table_constraints`: `HashMap<String, Vec<(String, BinaryOp, LiteralValue)>>` → `HashMap<String, Vec<TableConstraint>>`

### 4. Change `analyze_constraint_expr` signature — `src/query/constraints.rs`

Change first parameter from `&mut HashSet<ColumnConstraint>` to `&mut Vec<ConstraintClause>`. The function now returns clauses rather than flat constraints.

Existing extraction logic stays the same but wraps results in `ConstraintClause::Single`.

### 5. Add OR handling to `analyze_constraint_expr` — `src/query/constraints.rs`

New match arm for `BinaryOp::Or`:

```
ResolvedWhereExpr::Binary(binary) if binary.op == BinaryOp::Or => {
    or_constraints_extract(expr, clauses);
    // Note: equivalences inside OR branches are NOT collected —
    // they're conditional on the branch being active
}
```

### 6. Add `or_constraints_extract` — `src/query/constraints.rs`

Extracts an `AnyOf` clause from an OR expression:

1. Flatten the OR tree: walk right-recursive `OR(a, OR(b, OR(c, d)))` into a list of branches `[a, b, c, d]`
2. For each branch, recursively extract constraints (into a separate `Vec<ConstraintClause>`, ignoring equivalences)
3. If any branch produces zero constraints → entire OR is opaque, return without adding a clause
4. If any branch produces multiple clauses (AND of constraints) → skip (compound branches not supported in first cut)
5. Each branch should produce exactly one clause. Unwrap `Single` constraints and collect; flatten nested `AnyOf` constraints
6. Check that all collected constraints reference the same table → if not, skip (cross-table OR)
7. Add `ConstraintClause::AnyOf(collected)` to the clauses

Flattening nested OR handles chains like `id = 1 OR id = 2 OR id = 3` which the parser produces as right-recursive trees.

### 7. Add IN handling to `analyze_constraint_expr` — `src/query/constraints.rs`

New match arm for `MultiOp::In`:

```
ResolvedWhereExpr::Multi(multi) if multi.op == MultiOp::In => {
    in_constraints_extract(&multi.exprs, clauses);
}
```

Extract from `exprs`: if `exprs[0]` is `Column` and all remaining are `Value`, produce `AnyOf` of equality constraints. Otherwise skip.

### 8. Add NOT IN handling to `analyze_constraint_expr` — `src/query/constraints.rs`

New match arm for `MultiOp::NotIn`:

`NOT IN (1, 2, 3)` is semantically `col != 1 AND col != 2 AND col != 3`. Extract as individual `Single(ColumnConstraint { op: NotEqual, ... })` clauses — these are AND-connected, fitting the existing model. No new representation needed.

If `exprs[0]` is `Column` and all remaining are `Value`, produce one `Single` clause per value with `BinaryOp::NotEqual`. Otherwise skip.

### 9. Update `between_constraints_extract` — `src/query/constraints.rs`

Change parameter from `&mut HashSet<ColumnConstraint>` to `&mut Vec<ConstraintClause>`. Wrap the two constraints in `ConstraintClause::Single`.

### 10. Update `collect_from_table_source` and `collect_query_constraints` — `src/query/constraints.rs`

Change parameter/return types from `HashSet<ColumnConstraint>` to `Vec<ConstraintClause>`.

### 11. Update `propagate_constraints` — `src/query/constraints.rs`

Input and output change to `Vec<ConstraintClause>`. Propagation rules:

- `Single` constraints: propagate through equivalences as today (if column has an equivalence, clone with the equivalent column)
- `AnyOf` constraints: propagate only when ALL constraints in the AnyOf reference the **same column** AND that column has an equivalence. Clone the entire AnyOf with the equivalent column. If constraints reference different columns, do not propagate (conservative).

Dedup: after propagation, remove duplicate clauses. For `Single`, compare the inner `ColumnConstraint`. For `AnyOf`, compare the full constraint list (order-independent — sort before comparing, or use a set-based check).

### 12. Update `analyze_query_constraints` — `src/query/constraints.rs`

Build `table_constraints` from `Vec<ConstraintClause>`:

- `Single(c)` → `TableConstraint::Comparison(column, op, value)` keyed by `c.column.table`
- `AnyOf(cs)` → `TableConstraint::AnyOf([(column, op, value), ...])` keyed by the shared table (all constraints guaranteed same table by extraction)

### 13. Update `row_constraints_match` — `src/cache/writer/cdc.rs`

Iterate `Vec<TableConstraint>`. All must match (AND semantics):

- `Comparison(col, op, val)`: same as current — check single constraint
- `AnyOf(alternatives)`: check each alternative, return true if **any** matches. If **none** match, return false (row doesn't satisfy this clause)

Extract the single-constraint matching logic into a helper `row_constraint_matches_value` to reuse between `Comparison` and `AnyOf` branches.

### 14. Update `has_table_constraints` checks — `src/cache/writer/cdc.rs`

The `.contains_key()` checks on `table_constraints` continue to work unchanged since the HashMap key (table name) is the same.

### 15. Update existing tests — `src/query/constraints.rs`

All `column_constraints.len()` assertions change to count clauses (each `Single` is one clause). For existing tests with only equality/inequality constraints, the count stays the same — each constraint is a `Single` clause.

The `has_constraint` helper needs updating to search within both `Single` and `AnyOf` variants of `TableConstraint`.

### 16. Add new tests — `src/query/constraints.rs`

**IN tests:**
- `WHERE id IN (1, 2, 3)` → 1 AnyOf clause with 3 equality constraints
- `WHERE id IN (1, 2) AND name = 'alice'` → 1 AnyOf + 1 Single
- `WHERE id IN (SELECT ...)` → 0 constraints (subquery IN, not literal IN)
- IN propagation through JOIN: `a JOIN b ON a.id = b.id WHERE a.id IN (1, 2)` → both tables get AnyOf

**NOT IN tests:**
- `WHERE id NOT IN (1, 2, 3)` → 3 Single NotEqual constraints
- NOT IN propagation through JOIN

**OR tests:**
- `WHERE id = 1 OR id = 2` → 1 AnyOf clause
- `WHERE id = 1 OR id = 2 OR id = 3` → 1 AnyOf with 3 constraints (flattened)
- `WHERE id > 5 OR id < 2` → 1 AnyOf with 2 inequality constraints
- `WHERE (id = 1 OR id = 2) AND name = 'alice'` → 1 AnyOf + 1 Single
- Cross-table OR: `WHERE a.id = 1 OR b.id = 2` → 0 constraints (skipped)
- OR with opaque branch: `WHERE id = 1 OR func(id)` → 0 constraints
- OR with compound branch: `WHERE (id = 1 AND name = 'x') OR id = 2` → 0 constraints (compound branch, deferred)
- Same-table different-column OR: `WHERE id = 1 OR name = 'alice'` → 1 AnyOf with 2 constraints
- OR propagation through JOIN (same column): `a JOIN b ON a.id = b.id WHERE a.id = 1 OR a.id = 2` → both tables get AnyOf

## Files Modified

| File | Nature of change |
|------|-----------------|
| `src/query/constraints.rs` | New types (`ConstraintClause`, `TableConstraint`), OR/IN/NOT IN extraction, propagation updates, tests |
| `src/cache/writer/cdc.rs` | `row_constraints_match` handles `TableConstraint` enum |

## Design Notes

### Why not full CNF conversion?

General CNF conversion of `(A AND B) OR C` into `(A OR C) AND (B OR C)` can cause exponential blowout. We avoid this by limiting OR extraction to branches that produce a single constraint each. Compound branches like `(id = 1 AND name = 'x') OR id = 2` are skipped. This covers the most common patterns (IN lists, simple OR of comparisons) without complexity.

### AnyOf dedup during propagation

After propagation, the same AnyOf could be added multiple times (via different equivalence chains). For `Single` constraints, the existing approach works. For `AnyOf`, dedup requires comparing the full constraint list. Since constraint counts are small, a sort-and-compare approach is sufficient.

### Equivalences inside OR branches

Equivalences found inside OR branches (e.g., `WHERE (a.id = b.id AND a.id = 1) OR a.id = 2`) are NOT collected. They're conditional on the branch being active and can't be used for unconditional propagation.

### NOT IN as individual constraints

`NOT IN (1, 2, 3)` decomposes into `!= 1 AND != 2 AND != 3`. Each is an independent AND-connected constraint. A CDC row with `id = 1` fails the `!= 1` constraint → row not in result set → skip invalidation. This is correct and requires no new representation.

### Interaction with BETWEEN

BETWEEN already produces two `Single` inequality constraints. No interaction with OR/IN changes.

### What this does NOT include

- **Compound OR branches**: `(id = 1 AND name = 'x') OR (id = 2 AND name = 'y')` requires `AnyOf(Vec<Vec<ColumnConstraint>>)` — each branch is an AND-group. Deferred.
- **Cross-table OR**: Correctly identified as non-filterable per-table. Skipped (safe default).
- **ANY/ALL operators**: `id = ANY(ARRAY[1,2,3])` is semantically similar to IN but has different AST representation. Deferred.

## Verification

1. `cargo check` — compile check
2. `cargo test` — all existing tests pass with updated assertions
3. `cargo clippy -- -D warnings` — no warnings
4. New OR/IN/NOT IN tests pass
