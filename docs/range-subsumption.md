# Range-Based Constraint Subsumption

## Context

The current `table_constraints_subsumed` function (in `constraints.rs`) is equality-only. If the cached query has any non-equality constraint, subsumption is rejected:

```rust
cached_cs.iter().all(|(_, op, _)| *op == BinaryOp::Equal)
    && cached_cs.iter().all(|c| new_cs.contains(c))
```

This means common range patterns miss subsumption entirely:

```sql
-- Cached: SELECT * FROM events WHERE id > 5
-- New: SELECT * FROM events WHERE id > 10
-- id > 10 implies id > 5, but current code rejects this
```

**Goal**: Extend subsumption to handle range constraints (inequalities, BETWEEN) on ordered types, plus equality/not-equal on all types.

## Approach: Per-Column Range Reduction

Instead of comparing constraint tuples directly, reduce all constraints on a single column to a canonical **ColumnRange** and check containment.

### ColumnRange Enum

```rust
struct RangeBound {
    value: LiteralValue,
    inclusive: bool,  // >= vs >
}

enum ColumnRange {
    /// Values are incomparable (Parameter, Null, mixed types) — can't reason
    Unknown,
    /// No constraints on this column — any value matches
    Unconstrained,
    /// Contradictory constraints — no value can satisfy (e.g., = 5 AND > 10)
    Empty,
    /// Exactly one value: column = v
    Equal(LiteralValue),
    /// Bounded interval with possible exclusions
    Range {
        lower: Option<RangeBound>,
        upper: Option<RangeBound>,
        not_equal: Vec<LiteralValue>,
    },
}
```

Why an enum:
- Each variant is a distinct semantic state — avoids nonsensical field combinations
- `Unknown` and `Empty` are explicit rather than silently collapsing to `Unconstrained`
- Subsumption dispatches cleanly on `(cached_variant, new_variant)` pairs
- No invalid states to guard against

### Building a ColumnRange

`column_range_build(constraints: &[(BinaryOp, LiteralValue)]) -> ColumnRange`

1. If no constraints → `Unconstrained`
2. If any value is incomparable (Parameter, Null, NullWithCast) → `Unknown`
3. If any constraint is `=`:
   - Multiple `=` with different values → `Empty` (contradictory)
   - Single `=` value: validate against other constraints
     - If any bound contradicts the equality (e.g., `= 5 AND > 10`) → `Empty`
     - If any `!=` matches the equality value → `Empty`
     - Otherwise → `Equal(v)`
4. Otherwise build `Range`:
   - `>` / `>=` → set or tighten `lower` (keep the higher bound; at equal value, prefer exclusive `>`)
   - `<` / `<=` → set or tighten `upper` (keep the lower bound; at equal value, prefer exclusive)
   - `!=` → collect into `not_equal`
   - After tightening, validate bounds aren't contradictory (lower > upper) → `Empty`

Tightening uses `literal_value_order` for bound comparison. If it returns `None` → `Unknown`.

### Subsumption Check

`column_range_subsumes(cached: &ColumnRange, new: &ColumnRange) -> bool`

| Cached | New | Subsumed? | Rationale |
|--------|-----|-----------|-----------|
| `Unknown` | anything | No | Can't reason about cached coverage |
| anything | `Unknown` | No | Can't reason about what new needs |
| `Empty` | anything | No | Cached has no rows to serve from |
| anything | `Empty` | Yes | New returns nothing — trivially covered |
| `Unconstrained` | anything | Yes | Cached loaded all rows |
| anything | `Unconstrained` | No | New wants all rows, cached is restricted |
| `Equal(a)` | `Equal(b)` | `a == b` | Same point |
| `Equal(_)` | `Range { .. }` | No | Cached has one value, new wants a range |
| `Range { .. }` | `Equal(v)` | `range_contains(v)` | Point within interval |
| `Range { .. }` | `Range { .. }` | bounds + exclusions | See below |

Note: rows where `anything` appears in the table above are checked top-to-bottom, so `(Empty, Empty)` hits the `(Empty, anything)` rule first → No. This is correct: an empty cached result has no data to serve.

**Range-vs-Range detail**: new's range must be contained within cached's range:
- If cached has a lower bound, new must have a lower bound that is >= cached's
- If cached has an upper bound, new must have an upper bound that is <= cached's
- If new has no lower bound but cached does → No (new is open-ended below)
- If new has no upper bound but cached does → No (new is open-ended above)
- Each cached `not_equal` value must be excluded by new — either in new's `not_equal` list, or outside new's range

**Bound comparison helper**: `bound_compare(a: &RangeBound, b: &RangeBound) -> Option<Ordering>` — delegates to `literal_value_order` for value comparison, using inclusivity as tiebreaker (exclusive is tighter).

### Type Safety of Ordering

Range bounds only make sense for types with well-defined ordering. `literal_value_order` returns `None` for incomparable types, which produces `Unknown`.

| Type | Range bounds | Equal/NotEqual | Notes |
|------|-------------|----------------|-------|
| Integer | Yes | Yes | |
| Float | Yes | Yes | NotNan ensures total order |
| Boolean | No | Yes | `literal_value_order` returns None |
| String | Yes | Yes | Lexicographic — correct for C/POSIX collation |
| StringWithCast | Yes | Yes | Lexicographic — correct for dates, UUIDs, timestamps |
| Parameter | No | No | `Unknown` — value not known at analysis time |
| Null | No | No | `Unknown` — NULL comparisons are three-valued |

String ordering matches what `where_value_compare_string` already does in CDC row matching. If locale-sensitive collations become a concern, phase 3 can restrict string range comparison to known-safe cast types.

## Changes

### 1. Add `RangeBound` and `ColumnRange` — `src/query/constraints.rs`

New types as described above. Private to the module — implementation details of subsumption, not part of the public API.

### 2. Add `column_range_build` — `src/query/constraints.rs`

Takes `&[(BinaryOp, LiteralValue)]` for a single column. Returns `ColumnRange`.

Handles constraint folding: equal absorption, bound tightening, contradiction detection, incomparable value detection.

### 3. Add `column_range_subsumes` — `src/query/constraints.rs`

Takes two `&ColumnRange` references. Returns `bool`. Core containment logic dispatching on variant pairs.

Helper: `range_contains_value(lower, upper, not_equal, value) -> bool` for the `Range` vs `Equal(v)` case.

### 4. Refactor `table_constraints_subsumed` — `src/query/constraints.rs`

Current implementation compares flat constraint lists. New implementation:

1. Group `table_constraints[table]` entries by column name — both for cached and new
2. For each column that cached constrains:
   a. Build `ColumnRange` from cached's constraints on that column
   b. Build `ColumnRange` from new's constraints on that column (or `Unconstrained` if new has none)
   c. Call `column_range_subsumes(cached_range, new_range)`
3. All columns must pass → table is subsumed

Columns that cached does NOT constrain don't need checking (cached loaded all values).

### 5. Update tests — `src/query/constraints.rs`

Existing subsumption tests continue to pass. The `test_subsumption_rejects_non_equality` test changes — it currently asserts `false` for `id > 5` vs `id > 10`, and should now assert `true`.

New tests:

**Range tightening:**
- `id > 3` subsumed by `id > 5`
- `id > 3` NOT subsumed by `id > 1`
- `id >= 3` subsumed by `id > 3` (exclusive tighter than inclusive at same value)
- `id >= 3` subsumed by `id >= 3`

**Range containment:**
- `id >= 3 AND id <= 10` subsumed by `id BETWEEN 5 AND 8`
- `id > 0 AND id < 100` NOT subsumed by `id > 50` (no upper bound)

**Point in range:**
- `id > 3` subsumed by `id = 5`
- `id > 3` NOT subsumed by `id = 2`

**Equal vs range:**
- `id = 5` NOT subsumed by `id > 3`
- `id = 5` subsumed by `id = 5` (unchanged from v1)

**Not-equal:**
- `id != 5` subsumed by `id = 3`
- `id != 5` NOT subsumed by `id = 5`
- `id != 5` subsumed by `id > 10` (range excludes 5)
- `id != 5` NOT subsumed by `id > 3` (range includes 5)
- `id != 5` subsumed by `id != 5`
- `id != 5` NOT subsumed by `id != 3`

**Mixed columns:**
- `id > 3 AND name = 'alice'` subsumed by `id > 5 AND name = 'alice'`
- `id > 3 AND name = 'alice'` NOT subsumed by `id > 5 AND name = 'bob'`

**Empty (contradictory):**
- Any query subsumes `id = 5 AND id > 10` (new is Empty → trivially covered)
- `id = 5 AND id > 10` does NOT subsume anything (cached is Empty → no data)

**Unknown (incomparable):**
- Parameterized constraint: neither direction subsumed

## Files Modified

| File | Nature of change |
|------|-----------------|
| `src/query/constraints.rs` | New types (`RangeBound`, `ColumnRange`), new functions (`column_range_build`, `column_range_subsumes`), refactored `table_constraints_subsumed`, new tests |

No other files change. The public interface of `table_constraints_subsumed` is unchanged — same signature, same semantics, broader coverage.

## What This Does NOT Include

- **IN-set constraints** (phase 2): `WHERE id IN (1, 2, 3)` — requires OR/IN extraction from `or-in-constraints.md` first
- **Collation-aware string ordering** (phase 3): restrict string range comparison to known-safe cast types
- **Cross-column constraints**: `col_a < col_b` — doesn't reduce to per-column ranges
