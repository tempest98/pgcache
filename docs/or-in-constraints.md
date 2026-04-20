# CNF-Based Constraint Extraction

## Status

IN and NOT IN extraction are implemented. OR extraction is **not** — `BinaryOp::Or` falls into the catch-all "cannot extract" arm in `analyze_constraint_expr`, and `test_no_propagation_with_or` asserts that `WHERE id = 1 OR id = 2` currently yields zero constraints.

Rather than adding OR as a one-off case, this doc proposes refactoring constraint extraction around a general CNF (conjunctive normal form) representation. OR, IN, NOT IN, BETWEEN, and compound disjunctions all become instances of the same structure.

## Context

`analyze_constraint_expr` in `src/query/constraints.rs` extracts column constraints from WHERE clauses for CDC invalidation filtering. Today it handles AND, comparisons, BETWEEN, IN, and NOT IN. OR is ignored.

The current `TableConstraint` enum is expressive enough for IN (same-column equality set) but not for general OR:

```rust
pub enum TableConstraint {
    Comparison(EcoString, BinaryOp, LiteralValue),
    AnyOf(EcoString, Vec<LiteralValue>),  // single-column, equality-only
}
```

Patterns that don't fit this shape:
- Same-table, different-column OR: `id = 1 OR name = 'alice'`
- OR with inequalities: `id > 5 OR id < 2`
- Compound OR branches: `(id = 1 AND name = 'x') OR (id = 2 AND name = 'y')`

Each would otherwise need its own structural extension. CNF subsumes all of them.

## Key Design Constraint: Cross-Table OR

OR branches that span multiple tables cannot be used for per-table CDC filtering. Consider:

```sql
SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id
WHERE o.status = 'urgent' OR c.vip = true
```

A CDC event on `orders` with `status = 'normal'` — the local branch fails, but the row could still be in the result set if the joined customer has `vip = true`. We can't evaluate the remote branch from a single table's CDC event.

**Rule**: A CNF clause is assignable to a table only when all of its literals reference the same table. Multi-table clauses are dropped — the other AND-connected clauses are still usable.

## CNF as the Canonical Form

CNF is conjunction of disjunctions: `(L1 ∨ L2) ∧ (L3 ∨ L4 ∨ L5) ∧ ...`. Under CNF:

- Plain comparison `id = 1` → one 1-literal clause `[id=1]`
- AND chain `id = 1 AND name = 'x'` → two 1-literal clauses `[id=1]`, `[name='x']`
- `IN (1,2,3)` → one 3-literal clause `[id=1, id=2, id=3]`
- `NOT IN (1,2,3)` → three 1-literal clauses `[id!=1]`, `[id!=2]`, `[id!=3]`
- BETWEEN → two 1-literal clauses (`[>=low]`, `[<=high]`)
- Simple OR `id = 1 OR id = 2` → one 2-literal clause `[id=1, id=2]`
- Cross-column OR `id = 1 OR name = 'x'` → one 2-literal clause `[id=1, name='x']`
- Inequality OR `id > 5 OR id < 2` → one 2-literal clause `[id>5, id<2]`
- Compound `(a ∧ b) ∨ c` → two clauses `[a, c]`, `[b, c]` (distribute OR over AND)

**Evaluation** (CDC row match): row matches iff every clause has at least one satisfied literal. One loop, no special cases.

**Why classical CNF over Tseytin**: Tseytin avoids exponential blowup in CNF size by introducing auxiliary variables. But Tseytin's aux variables are placeholders for subexpressions — they're not column-level literals we can evaluate against a CDC row. Our matching model (concrete row values, direct literal evaluation) needs real literals in every clause; aux-variable clauses would force tree-walk evaluation, giving up CNF's benefits. Tseytin fits SAT-solver consumers, not row-predicate evaluators.

The classical distribution cost is bounded at extraction time (see budget below).

## Three-Valued Logic Safety

SQL's WHERE clause evaluates predicates under Kleene 3VL and filters to rows where the predicate is TRUE (UNKNOWN counts as rejection). CNF conversion is safe under 3VL because the core identities hold in Kleene logic:

- Distributivity: `A ∨ (B ∧ C) ≡ (A ∨ B) ∧ (A ∨ C)`
- De Morgan: `¬(A ∧ B) ≡ ¬A ∨ ¬B`, `¬(A ∨ B) ≡ ¬A ∧ ¬B`

These don't depend on the law of excluded middle, which is the identity that fails in 3VL. CNF-converted WHERE clauses produce the same filtered row set as the original.

NULL literals in comparisons (which always evaluate to UNKNOWN) are already excluded at extraction time by the existing code — we only admit non-NULL `LiteralValue` into literals. CNF doesn't change this.

## Types

Keep `column_constraints` and `equivalences` as they are. Replace `TableConstraint` with a CNF clause type:

```rust
/// A CNF literal: column op value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Literal {
    pub column: EcoString,
    pub op: BinaryOp,
    pub value: LiteralValue,
}

/// A CNF clause — disjunction of literals. Interpreted as OR.
/// Single-literal clauses are the common case (plain comparisons).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Clause(pub Vec<Literal>);

pub struct QueryConstraints {
    pub column_constraints: HashSet<ColumnConstraint>,       // unchanged
    pub equivalences: HashSet<ColumnEquivalence>,            // unchanged
    pub table_constraints: HashMap<EcoString, Vec<Clause>>,  // CNF per table
}
```

`column_constraints` stays as a separate flat set of single-column facts, for use by equivalence propagation and other machinery that reasons about individual comparisons. Once CNF is well-understood, unifying the two can be revisited.

## CNF Conversion

One recursive function over the resolved WHERE AST, with an opacity-aware result type:

```rust
enum CnfResult {
    /// Successfully extracted — may be empty (no extractable clauses).
    Clauses(Vec<Clause>),
    /// Subtree contains a predicate we can't reason about.
    /// Propagates upward through OR; absorbed by AND.
    Opaque,
}

fn expr_to_cnf(expr: &ResolvedWhereExpr, budget: &mut usize) -> CnfResult
```

Rules:

| Input | Output |
|---|---|
| Literal `col op value` | `Clauses([[literal]])` |
| BETWEEN | `Clauses([[>= low], [<= high]])` |
| IN | `Clauses([[col=v1, col=v2, ...]])` — one clause |
| NOT IN | `Clauses([[col!=v1], [col!=v2], ...])` — AND of 1-literal clauses |
| AND(L, R) | Union of clauses; `Opaque` conjunct contributes nothing |
| OR(L, R) | Cartesian product — for each `(cl_l, cl_r)` pair, concat into one clause. Any `Opaque` branch forces `Opaque`. |
| Anything else (function call, subquery, opaque operator) | `Opaque` |

**Asymmetric handling of `Opaque` in AND vs OR is required for correctness:**
- Opaque conjunct in AND: the unknown subtree might be TRUE — dropping it gives us a weaker predicate, which means over-invalidation. Safe.
- Opaque branch in OR: the unknown branch might be TRUE — dropping it would incorrectly narrow the predicate and cause us to filter out rows that actually match. Must bail the entire OR.

**Budget**: the cartesian product in OR can expand exponentially. A running clause-count budget (e.g., 32 or 64 total clauses) bounds extraction cost. When OR would exceed the budget, return `Opaque` for that subtree — the AND context absorbs it safely. Real-world SQL rarely has the shape (wide AND-under-OR) that triggers blowup; when it does, bailing to "no constraints" is a correct, predictable fallback (degrades to today's "invalidate on any CDC event" behavior for that subtree).

## Per-Table Clause Assignment

After CNF, assign each clause to a table. Build CNF over `ResolvedColumnNode` (which carries table info), then strip to `EcoString` column names at assignment time:

```rust
fn clauses_by_table(
    clauses: Vec<ResolvedClause>,
) -> HashMap<EcoString, Vec<Clause>> {
    let mut by_table = HashMap::new();
    for clause in clauses {
        let tables: HashSet<_> = clause.literals.iter().map(|l| &l.column.table).collect();
        if tables.len() == 1 {
            let table = tables.into_iter().next().unwrap().clone();
            by_table.entry(table).or_default().push(clause.to_table_clause());
        }
        // multi-table clause: drop (can't use for per-table CDC filter)
    }
    by_table
}
```

Single-literal clauses (the common case, from AND of simple comparisons) always go to their one table.

## Impact on Existing Code

| Call site | Change |
|---|---|
| `analyze_constraint_expr` | Replaced by `expr_to_cnf`. Equivalence / `column_constraints` collection stays as a separate pass over the AST. |
| `in_constraints_extract`, `not_in_constraints_extract`, `between_constraints_extract` | Absorbed into CNF rules. Deleted. |
| `analyze_query_constraints` | Runs CNF + per-table assignment to populate `table_constraints`. |
| `propagate_constraints` | Propagate single-column clauses (all literals share a column with an equivalence → clone clause with the equivalent column). Multi-column clauses don't propagate. Matches current `AnyOf` behavior. |
| `row_constraints_match` in `src/cache/writer/cdc.rs` | One loop: every clause must have ≥1 matching literal. Collapses the current `Comparison` / `AnyOf` dispatch. |
| `table_constraints_subsumed` + `column_range_build` | Detect multi-column clauses in cached constraints → fail subsumption conservatively for v1. Single-column and equality-set clauses continue through the existing `ColumnRange` path. |

## Test Changes

Existing equality / inequality / IN / NOT IN / BETWEEN tests continue to pass — their CNF output is identical in shape to the current output.

Invert `test_no_propagation_with_or` (currently asserts 0 constraints for `id = 1 OR id = 2`; should assert 1 clause with 2 literals).

New tests:
- **Simple OR**: `id = 1 OR id = 2` → 1 clause `[id=1, id=2]`
- **Flattened chain**: `id = 1 OR id = 2 OR id = 3` → 1 clause with 3 literals
- **Cross-column same-table**: `id = 1 OR name = 'alice'` → 1 clause with 2 literals
- **Inequality OR**: `id > 5 OR id < 2` → 1 clause
- **OR + AND**: `(id = 1 OR id = 2) AND name = 'alice'` → 2 clauses
- **Compound**: `(id = 1 AND name = 'x') OR id = 2` → 2 clauses `[id=1, id=2]`, `[name='x', id=2]`
- **Nested compound**: `(a AND b) OR (c AND d)` → 4 clauses
- **Cross-table OR**: `a.id = 1 OR b.id = 2` → 0 constraints (multi-table clause dropped)
- **Opaque branch**: `id = 1 OR func(id)` → 0 constraints (OR with opaque branch)
- **Budget exhaustion**: construct a pathological WHERE that would exceed the budget; verify extraction returns no constraints for that subtree while other AND conjuncts still produce clauses
- **Propagation through JOIN**: `a JOIN b ON a.id = b.id WHERE a.id = 1 OR a.id = 2` → both tables receive the same-column 2-literal clause

## Files Modified

| File | Nature of change |
|------|-----------------|
| `src/query/constraints.rs` | New `Literal` / `Clause` types, CNF converter `expr_to_cnf`, per-table assignment, updated `propagate_constraints`, updated tests |
| `src/cache/writer/cdc.rs` | `row_constraints_match` rewritten as "every clause has ≥1 matching literal" |

## Suggested Phasing

**Phase 1 — Structural refactor only.** Introduce `Literal` and `Clause`. Rewrite `row_constraints_match`, `propagate_constraints`, `table_constraints_subsumed`, and `column_range_build` to consume `Vec<Clause>`. Keep extraction behavior identical: every current case produces 1-literal clauses or same-column multi-literal clauses. All existing tests pass unchanged.

**Phase 2 — Replace extraction with CNF conversion.** Swap `analyze_constraint_expr` for `expr_to_cnf` + per-table assignment. Existing tests still pass (IN, NOT IN, BETWEEN, AND chains produce identical CNF output). Add the OR test suite. Invert `test_no_propagation_with_or`.

**Phase 3 — Subsumption for same-column multi-literal clauses.** Route them through `ColumnRange::InSet` (already exists). Multi-column clauses remain conservatively not-subsumable.

**Phase 4 (optional) — Polish.** Clause dedup, tautology simplification, De Morgan pushdown for `NOT`.

Splitting phase 1 and phase 2 keeps each diff small and reviewable in isolation — phase 1 is pure mechanical refactor with no behavior change, phase 2 is where OR extraction turns on.

## Out of Scope

- **Tseytin transformation**: evaluated and rejected. Tseytin avoids exponential CNF blowup by introducing aux variables, but aux vars aren't column-level literals and can't be evaluated against CDC rows directly. Fits SAT-solver consumers, not our row-predicate evaluation model.
- **Tree-walk evaluation as the primary path**: evaluating the WHERE AST directly per row would avoid extraction entirely, but gives up canonical per-table clause structure that subsumption and efficient per-table CDC routing rely on. Could be a future fallback for subtrees that exceed the CNF budget, but not the primary design.
- **ANY / ALL array operators**: `id = ANY(ARRAY[1,2,3])` has IN-like semantics but a different AST shape. Handle as a follow-up in the extraction rules table.

## Verification

1. `cargo check`
2. `cargo test` — all existing tests pass unchanged; new OR and budget tests pass
3. `cargo clippy -- -D warnings`
