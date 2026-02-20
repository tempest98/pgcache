# Correlated Subquery Support

A correlated subquery references columns from its outer query. Currently pgcache detects these at
resolution time and returns `CorrelatedSubqueryNotSupported`, causing the query to be forwarded to
origin. This document describes the plan to support them.

Work is split into two phases:

- **Phase 1** (this document): make correlated subqueries resolve successfully, capturing outer
  column references as metadata on the resolved `Subquery` node.
- **Phase 2** (deferred): use that metadata to drive correct CDC handling and cache invalidation.

## Background

### What is a correlated subquery?

A subquery is correlated when its expression tree references columns from the outer query's scope:

```sql
-- Correlated EXISTS: oi.order_id references the outer `o` table
SELECT * FROM orders o
WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id)

-- Correlated IN: user_id references the outer `u` table
SELECT * FROM users u
WHERE u.credit_limit > (SELECT SUM(total) FROM orders WHERE user_id = u.id)

-- Correlated scalar in SELECT list
SELECT d.name, (SELECT COUNT(*) FROM employees WHERE dept_id = d.id)
FROM departments d
```

### Join-equivalent vs. non-join-equivalent

Some correlated subqueries (typically EXISTS/IN) can be mechanically rewritten as JOINs. This
distinction matters for query executors performing decorrelation. For pgcache it is irrelevant —
both categories need the same treatment for caching and invalidation purposes — so they are handled
uniformly.

### What pgcache needs from a correlated subquery

- **Which inner tables does it depend on?** These drive CDC invalidation.
- **Is it cacheable?** Same rules as non-correlated subqueries.
- **Which outer columns does it reference?** Metadata needed for Phase 2.

## Phase 1: Resolution

### Representation

The `ResolvedWhereExpr::Subquery` variant gains an `outer_refs` field:

```rust
Subquery {
    query: Box<ResolvedQueryExpr>,
    sublink_type: SubLinkType,
    test_expr: Option<Box<ResolvedWhereExpr>>,
    /// Columns from the outer query scope referenced within this subquery.
    /// Empty for non-correlated subqueries.
    outer_refs: Vec<ResolvedColumnNode>,
},
```

The inner `query` is a fully resolved `ResolvedQueryExpr`. Outer column references appear inside it
as regular `Column(ResolvedColumnNode)` nodes — `ResolvedColumnNode` is just data (schema, table,
column name, type) and carries no inherent "I am in scope" information. The `outer_refs` field is
the only thing that marks them as correlated; it is a collected set of those same nodes for
inspection by downstream code.

`nodes()` for the `Subquery` variant should **not** traverse `outer_refs` separately. The same
`ResolvedColumnNode`s are already reachable through `query`'s expression tree; including both
would double-count.

### Why downstream code needs no changes (for now)

**Table dependency tracking** walks `ResolvedTableNode`s from FROM clauses, not column refs from
expression trees. Outer column refs appearing as `Column` nodes inside the inner query's WHERE
clause do not register as new inner-table dependencies — the outer table is not in the inner FROM,
so it never produces a `ResolvedTableNode` in the inner scope.

**Constraint extraction** (`constraints.rs`) already has `ResolvedWhereExpr::Subquery { .. } => {}`
— it skips the entire subquery body. Correlated predicates inside the inner query are already
ignored.

**Cacheability** (`cache/query.rs`) operates on the unresolved `WhereExpr` AST. Correlation is a
resolution-time concept, invisible there. The existing subquery cacheability path applies unchanged.

### Files to modify

| File | Change |
|---|---|
| `src/query/resolved.rs` | Add `outer_refs` field; update `ResolutionScope`; rewrite `subquery_resolve`; thread sink through expression resolvers; remove `CorrelatedSubqueryNotSupported` |
| `src/proxy/query.rs` | Remove forwarding logic for `CorrelatedSubqueryNotSupported` |

### `ResolutionScope` — outer table snapshot

Add an owned snapshot of outer-scope tables:

```rust
struct ResolutionScope<'a> {
    // existing fields...
    /// Owned snapshot of outer scope tables for correlated reference fallback.
    /// Populated when entering a subquery; empty at top level.
    outer_tables: Vec<(TableMetadata, Option<String>)>,
}
```

Owned clone avoids lifetime stacking — `TableMetadata` is `Clone`. `ResolutionScope::new`
initialises the field as empty. A constructor (e.g. `new_inner`) builds a child scope with
`outer_tables` set from the parent.

For nested correlated subqueries, `outer_tables` must be the union of the parent's `tables` and
the parent's own `outer_tables`, so that a doubly-nested subquery can reach any ancestor level.

### Sink parameter

Add `outer_refs: &mut Vec<ResolvedColumnNode>` to all expression-resolving functions that may
encounter a correlated reference inside a subquery context:

- `where_expr_resolve`
- `select_columns_resolve` (SELECT list can contain correlated refs)
- `having_resolve`
- join condition resolvers

At top-level resolution entry points (`query_expr_resolve`, `select_node_resolve`), pass a local
empty `Vec` as the sink. After the call it is expected to be empty; no correlated refs exist at
the top level.

### `subquery_resolve` — rewrite

Replace the current error-interception logic with:

1. Snapshot current scope's `tables` and `outer_tables` into an owned `outer_tables` for the child
   scope (union, as described above).
2. Construct the child `ResolutionScope` with that snapshot.
3. Create a local `outer_refs: Vec<ResolvedColumnNode>`.
4. Run inner resolution passing `&mut outer_refs` as the sink.
5. Return the resolved inner query with `outer_refs` attached to the `Subquery` variant.

When column resolution fails in the child scope, fall back to `outer_tables`. On success, push the
resolved `ResolvedColumnNode` to the sink and return it as a normal `Column(resolved_col)` node.

### Remove `CorrelatedSubqueryNotSupported`

The error variant, its two detection sites in `subquery_resolve`, and the upstream forwarding
logic in `proxy/query.rs` are all deleted.

### Tests

Convert the six existing `test_correlated_*_detected` tests: instead of asserting
`CorrelatedSubqueryNotSupported`, assert successful resolution and check that `outer_refs`
contains the expected `ResolvedColumnNode`s.

Add new tests:
- Correlated ref in SELECT list (scalar subquery)
- Correlated ref in HAVING clause
- Non-correlated subquery has `outer_refs: []`
- Doubly-nested correlated subquery (inner references grandparent scope)
- Mixed: inner scope column and outer scope column in the same predicate

## Phase 2: CDC and Invalidation (deferred)

Phase 2 will determine how `outer_refs` and inner table dependencies of correlated subqueries feed
into cache invalidation. Questions to resolve:

- Does `SubqueryKind` determination need changes for correlated vs. non-correlated subqueries?
  The `SubLinkType` still determines inclusion/exclusion/scalar semantics, so likely no change
  is needed — but this needs verification against the CDC event processing path.
- When an inner table of a correlated subquery changes, the existing `Subquery(SubqueryKind)`
  tracking likely handles it correctly. Verify this is true by tracing a CDC event through the
  invalidation path for a correlated subquery.
- Changes to outer query rows affect correlated subquery results. These are already covered by
  the outer table's CDC tracking, but the interaction between outer row changes and cached
  subquery-dependent results needs explicit reasoning.
