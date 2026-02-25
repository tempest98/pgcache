# Predicate Subsumption: Skipping Population for Covered Queries

## Context

When a new query is registered, pgcache populates the cache by executing it against the origin database. This is the most expensive part of registration — a network round-trip to origin, row transfer, and insertion into the cache DB.

In many cases, the data needed by a new query is already present in the cache, loaded by a previously registered query. If we can detect this, we can skip population entirely and serve the query from cache immediately.

**Goal**: At registration time, detect when a new query's result set is already covered by existing cached data and serve it immediately instead of populating from origin.

## The Idea: Predicate Subsumption

A new query Q_new is "subsumed" by an existing cached query Q_cached when every row that satisfies Q_new's predicates also satisfies Q_cached's predicates. In other words, Q_new's result set is a subset of Q_cached's result set, meaning all needed rows are already in the cache table.

Formally: `P_new -> P_cached` (the new predicate implies the cached predicate).

For conjunctive (AND-only) predicates, this simplifies to: every constraint in the cached query must be implied by the new query's constraints.

### Examples

**Full table scan covers everything:**
```sql
-- Cached
SELECT * FROM test
-- New (subsumed - all rows of test are already loaded)
SELECT * FROM test WHERE foo = 'bar'
```

**Multi-tenant narrowing:**
```sql
-- Cached
SELECT * FROM orders WHERE tenant_id = 1
-- New (subsumed - tenant_id = 1 rows are already loaded)
SELECT * FROM orders WHERE tenant_id = 1 AND status = 'active'
SELECT * FROM orders WHERE tenant_id = 1 AND created_at > '2024-01-01'
SELECT COUNT(*) FROM orders WHERE tenant_id = 1
```

**Range narrowing:**
```sql
-- Cached
SELECT * FROM events WHERE id > 5
-- New (subsumed - id > 10 implies id > 5)
SELECT * FROM events WHERE id > 10
```

## Subsumption Rules

The `QueryConstraints` infrastructure already decomposes WHERE clauses into `ColumnConstraint` sets (column, op, value triples). Subsumption checking operates on these.

### Equality (start here for v1)

For each equality constraint in the cached query, the new query must have a matching equality constraint on the same column with the same value.

- Cached `{tenant_id = 1}`, New `{tenant_id = 1, status = 'active'}` — cached is a subset of new's constraints, so new's rows are a subset of cached's rows. Subsumed.
- Cached `{}` (no WHERE), New `{anything}` — empty set is subset of everything. Subsumed.
- Cached `{tenant_id = 1}`, New `{tenant_id = 2}` — different values. Not subsumed.

### Inequality (future extension)

For each inequality constraint in the cached query, the new query must have a constraint that implies it:
- Cached `{id > 5}`, New `{id > 10}` — `id > 10 -> id > 5`. Subsumed.
- Cached `{id > 5}`, New `{id = 10}` — `id = 10 -> id > 5`. Subsumed.
- Cached `{id >= 5, id <= 100}`, New `{id BETWEEN 10 AND 50}` — narrower range. Subsumed.

### What Cannot Be Subsumed

- **OR predicates**: Constraint analysis correctly treats these as opaque (no constraints extracted).
- **LIMIT queries**: If the cached query has `max_limit = Some(n)`, it may not have all matching rows. Only consider cached queries where `max_limit` is None.
- **Different join structures**: A cached single-table query can't subsume a joined query.
- **Disjunctive coverage**: Cached `{tenant_id = 1}` + `{tenant_id = 2}` together cover `{tenant_id IN (1,2)}`, but detecting this requires reasoning about unions. Not worth the complexity.
- **Functions/expressions in predicates**: Constraint analysis only extracts column-vs-literal comparisons.

## How Generations Work (Important Context)

Generations are a GC mechanism, not a data visibility scope. All rows in a cache table are visible to all queries against it regardless of generation. The generation stamp on a row records when it was inserted. The GC heuristic: any row with a generation lower than the minimum active generation is unused and can be purged.

The pgcache_pgrx extension's CustomScan tracker updates row generations as a side-effect of query execution — when a query runs against the cache DB, accessed rows are stamped with the current `mem.query_generation`. This is already used by `cache_query_generation_bump()` during CLOCK eviction to re-stamp rows without re-populating.

For subsumption, this means: executing the subsumed query once against the cache DB is sufficient to stamp the relevant rows with the new generation, keeping them safe from GC. No explicit re-population or row copying needed.

## Proposed Flow

### At the Coordinator (query_cache.rs)

On cache miss, the coordinator currently sends `QueryCommand::Register` to the writer and forwards the query to origin. With subsumption:

1. Cache miss detected (no fingerprint match)
2. Forward query to origin as usual (don't block the client)
3. Send registration to writer as usual — but include a **oneshot channel** for the writer to respond on

### At the Writer (writer/query.rs)

During `query_register`, after resolving the query and extracting constraints:

1. Check subsumption: for each table in the query, look at existing cached queries where `max_limit` is None and `state` is Ready
2. Check if the new query's constraints imply the cached query's constraints (per table)
3. If **subsumed**:
   - Assign a new generation
   - Execute the query against the cache DB with `mem.query_generation` set (stamps rows, same as `cache_query_generation_bump`)
   - Register metadata (CachedQuery, update queries, etc.)
   - Send back resolved query + generation via the oneshot channel
4. If **not subsumed**:
   - Proceed with normal registration (origin population)
   - Drop the oneshot (coordinator ignores)

### Back at the Coordinator

If the oneshot resolves with a response:
- The query is already being served from origin for this first execution (forwarded above)
- Update `CacheStateView` to Ready with the resolved query + generation
- All subsequent executions are immediate cache hits

This means the very first execution still goes to origin (we forwarded before we knew), but every execution after that is served from cache with zero population latency. The alternative — waiting for the writer's response before deciding to forward — would add latency to the first execution on the coordinator thread, which we want to avoid.

## Key Implementation Points

### Subsumption Check Function

A function on `QueryConstraints` (or standalone) that checks whether one constraint set subsumes another. For v1, equality-only:

```rust
/// Returns true if every row matching `self`'s constraints also matches
/// `other`'s constraints. In other words, `other`'s constraint set is a
/// subset of `self`'s constraint set (more constraints = narrower result).
fn is_subsumed_by(&self, other: &QueryConstraints) -> bool {
    // Every constraint in `other` must appear in `self`
    other.column_constraints.is_subset(&self.column_constraints)
}
```

Note: this only works for equality constraints in v1. For inequality subsumption (e.g., `id > 10` implies `id > 5`), each constraint would need pairwise implication checking rather than simple set containment.

### Constraints on CacheStateView

The writer already has constraints via `UpdateQuery`. The subsumption check happens at the writer, which has full access to all cached query metadata. No changes to `CacheStateView` needed.

### Oneshot on QueryCommand::Register

Add an optional oneshot sender to `QueryCommand::Register` for the writer to respond when subsumption is detected:

```rust
QueryCommand::Register {
    fingerprint: u64,
    cacheable_query: Box<CacheableQuery>,
    search_path: Vec<String>,
    started_at: Instant,
    /// Writer responds here if the query is subsumed (skip population).
    /// Contains (generation, resolved) for the coordinator to update state.
    subsume_tx: Option<oneshot::Sender<SubsumeResult>>,
}
```

### Generation Stamping

Reuse the pattern from `cache_query_generation_bump()`:
```
SET mem.query_generation = <new_gen>
→ execute resolved query against cache DB (stamps rows as side-effect)
→ SET mem.query_generation = 0
```

## Value Proposition

The multi-tenant case alone justifies this. SaaS applications commonly have patterns like:
```sql
SELECT * FROM orders WHERE tenant_id = $1
SELECT * FROM orders WHERE tenant_id = $1 AND status = $2
SELECT * FROM orders WHERE tenant_id = $1 AND created_at > $2
SELECT COUNT(*) FROM orders WHERE tenant_id = $1
```

Once the first query populates for a given tenant, all subsequent queries on that tenant's data become instant cache registrations — one local query execution instead of an origin round-trip. This dramatically improves cache warm-up latency.

Similarly, small reference tables cached with `SELECT * FROM config` cover all future queries on that table immediately.
