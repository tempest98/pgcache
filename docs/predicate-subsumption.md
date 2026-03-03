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
- **LIMIT queries**: If the cached query has a LIMIT (`has_limit` is true on its `UpdateQuery`), it may not have all matching rows. Only consider `UpdateQuery` entries where `has_limit` is false.
- **Different join structures**: A cached single-table query can't subsume a joined query.
- **Disjunctive coverage**: Cached `{tenant_id = 1}` + `{tenant_id = 2}` together cover `{tenant_id IN (1,2)}`, but detecting this requires reasoning about unions. Not worth the complexity.
- **Functions/expressions in predicates**: Constraint analysis only extracts column-vs-literal comparisons.

## How Generations Work (Important Context)

Generations are a GC mechanism, not a data visibility scope. All rows in a cache table are visible to all queries against it regardless of generation. The generation stamp on a row records when it was inserted. The GC heuristic: any row with a generation lower than the minimum active generation is unused and can be purged.

The pgcache_pgrx extension's CustomScan tracker updates row generations as a side-effect of query execution — when a query runs against the cache DB, accessed rows are stamped with the current `mem.query_generation`. This is already used by `cache_query_generation_bump()` during CLOCK eviction to re-stamp rows without re-populating.

For subsumption, this means: executing the subsumed query once against the cache DB is sufficient to stamp the relevant rows with the new generation, keeping them safe from GC. No explicit re-population or row copying needed.

## Proposed Flow

### At the Coordinator (query_cache.rs)

On cache miss, the coordinator currently forwards the query to origin and sends `QueryCommand::Register` to the writer. With subsumption, the coordinator **defers the origin forward** to give the writer a chance to detect subsumption:

1. Cache miss detected (no fingerprint match)
2. Set state to `Loading` in `CacheStateView` (so request coalescing can queue subsequent arrivals)
3. **Do not forward to origin yet** — push the `QueryRequest` into the coalescing waiting queue as the first waiter (see `request-coalescing.md`)
4. Send `Register` to the writer
5. Continue processing other messages in the `select!` loop — the coordinator never blocks

The held first request sits in the waiting queue alongside any subsequent requests that arrive while the writer processes registration.

### At the Writer (writer/query.rs)

During `query_register`, after resolving the query and extracting constraints:

1. **Check subsumption per table**: for each table in the new query (identified by relation OID), look up existing `UpdateQuery` entries in `self.cache.update_queries` for that OID. Only consider entries where `has_limit` is false and the parent `CachedQuery` state is `Ready`.
2. For each such `UpdateQuery`, check if the new query's constraints on that table imply the cached query's constraints (i.e., the cached constraints are a subset of the new constraints — see Subsumption Check Function below).
3. **All tables must be covered**: subsumption succeeds only if every table referenced by the new query has at least one covering `UpdateQuery`.
4. If **subsumed**:
   - Assign a new generation
   - Execute the query against the cache DB with `mem.query_generation` set (stamps rows, same as `cache_query_generation_bump`)
   - Register metadata (CachedQuery, update queries, etc.)
   - Update `CacheStateView` to `Ready`
   - Send `WriterNotify::Registered { fingerprint, subsumed: Some(SubsumeResult { ... }) }`
5. If **not subsumed**:
   - Send `WriterNotify::Registered { fingerprint, subsumed: None }` **immediately** (before dispatching population)
   - Proceed with normal registration (origin population)

The `Registered` notification is always sent promptly — before population is dispatched. This gives the coordinator a fast answer about the held first request's fate.

If the cache DB query for generation stamping fails (e.g., cache table was dropped by concurrent eviction), send `Registered { subsumed: None }` and fall back to normal population.

### Back at the Coordinator

The coordinator handles `WriterNotify::Registered` in its `select!` loop (see `request-coalescing.md` for the full event loop):

- **Subsumed** (`subsumed: Some(...)`): state is already `Ready` (writer set it). Drain the entire waiting queue (first request + any coalesced requests) via `worker_request_send` — all served from cache. No origin round-trip.
- **Not subsumed** (`subsumed: None`): pop the first waiter from the queue and forward it to origin via `reply_forward`. State remains `Loading`. Remaining coalesced waiters stay in the queue and drain when `WriterNotify::Ready` arrives from population.

This is critical for queries that are called infrequently — potentially only once. Forwarding to origin eagerly would mean the only execution misses the cache. By deferring the forward, subsumption can serve even the first execution from cache.

**Latency tradeoff**: Every first-miss now waits for the writer to process registration and check subsumption before being forwarded (if not subsumed). This adds a few milliseconds (resolve + constraint analysis + subsumption check) to the non-subsumed path. This is small relative to a typical origin round-trip and is the cost of giving subsumption a chance to skip origin entirely.

### Shared Writer→Coordinator Channel

Both subsumption and request coalescing use the same `WriterNotify` channel (see `request-coalescing.md`). No per-Register oneshot is needed. The `Registered` variant carries the subsumption result; the `Ready` and `Failed` variants handle population completion:

```rust
enum WriterNotify {
    /// Registration processed — subsumption check complete.
    /// Sent immediately after the check, before population dispatch.
    Registered {
        fingerprint: u64,
        /// Some if subsumed (query is immediately Ready).
        /// None if not subsumed (population dispatched, first request should be forwarded).
        subsumed: Option<SubsumeResult>,
    },
    /// Population completed — query is Ready.
    Ready {
        fingerprint: u64,
        generation: u64,
        resolved: SharedResolved,
        max_limit: Option<u64>,
    },
    /// Population failed.
    Failed {
        fingerprint: u64,
    },
}
```

The coordinator's `select!` loop handles all three variants. See `request-coalescing.md` for the full event loop.

## Key Implementation Points

### Subsumption Check Function

The check must be **per-table**, not across the full `column_constraints` set. A cross-table `is_subset` check would incorrectly conclude subsumption when one table is covered but another isn't (e.g., a cached `orders` query would appear to subsume a join of `orders` and `items`).

The `table_constraints` field on `QueryConstraints` is already grouped by table name — use it for the comparison:

```rust
/// Returns true if all of `cached`'s constraints on `table` are implied by
/// `new`'s constraints on the same table. For v1, equality-only: every
/// (column, op, value) triple in `cached` must appear in `new`.
fn table_constraints_subsumed(
    new: &QueryConstraints,
    cached: &QueryConstraints,
    table: &str,
) -> bool {
    let cached_for_table = cached.table_constraints.get(table);
    let new_for_table = new.table_constraints.get(table);

    match (cached_for_table, new_for_table) {
        // Cached has no constraints on this table → all rows loaded. Subsumed.
        (None, _) => true,
        // Cached has constraints but new doesn't → new is broader. Not subsumed.
        (Some(cached_cs), None) => cached_cs.is_empty(),
        // Both have constraints → every cached constraint must appear in new.
        (Some(cached_cs), Some(new_cs)) => {
            cached_cs.iter().all(|c| new_cs.contains(c))
        }
    }
}
```

The outer check iterates all tables in the new query and requires each to be covered:

```rust
/// Check if an existing UpdateQuery covers the new query's constraints
/// for a specific table.
fn query_subsumption_check(
    new_constraints: &QueryConstraints,
    update_queries: &[UpdateQuery],
    table: &str,
) -> bool {
    update_queries.iter().any(|uq| {
        !uq.has_limit && table_constraints_subsumed(new_constraints, &uq.constraints, table)
    })
}
```

Note: this only works for equality constraints in v1. For inequality subsumption (e.g., `id > 10` implies `id > 5`), each constraint would need pairwise implication checking rather than simple set containment.

### Lookup Path

The subsumption check happens at the writer, which has full access to all cached query metadata. The lookup goes through `self.cache.update_queries` (an `IdHashMap<UpdateQueries>` keyed by relation OID). For each table in the new query, look up the `UpdateQueries` entry by OID and check its `queries: Vec<UpdateQuery>` for a covering entry. No changes to `CacheStateView` needed.

### Generation Stamping

Reuse the pattern from `cache_query_generation_bump()`:
```
SET mem.query_generation = <new_gen>
→ execute resolved query against cache DB (stamps rows as side-effect)
→ SET mem.query_generation = 0
```

### Observability

Track subsumption for visibility into the optimization's impact:

- **Counter**: `cache_subsumptions` — number of queries that skipped population via subsumption
- **Histogram**: `cache_subsumption_latency_seconds` — time from subsumption check through generation stamping (should be low single-digit milliseconds)

Without instrumentation, subsumption is invisible — queries silently skip population and appear as normal cache hits.

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
