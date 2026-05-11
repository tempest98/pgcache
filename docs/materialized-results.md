# Materialized Query Results

## Context

pgcache today caches source rows and re-evaluates queries against those cached rows at serve time. For most queries this is efficient, but for queries that reduce or transform a large input into a small output — aggregates, window functions, heavy filtering — the source-row cache stores far more data than needed and pays the re-evaluation cost on every hit.

Example: `SELECT count(*) FROM large_table` caches every row of `large_table` to return a single integer on each serve.

**Goal**: For queries that benefit from storing the result rather than re-computing it, maintain a second tier of cache — a materialized result — alongside the existing source-row cache. Serve from the materialized result when it is fresh; fall back to re-evaluating over source rows when it is stale.

---

## Design

### Storage model: result tables, not PG materialized views

Materialized results live in their own tables in the cache database, managed by pgcache the same way source-row cache tables are managed. We do not use PostgreSQL's `MATERIALIZED VIEW` feature.

Reasons:
- `REFRESH MATERIALIZED VIEW` is an origin-side operation and doesn't compose with pgcache's CDC-driven invalidation.
- Result tables integrate with the existing population + CDC invalidation infrastructure in pgcache's writer.
- Full control over population, stamping, and drop/truncate semantics. (MV tables themselves are not `pgcache_pgrx`-tracked — their consistency is managed via `MvState` transitions, not per-row generation stamps. See Serve path → Worker execution.)

### Table naming and layout

Source-row cache tables use the origin's schema and table name (e.g. origin's `public.orders` becomes `public.orders` in the cache DB, as an UNLOGGED table). MVs have no origin-assigned name, so they use a distinct convention:

- **Schema**: `pgcache_mv`.
- **Table name**: `q_<fingerprint>`, where `<fingerprint>` is the `u64` rendered as decimal.

Example: `pgcache_mv.q_18446744073709551615`.

Rationale:

- A dedicated schema isolates MVs from any possible origin table name, and makes MVs trivially enumerable for size-accounting and debugging (`SELECT ... FROM information_schema.tables WHERE table_schema = 'pgcache_mv'`).
- The `q_` prefix keeps the identifier unquoted-safe (PG requires a letter/underscore first).
- Decimal matches how fingerprints appear in tracing logs and metrics, so a log line like `cdc invalidating query 18446744073709551615` directly points at `pgcache_mv.q_18446744073709551615`.

Table properties:

- **UNLOGGED**, matching source-row cache tables. MVs are fully reconstructible from the source-row cache, so skipping WAL overhead is free correctness-wise and saves write bandwidth.
- **No primary key, no indexes, no generation-tracking triggers.** An MV is written once per rebuild and read whole (`SELECT * FROM mv_table [ORDER BY ...] [LIMIT user_limit]`); indexes would add write cost without read benefit. Targeted indexes for specific shapes (e.g. a GROUP BY + ORDER BY where a server-side sort is worth amortizing) can be added later.
- **Schema reset on startup**: `pgcache_mv` is created (if not exists) during `cache_database_reset()` along with the rest of the cache DB bootstrap.

Column names are preserved from `CREATE TABLE AS ... SELECT ...` — PG derives them from SELECT-list aliases or expression defaults (`count`, `sum`). Unaliased expressions like `SELECT 1 + 1` produce `?column?`, which `CREATE TABLE AS` accepts but is awkward; auto-aliasing to `_col1`, `_col2` at MV creation is a defer-able cleanup (also flagged in Serve path).

### Coexistence with source-row cache

A query eligible for materialization has **both** a source-row cache and a materialized result. The source-row cache is the invariant layer that always represents the truth; the materialized result is a derived projection that may be fresh or dirty at any given moment.

This coexistence matters for the serve path: when the materialized result is dirty, the coordinator falls back to evaluating the query over the source-row cache — no regression versus today's behavior.

### Eligibility

Eligibility is a three-way classification set at registration based on query shape, stored on the cache entry:

```rust
enum ShapeGate {
    /// Materialize unconditionally; skip the size gate.
    Materialize,
    /// Shape suggests reduction is possible; apply the size gate at first population.
    Measure,
    /// Shape rules out benefit; never materialize.
    Skip,
}
```

**v1 classification:**

| Outcome | Shapes |
|---------|--------|
| `Materialize` | Window function present. |
| `Measure` | Aggregate in SELECT list, GROUP BY / HAVING, DISTINCT, set operations other than `UNION ALL`. |
| `Skip` | Plain filter/projection, `UNION ALL`, ORDER BY + LIMIT (without a Measure shape), everything else. |

`Materialize` is reserved for window functions because they are the only shape that justifies materialization on pure compute grounds, even when the result is the same size as the source (e.g. `SELECT *, row_number() OVER (...) FROM t`). Recomputing the partition/sort on every serve is expensive regardless of cardinality.

`Measure` applies a size gate at first population: `result_rows × mv_size_ratio ≤ source_rows`. The shapes in this bucket are ones that *may* reduce meaningfully (an aggregate over many distinct groups, a DISTINCT with few unique values) — the only way to tell is to measure.

`mv_size_ratio` is a dynamic setting (default `10`). It plugs into the existing `DynamicConfig` / `DynamicConfigPatch` / `DynamicConfigHandle` machinery (`settings.rs:149`), which already supports wait-free `ArcSwap` reads and patch-based reload. Implementation checklist:

- Add `mv_size_ratio: u32` to `DynamicConfig` and `mv_size_ratio: Option<u32>` to `DynamicConfigPatch`.
- Update `DynamicConfig::new()` and `DynamicConfigPatch::apply()` to thread it through.
- Add matching TOML field, CLI arg, and env var (new settings are added to all three — env, TOML, CLI).
- Read at the gate check via `self.cache.dynamic.load().mv_size_ratio`.

**Interaction with sticky classification**: a reload changes behavior for *future* first-population decisions only. Existing `Ineligible` entries stay ineligible; existing `Fresh` entries stay `Fresh`. Retuning `mv_size_ratio` doesn't churn existing MVs — an entry only re-evaluates the gate after eviction + re-registration. This matches operator expectations (tuning shouldn't cause storage churn) and is consistent with the sticky-classification rule.

Per-query overrides remain future work.

`Skip` covers shapes where materialization is either guaranteed not to help or not yet handled:

- Plain filter / projection — the source-row cache already stores only the relevant rows; the MV would be a duplicate.
- `UNION ALL` — strictly additive; branches are already cached and concatenating them at serve time is two seq scans. MV would duplicate storage without saving compute. Other set operations (`UNION` / `INTERSECT` [`ALL`] / `EXCEPT` [`ALL`]) are `Measure` — they reduce the result and benefit from materialization when the reduction is large enough to pass the size gate. Serve-path ORDER BY for a set-op MV is emitted as a direct `Identifier` deparse (the resolver makes set-op ORDER BY Identifier-based, and those bare names match the MV column names derived from the leftmost branch's SELECT list).
- ORDER BY + LIMIT (without a Measure shape) — the source-row cache already stores the bounded row set; `SELECT * FROM mv` would duplicate what's already on disk. Covered in LIMIT interaction.
- Multi-table joins, expensive predicates (`LIKE '%foo%'`, full-text, regex) — on their own these don't reduce cardinality; when they do, it's because of a `Measure` shape layered on top (aggregate, DISTINCT, etc.) and that shape makes the decision.

Classification runs against the **decorrelated, resolved form** of the query, not the raw AST. Correlated subqueries are rewritten at registration (`decorrelate.rs`) — the shape we care about is the shape we actually populate and serve against.

**ORDER BY viability check**: after the primary classification, a query with non-empty top-level `order_by` is downgraded to `Skip` unless every ORDER BY expression can be served against the MV. For SELECT bodies this means each expression is structurally present in the SELECT list (positional lookup at serve time). For set-op bodies each ORDER BY must be an `Identifier` whose name matches an output column of the leftmost SELECT. In either case, a sort key that doesn't map would be lost at serve time — returning an arbitrary subset for user LIMIT < max_limit. Matching is pure structural / name equality; see Future Work → "ORDER BY matching improvements" for planned extensions.

Classification is **sticky** — we do not re-check on invalidation. Once a `Measure` query passes or fails the size gate at first population, that decision holds for the life of the cache entry. If the underlying data shifts dramatically, the decision only changes after the entry is evicted and re-registered.

**Skip → Measure candidates for later passes:**

- `ORDER BY` + `LIMIT` — revisit alongside the `max_limit` / LIMIT-sharing interaction.

### Parameters

Parameterized queries require no special handling. Parameters are substituted before fingerprinting, so each binding is its own cache entry and its own materialization candidate.

---

## Population

Runs in the writer, triggered by the coordinator on a cache hit that observes `mv_state == Pending { .. }` (see Serve path → Coordinator dispatch for the state flip). MV build is never in the request-critical path — the first hit that triggers the build falls through to source-row evaluation while the writer works behind it. The *next* hit after the build completes takes the MV fast path.

This coordinator-driven flow is uniform across first-build and rebuild, and across subsumed and non-subsumed queries: everyone arrives at `state = Ready, mv_state = Pending { has_table }` and waits for a cache hit to trigger the build. Pinned queries are the one exception — see **Pinned queries** below.

### Execution model (v1: sync in writer)

First-pop and rebuild run **synchronously in the writer task**, on the writer's own cache-DB connection. CDC event processing happens on the same task, so MV builds and CDC applies are naturally serialized: no Path A race, no need for `Building` / `BuildingDirty` states, and the first-population state re-check (below) only needs to cover the cross-task boundary where source-row population (pool) returns to the writer.

**Known trade-off**: long-running MV builds block CDC event processing for their duration, growing replication slot lag. In the common case this is acceptable — `Measure`'s size gate caps MV size at `source_rows / mv_size_ratio`, and `Materialize` (window functions) is rare. We ship a `CACHE_MV_BUILD_DURATION` histogram so operators can correlate build durations with slot-lag spikes.

If real workloads show meaningful lag impact, the follow-up is Option B: off-thread builds with `Building` / `BuildingDirty` (and `Rebuilding` / `RebuildingDirty`) race detection. See Future work. Shipping v1 as sync-in-writer deliberately — it validates the rest of the design without the state-machine complexity, and the move to off-thread is a self-contained change afterward.

### Data source

The MV is populated from the source-row cache, not from origin. This avoids a second origin round-trip and matches the rebuild path (where origin is not consulted). Because the source-row cache is kept consistent with origin via CDC, evaluating the query against the cache produces the same result as evaluating against origin.

### Build

Coordinator sees `mv_state == Pending { has_table }` on a cache hit, transitions to `Scheduled { has_table }`, and sends `QueryCommand::MvBuild { fingerprint }`. The writer's handler branches on `has_table`:

**`has_table = false` (first build).** Run the Measure gate (no-op for Materialize), then `CREATE UNLOGGED TABLE mv_table AS <deparsed-resolved> LIMIT max_limit` wrapped with SET/RESET of `mem.query_generation`. The gate:

- Compute `source_rows` by summing `count(*)` over the cache tables referenced by the resolved query.
- Run `SELECT count(*) FROM (<query> LIMIT max_limit)` to get `result_rows`.
- If `result_rows × mv_size_ratio ≤ source_rows`, proceed; otherwise transition to `Ineligible` (terminal).

Count-first is preferred over "populate, count, drop if ineligible": the extra count query is cheap for ineligible queries, while materializing-then-dropping can waste significant work on large results. Appending `LIMIT max_limit` to the count keeps the gate consistent with what would actually be stored.

**`has_table = true` (rebuild).** No gate (classification is sticky). Run a single transaction:

```sql
BEGIN;
SET mem.query_generation = <gen>;
TRUNCATE mv_table;
INSERT INTO mv_table SELECT ... FROM <cache tables> LIMIT <max_limit>;
COMMIT;
SET mem.query_generation = 0;
```

The table persists across rebuilds — we do not drop and recreate. Wrapping TRUNCATE and INSERT in one transaction matters for reader correctness — see **Invalidation and rebuild** below.

After either build variant succeeds, the writer transitions `Scheduled { .. } → Fresh`.

### Build state re-check (first-build only)

The count query and the `CREATE TABLE AS` run asynchronously on the cache DB and are not transactionally wrapped. During that work, a CDC event can flip the source-row cache to `Invalidated` or `Loading`. If we commit the MV anyway, its contents reflect pre-CDC source-row state — not stale in the write-ordering sense, but misaligned with current cache state.

Writer re-checks `CachedQueryView.state` immediately after the first build:

- Source-row state is still `Ready`: commit the MV → `Fresh`.
- Otherwise: `DROP TABLE IF EXISTS mv_table`, transition back to `Pending { has_table: false }`. The next cache hit re-dispatches.

Rebuild doesn't need this re-check — the full `BEGIN; TRUNCATE; INSERT; COMMIT;` is MVCC-atomic, and the overlap with CDC is the same "reads that overlap an invalidation" accommodation described in **Invalidation and rebuild**. Writer is single-threaded, so the re-check is a simple read of the same `CachedQueryView` entry — no lock dance.

### Pinned queries

Pinned queries get a thin bootstrap hook in the Ready handler so they stay warm without waiting for a user hit. After `query_ready_mark`, `mv_pinned_bootstrap` checks `mv_state`; if it's `Pending { has_table }`, self-transition to `Scheduled { has_table }` and send `MvBuild` on the writer's own queue. Build runs on the next tick of the writer loop.

The writer handler is the same one the coordinator drives for non-pinned queries — the bootstrap just performs the state flip that the coordinator would otherwise do on first hit. One code path for building/rebuilding MVs, without losing the "pinned queries stay warm" contract.

### Storage accounting

MV table bytes count toward the cache size total via `pgcache_total_size()` (`pgcache_pgrx/src/generation.rs`), which sums tracked source tables (those carrying the `pgcache_track_modification` trigger) UNION tables in the `pgcache_mv` schema. A large MV therefore counts against `cache_size` and triggers eviction the same way source-row growth does.

`pgcache_purge_rows` does **not** touch MV tables — it only acts on dshash-tracked source rows. MV cleanup is owned by pgcache's writer: `cache_query_evict` (`writer/cdc.rs`) calls `mv_drop` (`writer/mv.rs`), which executes `DROP TABLE IF EXISTS pgcache_mv.<table>`. Cluster-wide reset (`cache_database_reset` in `runtime.rs`) sweeps the whole `pgcache_mv` schema as a side effect of `DROP DATABASE`.

---

## Serve path

MV reads go through the same worker pool as source-row cache reads — the coordinator keeps its "read state, dispatch work" invariant, and the worker's existing streaming, protocol encoding, pipeline handling, and connection management are reused unchanged.

### Coordinator dispatch

On a cache hit the coordinator reads `CachedQueryView.mv_state` and picks the path:

- `Fresh` → dispatch a worker request with `mv_source = true` (fast path).
- `Pending { has_table }` → dispatch with `mv_source = false`. Transition `Pending { has_table } → Scheduled { has_table }` and send `MvBuild` to the writer. `has_table` is preserved through the flip so the writer knows whether to first-build or rebuild.
- `Scheduled { .. }` → dispatch with `mv_source = false`. Do not re-send; the writer is already on it.
- `Skipped` / `Ineligible` → dispatch with `mv_source = false`. Terminal states; no MV will ever exist.

The first request after registration (or invalidation) pays no extra latency — it sees exactly what it would have seen without the materialization feature. Subsequent requests after the build completes get the fast path.

The `Pending → Scheduled` transition uses a check-and-transition pattern under the DashMap write guard, so concurrent dispatches see at most one `MvBuild` command per state transition. `Fresh` and terminal states use only a shared guard to keep the hot path cheap.

Table name is derived from the fingerprint (`pgcache_mv.q_<fingerprint>`), not stored on `CachedQueryView`. The coordinator formats it at dispatch time.

### Worker execution

The worker request carries one new field:

```rust
/// When `true`, read from the materialized result table for this fingerprint
/// instead of deparsing the resolved query against source-row cache tables.
pub mv_source: bool,
```

The fingerprint is already on the request (keyed by the same u64 used to name the MV), so the worker reconstructs the table name locally.

SQL construction in `handle_cached_query_{text,binary}` branches on `mv_source`:

- `false` (existing path): deparse the resolved query against source-row cache tables, wrap with `SET mem.query_generation = N;`.
- `true`: `SELECT * FROM {mv_table} {ORDER BY N ASC|DESC, ...}? {LIMIT ...}?`, **no generation SET**.
  - ORDER BY is emitted **positionally** — `ORDER BY 2 DESC`, where the number is the 1-based position of the matching expression in the resolved SELECT list. The MV table's columns are named by `CREATE TABLE AS`, so source-qualified refs (`public.orders.status`, `count(orders.id)`) don't resolve against it; positional ORDER BY sidesteps naming entirely. The classifier guarantees a position exists for every ORDER BY expression (see "Eligibility").
  - Essential for common Measure shapes like `GROUP BY col ORDER BY count(*) DESC LIMIT 10`, where serve-time ordering is load-bearing: `SELECT * FROM mv` returns rows in arbitrary physical order, so for user LIMIT < max_limit only re-applying ORDER BY guarantees the correct top-M subset.
  - PG performs the sort on the (smaller) MV rather than re-sorting the full source-row cache — still a net win.
  - The incoming query's `LIMIT` is appended after the ORDER BY, via the same code path as today.

The MV stores up to `max_limit` rows, and user LIMITs are always ≤ `max_limit` by the existing invariant, so there's no overshoot risk.

Skipping the generation SET on the MV path is correct because MV tables are not `pgcache_pgrx`-tracked. Their consistency is managed entirely via `mv_state` transitions and rebuild transactions, not per-row generation stamps.

### What this inherits unchanged

- **Binary/text formats**: orthogonal to the MV branch — both paths benefit equally.
- **Pipeline handling** (`has_parse` / `has_bind` / `has_sync` / `pipeline_describe`): operates on the constructed SQL string, doesn't care how it was produced.
- **Column names / `RowDescription`**: `CREATE TABLE mv AS SELECT ...` preserves aliases and PG-default names (`count`, `sum`), so `SELECT * FROM mv` produces a `RowDescription` matching what the original query would emit.
- **Request coalescing**: the existing coalescing key is computed per-request from protocol params. Since `mv_source` is derived from `CachedQueryView.mv_state` at dispatch time (same for all waiters on the same fingerprint at that moment), it doesn't need to enter the key.

### Edge case: unaliased expressions

Expressions without aliases (`SELECT 1 + 1`) produce `?column?` in PG. `CREATE TABLE AS` accepts this but it's awkward as a stored-column name. We may want to auto-alias at MV creation (`_col1`, `_col2`) for cleanliness — minor, can defer.

---

## Invalidation and rebuild

### Invalidation is cheap — just a state flip

When any event invalidates a query's materialized result, the writer transitions `mv_state: Fresh → Pending { has_table: true }`. **No TRUNCATE, no DROP.** The stale MV rows stay on disk until the rebuild transaction replaces them, or until eviction drops the table.

Invalidation triggers:
- **CDC event** affecting a source table of the query.
- **LimitBump** — `max_limit` increases, so the existing MV is short of rows.

We do **not** rebuild immediately. Rebuild is triggered by the next serve.

This is the key property of the design: on write-heavy tables with rarely-read queries, the materialized result simply stays dirty most of the time and costs nothing (beyond the dead bytes on disk, accounted via `pgcache_total_size()`). On read-heavy queries with stable underlying data, the result stays warm between invalidations and serves fast. The system self-tunes without any explicit activation heuristic.

### Why defer the TRUNCATE

Two alternatives were considered and rejected:

1. **TRUNCATE at invalidation time (separate from rebuild)**: opens a window where a worker dispatched with `mv_source = true` can race the TRUNCATE and return an empty result set to the client. PG's AccessExclusive lock serializes against the SELECT, but if the worker's SELECT starts *after* TRUNCATE commits, it sees an empty relfilenode. The coordinator thought the MV was fresh; the client gets zero rows.
2. **DROP + CREATE**: same locking issue, plus churn on the catalog.

Folding TRUNCATE into the rebuild transaction (`BEGIN; TRUNCATE; INSERT; COMMIT`) is MVCC-correct: a worker's SELECT either runs before the commit (sees the pre-rebuild rows) or after (sees the fresh rows). Never an empty intermediate state.

### Reads that overlap an invalidation

A request dispatched when `mv_state == Fresh` may have its worker SELECT execute *after* a CDC event flips the state to `Pending { has_table: true }`. That SELECT returns rows from the pre-event MV. The write-ordering here is fine — the read began before the invalidating change landed, so from the client's perspective it's a read that completed on an earlier snapshot of the world, which is exactly what a read concurrent with a write means. This matches how the source-row path handles overlapping CDC updates (generation-filtered snapshot there; time-ordered snapshot here).

The bound: a request observes state no older than what the coordinator read at dispatch time. That's what we commit to. Tighter bounds (e.g., MV-level generation filtering mirroring source-row generation stamps) are plausible follow-ups but not needed for v1.

### Rebuild deduplication

If many requests arrive during the rebuild window, we don't want N concurrent rebuilds. The `Pending → Scheduled` transition handles this:

- Coordinator transitions `Pending { has_table: true } → Scheduled { has_table: true }` **before** sending `MvBuild` (see Serve path → Coordinator dispatch).
- Subsequent requests observing `Scheduled { .. }` fall through to source-row eval without re-sending.
- Writer transitions `Scheduled { .. } → Fresh` on rebuild commit.

No oneshot channels, no broadcast, no waiter parking. The coordinator never blocks on the rebuild.

### Rebuild under concurrent state changes

Between coordinator dispatch and writer processing, source-row state can change (another CDC event, a LimitBump). The writer re-checks state before rebuilding:

- If source-row state is `Ready` and `mv_state == Scheduled { has_table: true }`: run the rebuild transaction, transition `Scheduled → Fresh` on commit.
- Otherwise (source-row state is `Loading` / `Invalidated`): skip the rebuild, transition `Scheduled → Pending { has_table: true }`.

When source-row state returns to `Ready`, the next request hit observes `Pending { has_table: true }` and triggers another rebuild via the normal path. Rebuild is idempotent; no cross-channel coordination required.

---

## LIMIT interaction

The MV follows the same sharing model as the source-row cache: **one MV per fingerprint, shared across all user LIMIT values**. LIMIT is excluded from the fingerprint (existing invariant), so all queries that differ only in their LIMIT value resolve to the same cache entry and the same MV. This is the core storage property of the design — a fingerprint's MV cost is O(max_limit), not O(sum of distinct LIMITs observed).

### Population bounds

Population deparses the full resolved query (which includes any ORDER BY) and appends `LIMIT max_limit`:

```sql
CREATE TABLE mv_table AS <deparsed-resolved-query-with-ORDER-BY> LIMIT max_limit;
INSERT INTO mv_table     <deparsed-resolved-query-with-ORDER-BY> LIMIT max_limit;
```

When `max_limit = None`, no LIMIT is appended — the MV stores the full result (e.g., a bare `SELECT count(*)` produces 1 row regardless).

**ORDER BY at population is load-bearing for Measure + ORDER BY + LIMIT shapes.** For `GROUP BY col ORDER BY count(*) DESC LIMIT 10` with `max_limit = 10`, population must apply ORDER BY so the stored rows are the global top-10. Without it, `LIMIT max_limit` would select an arbitrary 10 groups, and a subsequent user LIMIT 5 would return the top-5 of those 10 — not the global top-5.

Because the resolved query's deparse already emits its ORDER BY, this is automatic: the population SQL is `<full deparse> LIMIT max_limit`.

### Serving from a shared MV

Serve-time SQL is `SELECT * FROM mv_table [ORDER BY ...] [LIMIT user_limit]`. The ORDER BY re-application (from `resolved.order_by`) is essential even though population already applied ORDER BY, because:
- `SELECT * FROM mv` returns rows in arbitrary physical order.
- For user LIMIT < max_limit, only re-applying ORDER BY guarantees the correct top-M subset.

User LIMITs > max_limit are gated off by the coordinator's `limit_is_sufficient()` check (existing invariant) — those requests fall through to source-row eval and trigger a `LimitBump`. The MV path is only taken when the existing MV is sufficient.

### LimitBump as an invalidation trigger

A `LimitBump` (max_limit grows) transitions `mv_state: Fresh → Pending { has_table: true }` — same state flip as CDC invalidation, no TRUNCATE. Lazy rebuild repopulates with the new `max_limit` on the next hit. After rebuild, the single (now larger) MV continues to cover all existing user LIMIT values — we do not materialize additional per-LIMIT variants.

### Eligibility under LIMIT

No classification changes. In particular, **ORDER BY + LIMIT without a Measure shape stays `Skip`**: the source-row cache already stores the bounded row set the user needs, and `SELECT * FROM mv` does exactly the same work as `SELECT * FROM <cache table>` — no compute saved.

Measure shapes with LIMIT (e.g. `GROUP BY col ORDER BY count(*) DESC LIMIT 10`) are normal Measure candidates. The size gate at first population naturally respects the cap: `result_rows ≤ max_limit`, so the comparison becomes `min(actual_result, max_limit) × N ≤ source_rows`. If `max_limit` is very small the gate may fail — acceptable, since small-result caches don't benefit enough from materialization to justify the storage.

### Stickiness under LimitBump

Both `ShapeGate` and the Measure-gate outcome are sticky across invalidation, including LimitBump. We do not re-evaluate size-gate eligibility when `max_limit` grows. If a query's eligibility genuinely changes with scale, eviction + re-registration handles it.

---

## CDC invalidation wiring

Two existing invalidation paths in `writer/cdc.rs` both need MV hooks.

### Path A: in-place maintained (single-table)

`update_queries_execute_concurrent` applies CDC row changes directly to the source-row cache. The source-row cache stays `Ready`, but the **materialized result is now out of step** — e.g., an INSERT into `orders` doesn't invalidate the source-row cache for `SELECT count(*) FROM orders`, but the MV's stored `1` is wrong.

Hook: for each query whose update-query predicate matched the CDC row, mark the MV dirty. The predicate-match criterion is the right granularity — it naturally skips queries whose cached rows are unaffected (e.g. a query filtered by `WHERE org_id = 5` doesn't have its MV dirtied by updates to org 6).

This also uniformly covers UPDATEs of ORDER BY columns within in-cache rows: the row's new values land in the cache via path A, the MV is dirtied, and serve falls through to source-row eval (which re-sorts) until the MV rebuilds.

### Path B: full invalidation (complex queries)

`cache_query_cdc_invalidate` marks the query `Invalidated` — the source-row cache can't be maintained in-place because the change could grow the result. The MV is out of step too.

Hook: same — mark the MV dirty. Technically redundant for correctness (the coordinator won't serve from MV when source-row state ≠ `Ready`), but keeping the states aligned simplifies reasoning and keeps metrics honest.

### Single helper, two call sites

```rust
fn mv_dirty_mark(&mut self, fingerprint: u64) {
    // Transition Fresh → Pending { has_table: true } on CachedQueryView.mv_state.
    // No-op when mv_state is anything else.
}
```

Called from the per-query match branch of `update_queries_execute_concurrent` (Path A) and from `cache_query_cdc_invalidate` (Path B).

**Ordering**: `mv_dirty_mark` should fire *before* the source-row cache mutation commits, so coordinators observe `mv_state != Fresh` no later than the point at which the source-row update becomes visible. The helper is an in-memory state flip on `CachedQueryView`, so "before" here is a simple statement-ordering property within the writer task — no extra synchronization needed.

No TRUNCATE, no SQL — just an in-memory state flip. This means invalidation is lock-free and cheap, and the helper can be invoked freely without worrying about cache-DB round trips. Any destructive work on the MV table is deferred to the rebuild transaction.

### Inherited limitation

The existing source-row-cache limitation: **an UPDATE that should bring a row into the cache from outside** (e.g. a row whose ORDER BY column was just bumped high enough to enter the top-N) is not tracked — pgcache doesn't know about rows outside the cache and can't pull them in on UPDATE. The MV inherits this because it is built from the source-row cache. The fix would live in the source-row-cache layer, not the MV layer.

### Schema-change / table invalidation

`cache_table_invalidate` (e.g. DDL on a source table) evicts every query referencing the table. MV tables for those queries must also be **dropped** — the query is going away permanently. Lives in the eviction path: `cache_table_invalidate` delegates to `cache_query_evict` per query, which drops the MV via the snippet in the Eviction section.

### What doesn't change

- The existing `update_queries_check_invalidate` logic that decides shrink-vs-grow stays untouched. It decides whether Path A or Path B handles the change; the MV hook fires in both.
- Source-row-cache CDC metrics counters (`CACHE_INVALIDATIONS`, `CACHE_FRESHNESS_HITS`) are unchanged — an MV-dirty event is not a source-row-cache invalidation. MV-specific counters are added under new metric names (see Metrics).

---

## Eviction

### Stale-MV pre-sweep

Deferring TRUNCATE to rebuild (see Invalidation and rebuild) means MVs in `Pending { has_table: true }` hold bytes on disk that will never be served — strictly dead weight until the next hit (which may never come) or eviction. Size-driven eviction reclaims those bytes first, before considering evicting cache entries that may still be useful.

At the start of `eviction_run`, before the eviction `while` loop:

```rust
for (fingerprint, view) in state_view.iter() {
    if view.mv_state == MvState::Pending { has_table: true } {
        let mv_table = format!("pgcache_mv.q_{fingerprint}");
        db_cache
            .execute(&format!("TRUNCATE {mv_table}"), &[])
            .await?;
        metrics::counter!(names::CACHE_MV_DIRTY_TRUNCATES).increment(1);
    }
}
self.cache.current_size = self.cache_size_load().await?;
```

State stays `Pending { has_table: true }` after the truncate — the table is empty but the next hit's rebuild will `TRUNCATE` (no-op on empty) + `INSERT`. No state transition on the pre-sweep.

**Why only `Pending { has_table: true }`, not `Scheduled { .. }`**: a `Scheduled` entry already has a build message in the writer's own queue, about to run TRUNCATE+INSERT (rebuild) or CREATE TABLE AS (first-build). Truncating here would churn the table for no benefit.

**Why not `Fresh`**: obvious — it's still serving the fast path.

The pre-sweep runs unconditionally on every eviction entry. If the reclaimed bytes bring `current_size` under the limit, the subsequent `while` loop exits immediately without evicting any cache entries. On steady-state workloads with many infrequent-access MVs, this means size pressure preferentially removes dead weight before live entries.

### Evicting a cache entry

When a cache entry is evicted (`cache_query_evict`), the MV table must be dropped alongside the source-row cache tables:

```rust
if view.mv_state.has_table() {
    let mv_table = format!("pgcache_mv.q_{fingerprint}");
    db_cache
        .execute(&format!("DROP TABLE IF EXISTS {mv_table}"), &[])
        .await?;
}
```

`MvState::has_table()` returns true for `Fresh` and for `Pending`/`Scheduled` variants whose `has_table` bit is set. `DROP TABLE IF EXISTS` is idempotent, and we only issue it when a table actually exists. Runs before the `CachedQueryView` entry is removed.

This also covers schema-change / DDL cleanup: `cache_table_invalidate` delegates to `cache_query_evict` for each affected query, so MVs are dropped on schema changes via the same path — no separate wiring.

---

## Metrics

New counters (names to be finalized; placeholders below):

- `CACHE_MV_HITS` — requests served directly from the MV table (fast path).
- `CACHE_MV_FALLTHROUGH` — requests that observed `mv_state != Fresh` and fell through to source-row eval.
- `CACHE_MV_REBUILDS` — rebuilds completed by the writer (Path A / Path B / LimitBump / Readmit all fold in).
- `CACHE_MV_SKIPPED_REBUILDS` — rebuild messages the writer dropped because source-row state was not `Ready` at the time of processing (see Rebuild under concurrent state changes).
- `CACHE_MV_DIRTY_TRUNCATES` — stale MV tables (`Pending { has_table: true }`) truncated by the eviction pre-sweep.
- `CACHE_MV_BUILD_DURATION` — histogram of MV build durations (first-pop and rebuild). Used to correlate replication slot lag spikes with MV build activity and decide whether to move to off-thread builds (Option B — see Future work).

Storage:

- MV bytes counted via `pgcache_total_size()` once the function is extended to include the `pgcache_mv` schema (see Population → Storage accounting).
- A per-MV breakdown (size-by-fingerprint or size-by-shape-gate-outcome) is a follow-up — useful for validating the `mv_size_ratio` default but not required for v1.

These metrics are distinct from the source-row-cache counters (`CACHE_INVALIDATIONS`, `CACHE_FRESHNESS_HITS`) because an MV-layer event is not a source-row-layer event.

---

## State model

Additions to `CachedQueryView` (defined in `cache/types.rs:310`):

```rust
struct CachedQueryView {
    // ...existing fields...

    /// Shape classification set at registration from the decorrelated,
    /// resolved form. Never changes for the life of the cache entry.
    shape_gate: ShapeGate,

    /// Runtime state of the materialized result.
    mv_state: MvState,
}

enum MvState {
    /// `ShapeGate::Skip` — never materialize. Terminal.
    Skipped,
    /// `ShapeGate::Measure` size gate evaluated and failed. Terminal — never
    /// materialize for the life of this cache entry.
    Ineligible,
    /// Should have a fresh MV but doesn't. `has_table` distinguishes:
    /// `false` = never built (first build pending, includes the Measure gate);
    /// `true` = stale table from a prior Fresh flipped by CDC invalidation or
    /// LimitBump (rebuild pending).
    Pending { has_table: bool },
    /// Build command sent to writer, writer hasn't processed yet. `has_table`
    /// is inherited from the `Pending` that triggered this dispatch and tells
    /// the writer which SQL to run (`CREATE TABLE AS` vs `TRUNCATE + INSERT`).
    Scheduled { has_table: bool },
    /// Table exists and contents are fresh. Serve-path fast path.
    Fresh,
}
```

**Table name is not stored.** It is derived from the fingerprint on demand: `pgcache_mv.q_<fingerprint>`. The coordinator formats it at dispatch; the writer formats it at build / eviction.

**Initial state at registration**:

- `ShapeGate::Skip` → `Skipped`
- `ShapeGate::Measure` / `Materialize` → `Pending { has_table: false }`

**Transitions**:

| From | To | Trigger |
|------|-----|---------|
| `Pending { h }` | `Scheduled { h }` | coordinator dispatches build (cache hit); or pinned bootstrap |
| `Scheduled { false }` | `Fresh` | writer completes first build (size gate passed or `Materialize`) |
| `Scheduled { false }` | `Ineligible` | `Measure` size gate failed |
| `Scheduled { false }` | `Pending { false }` | writer abort (source-row state flipped, or build error) — retry on next hit |
| `Scheduled { true }` | `Fresh` | writer commits rebuild transaction |
| `Scheduled { true }` | `Pending { true }` | writer aborts rebuild (source-row state not `Ready`, or batch error) |
| `Fresh` | `Pending { true }` | CDC invalidation (Path A or B) or LimitBump |
| any | (entry removed) | eviction drops the MV table and removes the `CachedQueryView` |

`Fresh` is the only state that produces a fast-path dispatch. All others fall through to source-row evaluation.

`Skipped` and `Ineligible` are terminal for the life of the cache entry; eviction + re-registration is the only way out.

---

## Future work

- **ORDER BY + LIMIT classification**: currently `Skip`; revisit alongside any future work on `max_limit` semantics.
- **Per-query / per-table `mv_size_ratio` overrides**: dynamic global tuning ships in v1 via `DynamicConfig`; per-table or per-query overrides could follow if observed workloads warrant it.
- **MV-level generation filtering**: tightens the "reads that overlap an invalidation" window (see Invalidation and rebuild → Reads that overlap an invalidation). Would mirror the source-row generation-stamp + filter model.
- **Incremental view maintenance** for aggregates like `count(*)` / `sum(x)` — avoids rebuild churn on write-heavy hot queries. Explicitly off the table for v1.
- **Off-thread MV builds with race detection (Option B)**: v1 runs MV builds synchronously in the writer task. The planned follow-up is to move builds to a pool and add `Building` / `BuildingDirty` (and `Rebuilding` / `RebuildingDirty`) states so Path A CDC events during a build are detected at commit time and trigger a retry. Trigger for the move is replication-lag evidence via `CACHE_MV_BUILD_DURATION`. See Population → Execution model.
- **ORDER BY matching improvements**: MV classification and serve-time positional ORDER BY use pure structural equality (`ResolvedColumnExpr::eq`) to map an ORDER BY expression to a SELECT-list position. This covers direct column refs, aggregates, arithmetic, etc. Possible future extensions if workloads need them:
  - Alias resolution — `ORDER BY alias_name` referencing a SELECT alias. Today this doesn't reach the MV classifier because `column_resolve` doesn't consult SELECT aliases (see "Resolver: ORDER BY alias support" below), so these queries fail resolution upstream. If the resolver is extended to accept alias references, the MV classifier would need to match them against the SELECT list.
  - Normalized matching — handles small rewrites like constant folding between SELECT and ORDER BY, if any turn out to appear in practice.
- **Resolver: ORDER BY alias support**: pgcache's `column_resolve` only searches FROM-scope tables for unqualified names, so `SELECT count(*) AS c FROM t GROUP BY x ORDER BY c` fails with `ColumnNotFound`. PostgreSQL resolves `c` against the SELECT-list alias first. This is a pre-existing pgcache limitation (not MV-specific); fixing it would make MV classification transparently cover a common idiom.
- **"Row entering the cache from outside" limitation**: a known limitation of the source-row cache layer (UPDATEs that shift ORDER BY + LIMIT windows by promoting an untracked row). Fix would live in the source-row cache, not the MV layer.
- **Per-MV size metrics** (size-by-fingerprint): helps validate the `mv_size_ratio` default against real workloads.
