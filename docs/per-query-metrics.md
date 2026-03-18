# Per-Query Operational Metrics

## Context

The `/status` endpoint currently returns per-query data limited to: fingerprint, sql_preview, tables, state, cached_bytes, max_limit, pinned. There are no operational metrics per query -- all Prometheus counters/histograms are global aggregates. This makes it impossible to understand the behavior of individual cached queries (which are hot, how often they're invalidated, cache-hit latency distribution, etc.).

## Approach: Dedicated `QueryMetrics` in a second DashMap

Add a `DashMap<u64, QueryMetrics>` to `CacheStateView`, alongside the existing `cached_queries` DashMap.

Metrics persist across eviction/re-registration cycles. Only cleaned when fingerprint is fully removed from both maps.

### No atomics, no Mutex

All writes go through `DashMap::get_mut()`, which takes an exclusive shard lock for the duration of the access. This means `QueryMetrics` fields can be plain `u64` and the `Histogram<u64>` needs no `Mutex` wrapper -- the DashMap shard lock provides mutual exclusion. Hold time per access is ~nanoseconds (increment a u64 or record a histogram value), so shard contention is negligible even on hot queries.

### Worker → Writer metrics channel

The worker thread does **not** access the metrics DashMap directly. Instead, after each cache hit the worker sends a small `WorkerMetrics { fingerprint, latency_us, bytes_served }` message through an `UnboundedSender` to the **writer thread**. The writer's `select!` loop (which already handles 4 channels) drains this 5th channel and updates the metrics DashMap.

This means only two threads modify `QueryMetrics`:
- **Coordinator** (multi-task, proxy runtime): hit_count, miss_count, last_hit_at, subsumption_count -- via `get_mut()`
- **Writer** (single-threaded): all other fields including cache_hit_latency histogram, worker-reported bytes_served, invalidation/readmission/eviction counts, population stats

The status endpoint reads via `get()` (shared access).

### Histogram library

Uses `hdrhistogram` crate (v7.5.4):
- ~10KB per histogram at sigfig=2 with u64 counters (500 queries = ~5 MB)
- 77M downloads, mature port of Gil Tene's Java HdrHistogram
- Range: 1us-60s (`new_with_bounds(1, 60_000_000, 2)`)
- API: `saturating_record()`, `value_at_quantile()`, `min()`, `max()`, `mean()`, `len()`

---

## New per-query fields in /status response

| Field | Type | Updated by |
|-------|------|------------|
| hit_count | u64 | Coordinator (Ready path) |
| miss_count | u64 | Coordinator (miss/loading/pending/invalidated) |
| last_hit_at_ms | Option\<u64\> | Coordinator, ms since process start |
| cached_since_ms | Option\<u64\> | Writer query_ready_mark |
| invalidation_count | u64 | Writer CDC invalidate |
| readmission_count | u64 | Writer query_readmit |
| eviction_count | u64 | Writer cache_query_evict |
| subsumption_count | u64 | Coordinator Subsumed path + Writer query_subsume |
| population_count | u64 | Writer query_ready_mark |
| last_population_duration_ms | Option\<u64\> | Writer query_ready_mark |
| total_bytes_served | u64 | Writer (from worker channel) |
| row_count | u64 | Writer (from population Ready command) |
| cache_hit_latency | Option\<LatencyStats\> | Writer (from worker channel) |

### LatencyStats structure

```json
{
  "count": 1523,
  "mean_us": 245.3,
  "p50_us": 210,
  "p95_us": 480,
  "p99_us": 920,
  "min_us": 85,
  "max_us": 3200
}
```

---

## Implementation Steps

### Step 1: Add hdrhistogram dependency

**File**: `Cargo.toml`

Add `hdrhistogram = "7.5.4"` to `[dependencies]`.

### Step 2: Define QueryMetrics, WorkerMetrics, and extend CacheStateView

**File**: `src/cache/types.rs`

Add `QueryMetrics` struct with plain `u64` fields and plain `Histogram<u64>`:

```rust
pub struct QueryMetrics {
    pub hit_count: u64,
    pub miss_count: u64,
    pub last_hit_at_ns: u64,              // nanos since CacheStateView.started_at
    pub cached_since_ns: u64,             // nanos since CacheStateView.started_at, 0 = not set
    pub invalidation_count: u64,
    pub readmission_count: u64,
    pub eviction_count: u64,
    pub subsumption_count: u64,
    pub population_count: u64,
    pub last_population_duration_us: u64,  // microseconds, 0 = not set
    pub total_bytes_served: u64,
    pub row_count: u64,
    pub cache_hit_latency: Histogram<u64>,
}
```

- Constructor: `QueryMetrics::new()` -- all fields 0, histogram `new_with_bounds(1, 60_000_000, 2)`

Add `WorkerMetrics` struct (sent from worker → writer):

```rust
pub struct WorkerMetrics {
    pub fingerprint: u64,
    pub latency_us: u64,
    pub bytes_served: u64,
}
```

Extend `CacheStateView`:

```rust
pub struct CacheStateView {
    pub cached_queries: DashMap<u64, CachedQueryView>,
    pub metrics: DashMap<u64, QueryMetrics>,
    pub started_at: Instant,
}
```

Replace `#[derive(Default)]` with explicit `CacheStateView::new()`.

### Step 3: Update CacheStateView construction

**File**: `src/cache/runtime.rs` (line 230)

Change `CacheStateView::default()` to `CacheStateView::new()`.

### Step 4: Add fingerprint to WorkerRequest

**File**: `src/cache/query_cache.rs`

- Add `fingerprint: u64` field to `WorkerRequest`
- Set it in `worker_request_send()` (fingerprint already available from dispatch)

### Step 5: Worker metrics channel and recording

**File**: `src/cache/runtime.rs`

Create the worker metrics channel in `cache_run()`:
- `let (worker_metrics_tx, worker_metrics_rx) = unbounded_channel::<WorkerMetrics>()`
- Pass `worker_metrics_tx` to `worker_run()`
- Pass `worker_metrics_rx` to `writer_run()`

Update `worker_run()` to accept `worker_metrics_tx: UnboundedSender<WorkerMetrics>`.

In the `spawn_local` closure, clone the sender and pass to `handle_worker_request()`.

Update `handle_worker_request()` to accept `worker_metrics_tx: UnboundedSender<WorkerMetrics>`. After successful `handle_cached_query()`, send:

```rust
let latency_us = msg.timing.worker_start_at
    .map(|s| s.elapsed().as_micros() as u64)
    .unwrap_or(0);
let _ = worker_metrics_tx.send(WorkerMetrics {
    fingerprint: msg.fingerprint,
    latency_us,
    bytes_served: bytes_served as u64,
});
```

**File**: `src/cache/worker.rs`

- Change `handle_cached_query()` return type to `CacheResult<usize>` (bytes served)
- In `handle_cached_query_text()` and `handle_cached_query_binary()`: add `bytes_served: usize` counter, increment on each DataRow frame written to client. Return `Ok(bytes_served)`.

### Step 6: Writer processes worker metrics channel

**File**: `src/cache/writer/core.rs`

Update `writer_run()` to accept `mut worker_metrics_rx: UnboundedReceiver<WorkerMetrics>`.

Add a 5th arm to the `select!` loop:

```rust
msg = worker_metrics_rx.recv() => {
    if let Some(wm) = msg {
        writer.worker_metrics_record(wm);
    }
}
```

Add method to `CacheWriter`:

```rust
fn worker_metrics_record(&self, wm: WorkerMetrics) {
    if let Some(mut m) = self.state_view.metrics.get_mut(&wm.fingerprint) {
        m.total_bytes_served += wm.bytes_served;
        m.cache_hit_latency.saturating_record(wm.latency_us);
    }
}
```

### Step 7: Add row_count to QueryCommand::Ready

**File**: `src/cache/messages.rs`

Add `row_count: u64` to `QueryCommand::Ready` variant.

**File**: `src/cache/writer/population.rs`

- `population_stream()`: add row counter, increment on each `Row` arm, return `(cached_bytes, row_count)`
- `population_task()`: accumulate both bytes and row_count across branches, return `(total_bytes, total_row_count)`
- `population_worker()`: destructure tuple, include `row_count` in `QueryCommand::Ready`

**File**: `src/cache/writer/core.rs`

- Update `QueryCommand::Ready` match arm (line 211) to destructure `row_count`
- Pass `row_count` to `query_ready_mark()`

### Step 8: Increment metrics in coordinator (hot path)

**File**: `src/cache/query_cache.rs`

In `query_dispatch()` match arms:
- **Ready + sufficient** (line 192): `metrics_hit_record()`
- **Ready + insufficient** (line 202): `metrics_miss_record()`
- **Loading** (line 220): `metrics_miss_record()`
- **Pending** (line 229): `metrics_miss_record()`
- **Invalidated** (line 248): `metrics_miss_record()`

In `query_first_miss_handle()`: create metrics entry via `state_view.metrics.entry(fp).or_insert_with(QueryMetrics::new)`, then `metrics_miss_record()`.

In `subsumption_await()` on `Subsumed` path (line 405): `metrics_hit_record()` and increment `subsumption_count`.

In `pinned_queries_register()`: create metrics entry for each pinned query.

Helper functions using `get_mut()` for shard-lock exclusion:

```rust
fn metrics_hit_record(state_view: &CacheStateView, fingerprint: u64) {
    if let Some(mut m) = state_view.metrics.get_mut(&fingerprint) {
        m.hit_count += 1;
        m.last_hit_at_ns = state_view.started_at.elapsed().as_nanos() as u64;
    }
}

fn metrics_miss_record(state_view: &CacheStateView, fingerprint: u64) {
    if let Some(mut m) = state_view.metrics.get_mut(&fingerprint) {
        m.miss_count += 1;
    }
}
```

### Step 9: Record writer-side metrics

**File**: `src/cache/writer/query.rs`

- `query_ready_mark()`: after marking Ready, via `state_view.metrics.get_mut()`:
  - Set `cached_since_ns` = elapsed since `state_view.started_at`
  - Set `row_count` from new parameter
  - Set `last_population_duration_us` from `started_at.elapsed()`
  - Increment `population_count`
- `query_subsume()`: increment `subsumption_count` and `population_count`, set `cached_since_ns`
- `query_readmit()`: increment `readmission_count`, clear `cached_since_ns` to 0

**File**: `src/cache/writer/cdc.rs`

- `cache_query_cdc_invalidate()`: increment `invalidation_count`
- `cache_query_evict()`: increment `eviction_count`, clear `cached_since_ns`

### Step 10: Extend status response types

**File**: `src/cache/status.rs`

Add all new fields to `QueryStatusData`. Add `LatencyStats` struct:

```rust
#[derive(Debug, Serialize)]
pub struct LatencyStats {
    pub count: u64,
    pub mean_us: f64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub min_us: u64,
    pub max_us: u64,
}
```

### Step 11: Assemble metrics in status_respond

**File**: `src/cache/writer/core.rs`

In `status_respond()`: for each cached query, look up `state_view.metrics.get(&fingerprint)`:
- Read all fields directly (shared `get()` access)
- Extract histogram percentiles
- Convert `_ns` timestamps to `_ms` (divide by 1_000_000)
- Populate new `QueryStatusData` fields

### Step 12: Cleanup stale metrics

**File**: `src/cache/writer/core.rs`

In `stale_entries_cleanup()`: after existing cleanup, retain metrics entries only for fingerprints that still exist in either `cached_queries` BiHashMap or `state_view.cached_queries` DashMap.

### Step 13: Initialize metrics on writer-side registration

**File**: `src/cache/writer/query.rs`

In `query_register()`, after `cached_query_insert`: ensure metrics entry exists via `state_view.metrics.entry(fp).or_insert_with(QueryMetrics::new)`. Handles pinned queries registered directly by writer.

---

## Verification

1. `cargo check` -- compiles cleanly
2. `cargo clippy -- -D warnings` -- no warnings
3. `cargo test` -- existing tests pass
4. Manual: run pgcache, execute queries, hit `/status` endpoint, verify new fields appear
5. Verify `hit_count` increments on repeated cacheable queries
6. Verify `invalidation_count` increments after INSERT on origin table
7. Verify `cache_hit_latency` shows reasonable p50/p95/p99 values
8. Verify `row_count` matches actual cached rows
