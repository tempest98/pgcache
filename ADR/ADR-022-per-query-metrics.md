# ADR-022: Per-Query Metrics

## Status
Accepted

## Context
The Prometheus metrics and `/status` endpoint provided only global aggregates — total cache hits, total misses, etc. There was no way to understand the behavior of individual cached queries: which queries are hot, how often each is invalidated, what their cache-hit latency distribution looks like, or how much data each serves. This made it difficult to tune caching configuration or diagnose performance issues for specific queries.

## Decision
Add per-query operational metrics tracked in a `DashMap<u64, QueryMetrics>` on `CacheStateView`, alongside the existing `cached_queries` DashMap. Metrics are exposed through the `/status` endpoint as additional fields on each query's status entry.

### Tracked Metrics

| Field | Description | Updated by |
|-------|-------------|------------|
| hit_count | Total cache hits | Coordinator |
| miss_count | Total cache misses | Coordinator |
| last_hit_at_ns | Time of last hit (ns since startup) | Coordinator |
| registered_at_ns | Time query was first seen (ns since startup) | Coordinator |
| cached_since_ns | Time query last became Ready (cleared on invalidation/eviction) | Writer |
| invalidation_count | Times invalidated by CDC | Writer |
| readmission_count | Times readmitted after invalidation | Writer |
| eviction_count | Times evicted by cache policy | Writer |
| subsumption_count | Times served via predicate subsumption | Coordinator + Writer |
| population_count | Times populated from origin | Writer |
| last_population_duration_us | Duration of last population | Writer |
| total_bytes_served | Cumulative bytes served to clients | Writer (from worker channel) |
| population_row_count | Rows inserted during last population | Writer |
| cache_hit_latency | Latency histogram (p50/p95/p99/mean/min/max) | Writer (from worker channel) |

### No Atomics, No Mutex

All writes go through `DashMap::get_mut()`, which holds an exclusive shard lock for the duration of access. Since each access is a simple u64 increment or histogram record (~nanoseconds), shard contention is negligible. This means `QueryMetrics` fields are plain `u64` / `Option<NonZeroU64>` and the `Histogram<u64>` needs no separate synchronization. Optional timestamps use `Option<NonZeroU64>` to distinguish "not set" from zero without a separate flag.

### Two-Writer Architecture

Only two threads modify `QueryMetrics`:

1. **Coordinator** (proxy runtime): hit_count, miss_count, last_hit_at_ns, subsumption_count — updated during query dispatch via `metrics_hit_record()` and `metrics_miss_record()` helpers
2. **Writer** (single-threaded): all other fields — invalidation, eviction, readmission, population stats, plus worker-reported bytes_served and latency histogram

### Worker Metrics Channel

The worker thread (which handles cache hits on the hot path) does not access the metrics DashMap directly. After each cache hit, the worker sends a `WorkerMetrics { fingerprint, latency_us, bytes_served }` message through an `UnboundedSender` to the writer thread. The writer drains this channel in its `select!` loop and updates the DashMap.

This keeps the worker focused on I/O and avoids contention on the metrics DashMap from the hot path.

### Latency Histogram

Uses the `hdrhistogram` crate — a mature port of Gil Tene's HdrHistogram. Each query gets its own histogram configured with:
- Range: 1 microsecond to 60 seconds
- 2 significant figures of precision
- ~10KB memory per histogram

The `/status` endpoint extracts percentiles into a `LatencyStats` summary: count, mean, p50, p95, p99, min, max.

### Metric Lifecycle

Metrics entries are created when a query is first seen (coordinator `query_first_miss_handle()` or writer `query_register()`). They persist across eviction and re-registration cycles — an evicted query that returns retains its cumulative hit counts and latency history. Entries are only cleaned up when the fingerprint is fully removed from both the cached queries map and the metrics map.

## Rationale
- **DashMap shard locking** gives mutual exclusion without explicit atomics or mutexes, keeping the code simple while maintaining negligible contention
- **Separating coordinator and writer writes** aligns with the existing threading model — no new synchronization boundaries needed
- **Worker → writer channel** for latency/bytes avoids touching shared state on the hot path
- **Persisting metrics across eviction** provides complete operational history — important for understanding queries that churn between cached and evicted states
- **HdrHistogram** provides accurate percentiles with bounded memory, well-suited for latency distributions

## Consequences

### Positive
- Operators can identify hot queries, poorly-performing queries, and over-invalidated queries from the `/status` endpoint
- Per-query latency distributions enable SLA monitoring at the individual query level
- Subsumption and population counts help validate cache efficiency
- No measurable overhead on the cache hit hot path (worker only sends a small message)

### Negative
- Memory grows linearly with distinct query fingerprints (~10KB per query for the histogram)
- The `/status` response becomes larger — may want pagination or filtering in the future
- Metrics are in-memory only; lost on restart (acceptable since the cache itself is ephemeral)

## Implementation Notes
- `QueryMetrics` and `WorkerMetrics` structs: `src/cache/types.rs`
- Coordinator-side recording: `metrics_hit_record()`, `metrics_miss_record()` in `src/cache/query_cache.rs`
- Worker metrics channel: created in `cache_run()` in `src/cache/runtime.rs`, drained by `worker_metrics_record()` in the writer's `select!` loop
- Writer-side recording: scattered across `src/cache/writer/query.rs` (population, subsumption, readmission) and `src/cache/writer/cdc.rs` (invalidation, eviction)
- Status assembly: `status_respond()` in `src/cache/writer/core.rs` reads metrics via `DashMap::get()` and extracts histogram percentiles into `LatencyStats`
- Status types: `QueryStatusData`, `LatencyStats` in `src/cache/status.rs`
