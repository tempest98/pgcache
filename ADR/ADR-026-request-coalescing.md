# ADR-026: Request Coalescing for Loading-State Queries

## Status
Accepted

## Context
When a cacheable query first arrives and triggers registration, pgcache forwards it to origin and begins populating the cache. While population is in progress the query state is `Loading`, which can take milliseconds to hundreds of milliseconds depending on result set size. Previously, every subsequent request for the same fingerprint during this window was also forwarded to origin — wasting origin capacity precisely when the cache is about to hold the answer.

This is particularly impactful for connection poolers like PgBouncer that funnel many application connections through pgcache. Bursts of identical queries — application cold start, dashboard refreshes, multi-tenant workloads with the same tenant parameter — produce thundering-herd traffic to origin during the Loading window. Since the goal of pgcache is to protect origin from read load, letting these queries spill through defeats the purpose.

## Decision
Queue `Loading`-state requests in a per-fingerprint waiting queue on the coordinator and drain them from cache once population completes. Specifically:

- **Per-fingerprint, per-CoalesceKey waiting queue**: `HashMap<Fingerprint, HashMap<CoalesceKey, Vec<QueryRequest>>>` on the coordinator. Outer key gives O(1) drain on Ready/Failed; inner key groups requests that produce identical wire bytes (query_type, has_sync, has_parse, has_bind, pipeline_describe, result_formats, limit)
- **WriterNotify channel**: New unbounded mpsc channel from writer to coordinator carrying `Ready { fingerprint, generation, resolved, max_limit }` and `Failed { fingerprint }`. Writer sends from `query_ready_mark` and `query_failed_cleanup`
- **Single-execution broadcast**: When draining a coalescing group, the first waiter becomes the primary `WorkerRequest` and remaining waiters are attached as `Vec<CoalescedClient>`. The worker creates a `tokio::sync::broadcast::channel<Bytes>` and `spawn_local`s a write task per coalesced client. Every byte chunk pushed to the primary's WriteQueue is also broadcast
- **No backpressure cap**: Waiters are queued unconditionally. Capping and falling back to origin would defeat the entire point of coalescing during a burst
- **LIMIT sufficiency check at drain**: If the cached `max_limit` is insufficient for a group's LIMIT, the group is forwarded to origin rather than served partial results
- **Zero overhead on the non-coalesced hot path**: When `coalesced` is empty, no broadcast channel is created, no spawned tasks exist, and no async cleanup futures are polled

## Rationale
- **Coordinator-side queue, not worker-side**: The coordinator already owns the dispatch decision; queuing there avoids waking workers for queries that will share an execution
- **CoalesceKey grouping** is required because protocol bytes vary by parameters — coalesced requests with different `result_formats` or `LIMIT` would receive incorrect responses
- **Broadcast over N independent worker executions**: Without coalescing, N waiting clients would each trigger their own cache DB query after Ready arrives. With broadcast, one cache DB execution serves all N clients in the group. This eliminates duplicate work on the cache database during a burst, which matters most when bursts happen
- **Per-client `spawn_local` write tasks**: A slow or dead coalesced client cannot block the primary or other coalesced clients. The broadcast channel's `Lagged` semantics provide a safe fallback for clients that fall too far behind
- **No cap**: Capping waiters and forwarding overflow to origin means the worst-case burst — exactly when coalescing matters most — escapes the protection. Memory growth from queued requests is bounded by client connection count, which is already bounded by the proxy's accept loop

## Consequences

### Positive
- Bursts of identical queries during Loading are served from a single cache execution rather than N origin round-trips
- Particularly effective for cold-start, dashboard, and multi-tenant patterns where many connections hit the same fingerprint
- Per-client write tasks isolate slow clients from each other and from the primary
- Notify channel is reusable: future predicate subsumption work will share the same writer→coordinator pathway

### Negative
- Worker error paths gained complexity — broadcast cleanup must run before each error return when broadcast is active
- Coalesced clients receive replies via two paths (async tasks plus per-client `reply_tx`), which is more moving parts than a single direct write
- A single broken client write can cause the whole group's bytes to be buffered in the broadcast channel until either delivery or `Lagged`

## Implementation Notes
Waiting queue and drain logic in `src/cache/query_cache.rs`; broadcast machinery in `src/cache/worker.rs`; `WriterNotify` enum in `src/cache/messages.rs` and channel plumbed through `src/cache/runtime.rs` and `src/cache/writer/{core,query}.rs`. Metrics: `pgcache.cache.coalesce_waiting` (gauge) and `pgcache.cache.coalesce_served` (counter).
