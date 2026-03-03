# Request Coalescing: Queuing Requests During Cache Loading

## Context

When a cacheable query first arrives and triggers registration, pgcache forwards it to origin and begins populating the cache. While population is in progress, the query state is `Loading`. Currently, every subsequent request for the same fingerprint during this window is also forwarded to origin (line 150-157 of `query_cache.rs`):

```rust
// Loading — forward to origin, don't re-register
Some(CachedQueryView {
    state: CachedQueryState::Loading,
    ..
}) => {
    trace!("cache loading {fingerprint}");
    reply_forward(msg.reply_tx, msg.pipeline, msg.data)
}
```

This is wasteful: if 50 connections simultaneously query the same data, the first triggers population while the other 49 each independently round-trip to origin. Once population completes (often just milliseconds later), the cache can serve all of them — but it's too late, they've already been forwarded.

**Goal**: Queue subsequent requests for a query that is currently `Loading`, then serve them all from cache once population completes.

## Design

### Waiting Queue on the Coordinator

Add a waiting queue to `QueryCache`, keyed by fingerprint:

```rust
/// Requests waiting for a Loading query to become Ready.
waiting: HashMap<u64, Vec<QueryRequest>>,
```

When a query arrives and the state is `Loading`:
1. Instead of calling `reply_forward`, push the `QueryRequest` into `waiting[fingerprint]`.
2. The proxy connection enters `ProxyMode::CacheRead(reply_rx)` as usual — it's already waiting on its oneshot.

When the state transitions to `Ready` (the writer calls `state_view_update` and the coordinator processes the `Ready` event):
1. Drain `waiting[fingerprint]`.
2. For each queued request, call `worker_request_send` — the same path as a normal cache hit.

### State Transitions That Drain the Queue

The queue must be drained on any terminal state change for the fingerprint:

| Transition | Action |
|---|---|
| Loading → Ready | Drain queue → `worker_request_send` each (cache hit) |
| Loading → Failed | Drain queue → `reply_forward` each (fall back to origin) |

The Ready/Failed transitions already pass through the coordinator (via `QueryCommand::Ready` and `QueryCommand::Failed` which the writer sends). The coordinator currently just updates `CacheStateView` — extend these handlers to also drain the waiting queue.

### LimitBump Loading

The LimitBump path also sets state to `Loading` (line 138-139 of `query_cache.rs`). Requests arriving during a LimitBump should also be coalesced. However, there's a subtlety: the incoming request may need fewer rows than the bump target, in which case the existing cached rows are already sufficient. Since `limit_is_sufficient` has already failed for the request that triggered the bump, and subsequent requests may have different LIMIT values, the simplest approach is to queue them all and let `worker_request_send` handle it when Ready.

### Invalidated / Readmit Loading

When a CDC-invalidated query is readmitted (line 177-186), it transitions to `Loading` and triggers re-population. Requests during this window should also be coalesced — same mechanism.

### Coordinator Event Loop

Currently the coordinator processes `QueryRequest` messages from the proxy. It does not directly process `QueryCommand::Ready/Failed` — those go to the writer thread. The writer updates `CacheStateView`, and the next time the coordinator dispatches a query for that fingerprint, it reads the updated state.

For coalescing, the coordinator needs to know when Loading → Ready happens so it can drain the queue promptly. Two approaches:

**Option A: Poll on next dispatch.** When the coordinator dispatches a query and sees state = Ready for a fingerprint that has waiters, drain the queue then. Simple, but waiters block until the next request for this fingerprint arrives — could be indefinitely.

**Option B: Notify channel from writer to coordinator.** The writer sends a notification (e.g., via an `mpsc` channel or the existing `query_tx` channel) when a query becomes Ready or Failed. The coordinator's event loop selects on both the proxy message channel and this notification channel.

Option B is correct. Option A introduces unbounded latency for queued requests.

The existing `query_tx` channel (`Sender<QueryCommand>`) flows from coordinator → writer. We need a reverse channel. Add a new channel:

```rust
/// Notifications from writer to coordinator.
notify_rx: mpsc::UnboundedReceiver<WriterNotify>,
```

```rust
enum WriterNotify {
    /// Registration processed — subsumption check complete.
    /// Sent immediately after the check, before population dispatch.
    /// See predicate-subsumption.md for the subsumption design.
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

The writer sends on this channel at three points:
1. **After the subsumption check** (`Registered`): sent immediately after resolving the query and checking subsumption, before population dispatch. Always sent for every `Register` command.
2. **After population completes** (`Ready`): sent by `query_ready_mark`.
3. **After population fails** (`Failed`): sent by `query_failed_cleanup`.

### Interaction with Predicate Subsumption

This channel is shared with predicate subsumption (see `predicate-subsumption.md`). The `Registered` variant is the subsumption result — it tells the coordinator whether the first request can be served from cache:

- **Subsumed** (`Registered { subsumed: Some(...) }`): The writer has already set `CacheStateView` to Ready. Drain the entire waiting queue via `worker_request_send` — the first request and any coalesced requests are all served from cache. No origin round-trip.
- **Not subsumed** (`Registered { subsumed: None }`): Pop the first waiter from the queue and forward it to origin via `reply_forward`. State remains `Loading`. Remaining coalesced waiters stay in the queue until `Ready` or `Failed`.

The first request is always held in the waiting queue (not forwarded eagerly) to give subsumption a chance to serve it from cache. This adds a few milliseconds of latency to non-subsumed first-misses but allows infrequent queries to skip origin entirely when their data is already cached.

### Coordinator Event Loop Change

The coordinator currently runs in a simple `while let` loop receiving `ProxyMessage`s. With coalescing and subsumption, the loop becomes a `tokio::select!` over two channels:

```rust
loop {
    tokio::select! {
        Some(proxy_msg) = proxy_rx.recv() => {
            // existing query_dispatch logic
        }
        Some(notify) = notify_rx.recv() => {
            match notify {
                WriterNotify::Registered { fingerprint, subsumed: Some(result) } => {
                    // Subsumed — drain all waiters from cache
                    self.waiting_drain_ready(
                        fingerprint, result.resolved, result.generation,
                    );
                }
                WriterNotify::Registered { fingerprint, subsumed: None } => {
                    // Not subsumed — forward first waiter to origin, keep rest queued
                    self.waiting_forward_first(fingerprint);
                }
                WriterNotify::Ready { fingerprint, generation, resolved, .. } => {
                    // Population complete — drain remaining waiters from cache
                    self.waiting_drain_ready(fingerprint, resolved, generation);
                }
                WriterNotify::Failed { fingerprint } => {
                    // Population failed — forward remaining waiters to origin
                    self.waiting_drain_failed(fingerprint);
                }
            }
        }
    }
}
```

### Draining the Queue

**On Ready — broadcast via single worker request:**

Instead of dispatching N separate `WorkerRequest`s (each independently executing the same query against the cache DB), coalesced requests piggyback on a single worker execution. The coordinator builds one `WorkerRequest` from the first waiter and attaches the remaining waiters' sockets:

```rust
fn waiting_drain_ready(&mut self, fingerprint: u64, resolved: SharedResolved, generation: u64) {
    let Some(mut waiters) = self.waiting.remove(&fingerprint) else {
        return;
    };

    // First waiter becomes the primary request
    let primary = waiters.remove(0);

    // Remaining waiters contribute their sockets and reply channels
    let coalesced: Vec<CoalescedClient> = waiters
        .into_iter()
        .map(|msg| CoalescedClient {
            client_socket: msg.client_socket,
            reply_tx: msg.reply_tx,
            timing: msg.timing,
            data: msg.data,
        })
        .collect();

    if let Err(e) = self.worker_request_send_coalesced(
        primary, Arc::clone(&resolved), generation, coalesced,
    ) {
        error!("coalesce serve failed: {e}");
    }
}
```

This adds a `coalesced` field to `WorkerRequest`:

```rust
pub struct WorkerRequest {
    // ... existing fields ...

    /// Additional clients to receive the same response bytes.
    /// Empty for non-coalesced requests.
    pub coalesced: Vec<CoalescedClient>,
}

pub struct CoalescedClient {
    pub client_socket: ClientSocket,
    pub reply_tx: oneshot::Sender<CacheReply>,
    pub timing: QueryTiming,
    /// Raw data buffer for origin fallback on error.
    pub data: BytesMut,
}
```

**On Registered (not subsumed) — forward first waiter to origin:**
```rust
fn waiting_forward_first(&mut self, fingerprint: u64) {
    if let Some(waiters) = self.waiting.get_mut(&fingerprint)
        && !waiters.is_empty()
    {
        let first = waiters.remove(0);
        let _ = reply_forward(first.reply_tx, first.pipeline, first.data);
    }
}
```

The first waiter was held to give subsumption a chance. Since it failed, forward to origin. Remaining waiters stay queued for `Ready`/`Failed`.

**On Failed:**
```rust
fn waiting_drain_failed(&mut self, fingerprint: u64) {
    if let Some(waiters) = self.waiting.remove(&fingerprint) {
        for msg in waiters {
            let _ = reply_forward(msg.reply_tx, msg.pipeline, msg.data);
        }
    }
}
```

### Worker Broadcast

Both the text and binary worker paths (`handle_cached_query_text`, `handle_cached_query_binary`) accumulate plaintext PostgreSQL wire protocol bytes in a `write_buf: BytesMut` and incrementally flush to `msg.client_socket`. The broadcast extends this to write the same bytes to all coalesced sockets.

The current write loop uses `tokio::select!` to concurrently read frames from the cache DB and write to the client. With broadcasting, the write arm writes to all sockets:

```rust
// In the select! write arm, after writing to the primary socket:
for client in &mut msg.coalesced {
    if let Err(e) = client.client_socket.write_all(written_bytes).await {
        error!("coalesce write failed: {e}");
        // Mark this client as failed — skip on future writes
    }
}
```

After the streaming loop completes (ReadyForQuery received, connection returned to pool):
1. Append ReadyForQuery to `write_buf` if needed (same as today).
2. Final flush of `write_buf` to primary socket and all coalesced sockets.
3. Send `CacheReply::Complete(timing)` on the primary `reply_tx` and each coalesced `reply_tx`.

**On error**: Send `CacheReply::Error(data)` on each coalesced client's `reply_tx` using their stored `data` buffer, so each proxy connection falls back to origin independently.

#### Protocol compatibility

The bytes written to `write_buf` include protocol framing that depends on the request's characteristics: `has_parse`, `has_bind`, `pipeline_describe`, and `has_sync` control which prefix messages (ParseComplete, BindComplete) and suffix (ReadyForQuery) are included. The `query_type` determines whether the text or binary path is used.

For coalesced requests to receive valid protocol bytes, they must have matching protocol parameters. In practice this is nearly always the case — coalesced requests come from the same application using the same driver and query path. For correctness, the coordinator should verify protocol compatibility when building the coalesced list and fall back to a separate `WorkerRequest` for any mismatches.

#### LIMIT compatibility

The worker appends the primary request's LIMIT to the SQL executed against the cache DB. Coalesced requests with different LIMITs would receive the wrong number of rows. The coordinator should group waiters by LIMIT when building coalesced requests. Waiters with a different LIMIT than the primary get dispatched as separate `WorkerRequest`s. In the common case (identical queries from the same application), all waiters share the same LIMIT.

### Backpressure / Queue Limits

An unbounded queue per fingerprint could accumulate memory if population takes a long time or stalls. Add a configurable cap (e.g., `max_coalesce_waiters: usize`, default 100). When the queue for a fingerprint reaches the cap, fall back to `reply_forward` for the excess request. This bounds memory and provides a safety valve.

```rust
if waiters.len() >= self.max_coalesce_waiters {
    reply_forward(msg.reply_tx, msg.pipeline, msg.data)?;
} else {
    waiters.push(msg);
}
```

### Timeout

If the writer never sends Ready or Failed (e.g., population hangs), waiters would block indefinitely. This is already a problem for the proxy connection's oneshot — it has no timeout today. Coalescing doesn't make this worse; the proxy connection would time out or be closed by the client regardless.

If we want belt-and-suspenders: when draining on Ready/Failed, also check for stale entries in `waiting` (entries older than some threshold) and forward them to origin. But this is a separate concern and not required for v1.

## What Doesn't Change

- **First request**: Still forwarded to origin (the client that triggered registration gets its answer from origin, same as today).
- **Pending state**: Requests during the admission gate (`Pending(n)`) are still forwarded — the query hasn't been admitted yet and there's nothing to coalesce on.
- **Non-coalesced cache hits**: Normal Ready cache hits (no waiters queued) follow the existing single-request path. The `coalesced` vec is empty and the worker behaves identically to today.
- **Population**: The writer and population workers are unchanged.
- **CacheStateView**: Unchanged. The coordinator reads state as before; the notify channel is a separate signal that carries the Ready/Failed data directly.

## Observability

- **Gauge**: `cache_coalesce_waiting` — current number of requests queued across all fingerprints
- **Counter**: `cache_coalesce_served` — requests served from cache via coalescing (instead of forwarding to origin)
- **Counter**: `cache_coalesce_overflow` — requests that exceeded the per-fingerprint cap and were forwarded

## Value Proposition

Connection poolers like PgBouncer commonly funnel many application connections through a smaller number of database connections. When an application server starts up or a new query pattern appears, a burst of identical queries can arrive simultaneously. Without coalescing, each one independently hits origin. With coalescing, one hits origin while the rest wait a few milliseconds and get served from cache.

This is particularly impactful for:
- **Application cold start**: Many connections issuing the same configuration/lookup queries
- **Multi-tenant workloads**: Same query template with same tenant parameter arriving from multiple connections
- **Dashboard queries**: Multiple users viewing the same dashboard simultaneously
