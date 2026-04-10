# Request Coalescing: Queuing Requests During Cache Loading

## Context

When a cacheable query first arrives and triggers registration, pgcache forwards it to origin and begins populating the cache. While population is in progress, the query state is `Loading`. Currently, every subsequent request for the same fingerprint during this window is also forwarded to origin (lines 193-200 of `query_cache.rs`):

```rust
// Loading — forward to origin, don't re-register
Some(CachedQueryView {
    state: CachedQueryState::Loading,
    ..
}) => {
    trace!("cache loading {fingerprint}");
    self.metrics_miss_record(fingerprint);
    reply_forward(msg.reply_tx, msg.pipeline, msg.data)
}
```

This is wasteful: if 50 connections simultaneously query the same data, the first triggers population while the other 49 each independently round-trip to origin. Once population completes (often just milliseconds later), the cache can serve all of them — but it's too late, they've already been forwarded.

**Goal**: Queue subsequent requests for a query that is currently `Loading`, then serve them all from cache once population completes.

## Design

### Coalescing Groups

The bytes written to `write_buf` include protocol framing that depends on the request's characteristics: `has_parse`, `has_bind`, `pipeline_describe`, and `has_sync` control which prefix messages (ParseComplete, BindComplete) and suffix (ReadyForQuery) are included. The `query_type` determines whether the text or binary path is used. The `result_formats` control per-column encoding in the binary protocol path. Additionally, the LIMIT clause determines how many rows the worker fetches — coalesced requests with different LIMITs would receive the wrong number of rows.

For coalesced requests to receive valid protocol bytes and correct results, they must share identical protocol and query parameters. The waiting queue is keyed by a **coalescing group key** that includes all parameters affecting the wire output:

```rust
/// Key for grouping coalesced requests. Requests in the same group
/// produce identical wire protocol bytes and can share a single worker execution.
#[derive(Hash, Eq, PartialEq)]
struct CoalesceKey {
    fingerprint: u64,
    query_type: QueryType,
    has_sync: bool,
    has_parse: bool,
    has_bind: bool,
    pipeline_describe: PipelineDescribe,
    result_formats: Vec<i16>,
    limit: Option<u64>,
}
```

In the common case (identical queries from the same application using the same driver), all waiters share one group. Heterogeneous clients (e.g., text vs binary protocol, different LIMITs) get separate worker executions while still benefiting from coalescing within their group.

### Waiting Queue on the Coordinator

Add a waiting queue to `QueryCache`. The outer map is keyed by fingerprint so that draining on a `WriterNotify::Ready`/`Failed` is O(1) without scanning every group. The inner map is keyed by `CoalesceKey` so requests that differ only in protocol parameters are grouped separately:

```rust
/// Outer key: fingerprint (O(1) drain on Ready/Failed).
/// Inner key: CoalesceKey grouping requests that share identical response bytes.
type WaitingQueue = HashMap<u64, HashMap<CoalesceKey, Vec<QueryRequest>>>;

/// Requests waiting for a Loading query to become Ready.
waiting: Rc<RefCell<WaitingQueue>>,
```

When a query arrives and the state is `Loading`:
1. Instead of calling `reply_forward`, push the `QueryRequest` into `waiting[fingerprint][coalesce_key]`.
2. The proxy connection enters `ProxyMode::CacheRead(reply_rx)` as usual — it's already waiting on its oneshot.

When the state transitions to `Ready` (the writer sends `WriterNotify::Ready` and the coordinator processes it):
1. Remove the fingerprint entry from `waiting` in one call, yielding all its coalescing groups.
2. For each group, dispatch a single `worker_request_send_coalesced` — one worker execution serves all waiters in the group (see [Draining the Queue](#draining-the-queue)).

### State Transitions That Drain the Queue

The queue must be drained on any terminal state change for the fingerprint:

| Transition | Action |
|---|---|
| Loading → Ready | Drain groups → `worker_request_send_coalesced` per group (cache hit) |
| Loading → Failed | Drain groups → `reply_forward` each waiter (fall back to origin) |

The Ready/Failed transitions happen on the writer thread (`query_ready_mark` / `query_failed_cleanup`), not on the coordinator. The coordinator has no direct visibility into these state changes — this is why the `WriterNotify` channel is needed (see [Coordinator Event Loop](#coordinator-event-loop)).

### LimitBump Loading

The LimitBump path also sets state to `Loading` (lines 181-182 of `query_cache.rs`, in the "cache hit: Ready but insufficient rows" arm). Requests arriving during a LimitBump should also be coalesced. However, there's a subtlety: the incoming request may need fewer rows than the bump target, in which case the existing cached rows are already sufficient. Since `limit_is_sufficient` has already failed for the request that triggered the bump, and subsequent requests may have different LIMIT values, the simplest approach is to queue them all and serve from cache when Ready.

### Invalidated / Readmit Loading

When a CDC-invalidated query is readmitted (lines 222-230), it transitions to `Loading` and triggers re-population. Requests during this window should also be coalesced — same mechanism.

### Coordinator Event Loop

The coordinator currently runs inside `cache_run` (`src/cache/runtime.rs`) on a `LocalSet`. Its `tokio::select!` has five arms: cancellation, a `query_tx.closed()` liveness check on the writer, `cdc_signal_rx` (from the CDC thread), `cache_rx` (proxy messages — each is handled by cloning `QueryCache` and calling `spawn_local(handle_proxy_message(...))`), and `worker_metrics_rx` (from the worker thread).

The coordinator does not directly process `QueryCommand::Ready/Failed` — those go to the writer thread. The writer updates `CacheStateView`, and the next time the coordinator dispatches a query for that fingerprint, it reads the updated state.

For coalescing, the coordinator needs to know when Loading → Ready happens so it can drain the queue promptly. Two approaches:

**Option A: Poll on next dispatch.** When the coordinator dispatches a query and sees state = Ready for a fingerprint that has waiters, drain the queue then. Simple, but waiters block until the next request for this fingerprint arrives — could be indefinitely.

**Option B: Notify channel from writer to coordinator.** The writer sends a notification when a query becomes Ready or Failed. The coordinator's event loop selects on this notification channel alongside the existing arms.

Option B is correct. Option A introduces unbounded latency for queued requests.

Today, `query_tx` (`UnboundedSender<QueryCommand>`) flows coordinator → writer and a `worker_metrics_tx` flows worker → coordinator. We add a symmetric channel for writer → coordinator:

```rust
/// Notifications from writer to coordinator.
notify_rx: UnboundedReceiver<WriterNotify>,
```

```rust
enum WriterNotify {
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

Plumbing on the writer side:
- Create `(notify_tx, notify_rx)` in `cache_run` alongside the other channels.
- Pass `notify_tx` into `writer_run` and store it on `CacheWriter` (add a `pub(super) notify_tx: UnboundedSender<WriterNotify>` field, populated by `CacheWriter::new`).
- Send from two sites, both in `src/cache/writer/query.rs`:
  1. **`query_ready_mark`** — after `state_view_update(... CachedQueryState::Ready ...)` completes. Send `WriterNotify::Ready { fingerprint, generation, resolved, max_limit }`.
  2. **`query_failed_cleanup`** — after `self.state_view.cached_queries.remove(&fingerprint)`. Send `WriterNotify::Failed { fingerprint }`.

Both `query_ready_mark` and `query_failed_cleanup` are also reached via the LimitBump and Invalidated/Readmit paths — when a LimitBump or readmit re-population completes, the same notify is emitted. No special handling is needed; the notify channel drains waiters regardless of which path triggered `Loading`.

**State-view ordering**: `query_ready_mark` currently publishes Ready into `state_view` *before* the notify point above, so a new proxy request arriving between the two steps sees Ready in the state view and serves it as a normal cache hit. The queued waiters are drained when the notify arrives moments later. This ordering must be preserved.

### Interaction with Predicate Subsumption

The `WriterNotify` channel is also used by predicate subsumption (see `predicate-subsumption.md`). When subsumption is implemented, a `Registered` variant will be added to signal the coordinator whether a newly registered query was subsumed by an existing cached query. This enables holding the first request in the waiting queue to give subsumption a chance to serve it from cache — but that behavior is deferred to the subsumption work. For now, the first request is forwarded to origin immediately (same as today), and only subsequent requests during `Loading` are coalesced.

### Coordinator Event Loop Change

Add `notify_rx` as an additional arm to the existing `tokio::select!` in `cache_run` (alongside cancel, `query_tx.closed()`, `cdc_signal_rx`, `cache_rx`, and `worker_metrics_rx`):

```rust
msg = notify_rx.recv() => {
    match msg {
        Some(WriterNotify::Ready { fingerprint, generation, resolved, max_limit }) => {
            // Population complete — drain waiters from cache
            qcache.waiting_drain_ready(fingerprint, resolved, generation, max_limit);
        }
        Some(WriterNotify::Failed { fingerprint }) => {
            // Population failed — forward waiters to origin
            qcache.waiting_drain_failed(fingerprint);
        }
        None => {
            error!("writer notify channel closed");
            return Err(CacheError::WriterFailure.into());
        }
    }
}
```

The drain methods should take `&self` rather than `&mut self` so they can be called on the outer `qcache` binding without interfering with the `qcache.clone()` used by the `cache_rx` arm.

**Note on shared state**: The coordinator runs in a `LocalSet`. Proxy message handlers are `spawn_local`'d with a *cloned* `QueryCache` (see `handle_proxy_message` dispatch in `cache_run`), while `WriterNotify` handling runs inline in the select loop on the original binding. Both paths need to mutate the `waiting` map. Since everything runs on one thread, wrap the map in `Rc<RefCell<WaitingQueue>>` inside `QueryCache` — cloning `QueryCache` clones the `Rc`, so spawned tasks and the inline handler share the same underlying queue.

**max_limit on Ready**: The notify carries `max_limit` so the coordinator can check `limit_is_sufficient` for each waiter before serving from cache. Waiters whose LIMIT exceeds the cached `max_limit` must be forwarded to origin (and optionally trigger a `LimitBump`), because the cache doesn't yet hold enough rows for them.

### Draining the Queue

**On Ready — broadcast via single worker request:**

Instead of dispatching N separate `WorkerRequest`s (each independently executing the same query against the cache DB), coalesced requests piggyback on a single worker execution. The coordinator builds one `WorkerRequest` from the first waiter and attaches the remaining waiters' sockets:

```rust
fn waiting_drain_ready(
    &self,
    fingerprint: u64,
    resolved: SharedResolved,
    generation: u64,
    max_limit: Option<u64>,
) {
    // O(1) removal of all coalescing groups for this fingerprint
    let Some(groups) = self.waiting.borrow_mut().remove(&fingerprint) else {
        return;
    };

    let mut served = 0u64;
    for (_key, mut waiters) in groups {
        // First waiter becomes the primary request
        let primary = waiters.remove(0);

        // Waiters whose LIMIT exceeds the cached max_limit can't be served from
        // cache — forward them to origin instead. (Optional refinement: trigger
        // a LimitBump for these.)
        let primary_needed = limit_rows_needed(&primary.cacheable_query.query.limit);
        if !limit_is_sufficient(max_limit, primary_needed) {
            let _ = reply_forward(primary.reply_tx, primary.pipeline, primary.data);
            for msg in waiters {
                let _ = reply_forward(msg.reply_tx, msg.pipeline, msg.data);
            }
            continue;
        }

        served += waiters.len() as u64;

        // Remaining waiters contribute their sockets and reply channels
        let coalesced: Vec<CoalescedClient> = waiters
            .into_iter()
            .map(|msg| {
                // Compute origin fallback bytes: pipeline buffer if pipelined, raw data otherwise
                let fallback = match msg.pipeline {
                    Some(pipeline) => pipeline.buffered_bytes,
                    None => msg.data,
                };
                CoalescedClient {
                    client_socket: msg.client_socket,
                    reply_tx: msg.reply_tx,
                    timing: msg.timing,
                    data: fallback,
                }
            })
            .collect();

        if let Err(e) = self.worker_request_send_coalesced(
            fingerprint,
            primary,
            Arc::clone(&resolved),
            generation,
            coalesced,
        ) {
            error!("coalesce serve failed: {e}");
        }
    }

    if served > 0 {
        metrics::counter!(names::CACHE_COALESCE_SERVED).increment(served);
    }
    metrics::gauge!(names::CACHE_COALESCE_WAITING).set(self.waiting_count() as f64);
}
```

This adds a `coalesced` field to `WorkerRequest` (next to the existing `fingerprint`, `query_type`, `data`, `resolved`, `generation`, `result_formats`, `client_socket`, `reply_tx`, `timing`, `limit`, `has_sync`, `has_parse`, `has_bind`, `pipeline_describe`, `parameter_description`, and `forward_bytes` fields):

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
    /// Pre-computed origin fallback bytes (pipeline.buffered_bytes or raw data).
    pub data: BytesMut,
}
```

A sibling helper `worker_request_send_coalesced` mirrors the existing `worker_request_send` but accepts a `coalesced: Vec<CoalescedClient>` parameter, which is stored on the outgoing `WorkerRequest`. The non-coalesced `worker_request_send` sets `coalesced: vec![]`.

**On Failed:**
```rust
fn waiting_drain_failed(&self, fingerprint: u64) {
    let Some(groups) = self.waiting.borrow_mut().remove(&fingerprint) else {
        return;
    };

    for (_key, waiters) in groups {
        for msg in waiters {
            let _ = reply_forward(msg.reply_tx, msg.pipeline, msg.data);
        }
    }

    metrics::gauge!(names::CACHE_COALESCE_WAITING).set(self.waiting_count() as f64);
}
```

### Worker Broadcast

Both the text and binary worker paths (`handle_cached_query_text`, `handle_cached_query_binary`) use a `WriteQueue` — a zero-copy `VecDeque<Bytes>` implementing `Buf` for vectored writes (`src/cache/write_queue.rs`) — and a `tokio::select!` loop with two concurrent arms:

1. **Read arm**: `frame = framed.next()` reads framed messages from the cache DB via `FramedRead`
2. **Write arm**: `client_socket.write_buf(&mut write_queue), if !write_queue.is_empty()` flushes queued bytes to the primary client socket

The worker runs on a `LocalSet` (via `worker_run` → `spawn_local` in `runtime.rs`). The broadcast design leverages this: each coalesced client gets its own `spawn_local` write task, fed via a `tokio::sync::broadcast` channel.

#### Broadcast setup (before the select loop)

When `msg.coalesced` is non-empty, create a broadcast channel and spawn a write task per coalesced client:

```rust
let broadcast = if msg.coalesced.is_empty() {
    None
} else {
    let (tx, _) = tokio::sync::broadcast::channel::<Bytes>(64);

    let tasks: Vec<JoinHandle<Result<CoalescedClient, CoalescedClient>>> = msg
        .coalesced
        .drain(..)
        .map(|mut client| {
            let mut rx = tx.subscribe();
            spawn_local(async move {
                loop {
                    match rx.recv().await {
                        Ok(chunk) => {
                            if client.client_socket.write_all(&chunk).await.is_err() {
                                return Err(client);
                            }
                        }
                        // Channel closed — producer finished, drain complete
                        Err(RecvError::Closed) => return Ok(client),
                        // Missed chunks — byte stream is corrupted, abort
                        Err(RecvError::Lagged(_)) => return Err(client),
                    }
                }
            })
        })
        .collect();

    Some((tx, tasks))
};
```

Each task writes chunks independently. A slow or dead coalesced client cannot block the primary or other coalesced clients because:
- `broadcast::Sender::send` is non-blocking — it never waits for receivers.
- A receiver that falls behind gets `RecvError::Lagged` (the broadcast channel drops old messages for slow receivers), at which point the task aborts since the byte stream is corrupted.
- Uses `spawn_local` (not `tokio::spawn`) since the worker runs on a `LocalSet`.

#### Broadcasting during the select loop

Every `write_queue.push(chunk)` is accompanied by a broadcast of the same chunk. Both paths (text and binary) already push protocol frames as `Bytes` / `BytesMut`, so the `Bytes::clone()` is just an Arc increment:

```rust
// Helper: push to write queue and broadcast to coalesced clients
fn push_and_broadcast(
    write_queue: &mut WriteQueue,
    broadcast_tx: Option<&broadcast::Sender<Bytes>>,
    data: impl Into<Bytes>,
) {
    let bytes: Bytes = data.into();
    write_queue.push(bytes.clone());
    if let Some(tx) = broadcast_tx {
        let _ = tx.send(bytes);
    }
}
```

This replaces every `write_queue.push(...)` call in both text and binary functions — prefix messages (ParseComplete, BindComplete), RowDescription, DataRow batches, CommandComplete, and the final ReadyForQuery are all broadcast.

The write arm of the select loop is unchanged — it still flushes `write_queue` to the primary socket only. Coalesced clients are written by their own spawned tasks.

#### After the select loop

After the state machine reaches `Done`:

1. Return the cache DB connection to the pool (existing — `guard.release()`).
2. Push ReadyForQuery to `write_queue` and broadcast it (existing logic, now via `push_and_broadcast`).
3. Final flush of `write_queue` to primary socket via `write_all_buf` (existing).
4. Drop `broadcast_tx` — this closes the channel, causing each coalesced task's `recv()` to return `RecvError::Closed`, which cleanly ends the task.
5. Join all coalesced tasks, collecting their outcomes.
6. Return `(bytes_served, coalesced_outcomes)` to the caller.

#### Return type change

The text and binary functions return coalesced task outcomes alongside the primary result:

```rust
/// Outcome of a coalesced client's write task.
enum CoalescedOutcome {
    /// All bytes were delivered successfully.
    Ok(CoalescedClient),
    /// Write failed or broadcast lagged — byte stream is corrupted.
    Failed(CoalescedClient),
}

pub async fn handle_cached_query(
    conn: CacheConnection,
    return_tx: Sender<CacheConnection>,
    msg: &mut WorkerRequest,
) -> CacheResult<(usize, Vec<CoalescedOutcome>)> { ... }
```

When `msg.coalesced` is empty, the returned `Vec` is empty and the hot path is identical to today — no broadcast channel is created, no tasks are spawned.

#### Joining coalesced tasks

After the primary's final flush, or on primary error, the coalesced tasks must be joined before the function returns. A helper shared by both text and binary paths:

```rust
async fn coalesced_tasks_join(
    broadcast: Option<(broadcast::Sender<Bytes>, Vec<JoinHandle<Result<CoalescedClient, CoalescedClient>>>)>,
) -> Vec<CoalescedOutcome> {
    let Some((tx, tasks)) = broadcast else {
        return vec![];
    };
    drop(tx); // close channel — tasks see RecvError::Closed

    let mut outcomes = Vec::with_capacity(tasks.len());
    for task in tasks {
        match task.await {
            Ok(Ok(client)) => outcomes.push(CoalescedOutcome::Ok(client)),
            Ok(Err(client)) => outcomes.push(CoalescedOutcome::Failed(client)),
            Err(_) => {} // JoinError — task panicked, client lost
        }
    }
    outcomes
}
```

#### Reply handling in `handle_worker_request`

The caller in `runtime.rs` sends replies for both primary and coalesced clients:

```rust
async fn handle_worker_request(
    conn: CacheConnection,
    return_tx: Sender<CacheConnection>,
    mut msg: WorkerRequest,
    worker_metrics_tx: UnboundedSender<WorkerMetrics>,
) {
    msg.timing.worker_start_at = Some(Instant::now());

    match handle_cached_query(conn, return_tx, &mut msg).await {
        Ok((bytes_served, coalesced_outcomes)) => {
            // Record metrics (existing)
            let latency_us = msg.timing.worker_start_at
                .map(|s| s.elapsed().as_micros() as u64)
                .unwrap_or(0);
            let _ = worker_metrics_tx.send(WorkerMetrics {
                fingerprint: msg.fingerprint,
                latency_us,
                bytes_served: bytes_served as u64,
            });

            // Primary reply (existing)
            let _ = msg.reply_tx.send(CacheReply::Complete(Some(msg.timing)));

            // Coalesced replies
            for outcome in coalesced_outcomes {
                match outcome {
                    CoalescedOutcome::Ok(client) => {
                        let _ = client.reply_tx.send(
                            CacheReply::Complete(Some(client.timing)),
                        );
                    }
                    CoalescedOutcome::Failed(client) => {
                        let _ = client.reply_tx.send(CacheReply::Error(client.data));
                    }
                }
            }
        }
        Err(e) => {
            error!("handle_cached_query failed: {e}");

            // Primary reply (existing)
            let error_buf = msg.forward_bytes.take()
                .unwrap_or_else(|| msg.data.split_off(0));
            let _ = msg.reply_tx.send(CacheReply::Error(error_buf));

            // Coalesced clients are already handled — see error path below
        }
    }
}
```

#### Error path

When the primary path fails (e.g., cache DB connection error, `guard.poisoned = true`), the text/binary function must still join and reply to coalesced clients before returning `Err`. This keeps the cleanup in scope with the task handles:

```rust
// Inside handle_cached_query_text, on any error return:
let coalesced_outcomes = coalesced_tasks_join(broadcast).await;
for outcome in coalesced_outcomes {
    let client = match outcome {
        CoalescedOutcome::Ok(c) | CoalescedOutcome::Failed(c) => c,
    };
    let _ = client.reply_tx.send(CacheReply::Error(client.data));
}

guard.poisoned = true;
return Err(CacheError::InvalidMessage.into());
```

Since coalesced replies are sent inside the worker on the error path, `handle_worker_request`'s `Err` branch does not need to handle them — they've already been replied to.

#### Protocol and LIMIT compatibility

The bytes pushed to `WriteQueue` include protocol framing that depends on `has_parse`, `has_bind`, `pipeline_describe`, `has_sync`, `query_type`, and `result_formats`. The LIMIT clause determines how many rows the worker fetches. For coalesced requests to receive valid protocol bytes and correct row counts, they must share identical values for all of these.

This is enforced at the coordinator level, not in the worker. The waiting queue groups waiters by `CoalesceKey` which includes all these parameters (see [Coalescing Groups](#coalescing-groups)). The worker can assume all coalesced clients expect identical bytes.

### Timeout

If the writer never sends Ready or Failed (e.g., population hangs), waiters would block indefinitely. This is already a problem for the proxy connection's oneshot — it has no timeout today. Coalescing doesn't make this worse; the proxy connection would time out or be closed by the client regardless.

A future improvement could add a timeout that forwards stale waiters (entries older than some threshold) to origin. But this is a separate concern and not required for v1.

## What Doesn't Change

- **First request**: The client that triggers registration is forwarded to origin immediately, same as today. Only subsequent requests arriving while state is `Loading` are coalesced. (Future work: predicate subsumption may hold the first request to check if it can be served from an existing cached superset — see `predicate-subsumption.md`.)
- **Pending state**: Requests during the admission gate (`Pending(n)`) are still forwarded — the query hasn't been admitted yet and there's nothing to coalesce on.
- **Non-coalesced cache hits**: Normal Ready cache hits (no waiters queued) follow the existing single-request path. The `coalesced` vec is empty and the worker behaves identically to today.
- **Population**: The writer and population workers are unchanged.
- **CacheStateView**: Unchanged. The coordinator reads state as before; the notify channel is a separate signal that carries the Ready/Failed data directly. (State-view-vs-notify ordering is discussed in the [Coordinator Event Loop](#coordinator-event-loop) section.)

## Observability

Add two metric name constants to `src/metrics.rs` (`names` module), alongside the existing `CACHE_*` metrics:

- **Gauge**: `CACHE_COALESCE_WAITING = "pgcache.cache.coalesce_waiting"` — current number of requests queued across all fingerprints.
- **Counter**: `CACHE_COALESCE_SERVED = "pgcache.cache.coalesce_served"` — requests served from cache via coalescing (instead of forwarding to origin).

The gauge is updated on every `waiting` map mutation (push, drain) from a `waiting_count()` helper. The counter is incremented by the number of coalesced (non-primary) waiters each time a group drains successfully on Ready.

## Value Proposition

Connection poolers like PgBouncer commonly funnel many application connections through a smaller number of database connections. When an application server starts up or a new query pattern appears, a burst of identical queries can arrive simultaneously. Without coalescing, each one independently hits origin. With coalescing, one hits origin while the rest wait a few milliseconds and get served from cache.

This is particularly impactful for:
- **Application cold start**: Many connections issuing the same configuration/lookup queries
- **Multi-tenant workloads**: Same query template with same tenant parameter arriving from multiple connections
- **Dashboard queries**: Multiple users viewing the same dashboard simultaneously
