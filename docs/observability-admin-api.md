# Observability Admin API

## Goal

Add an HTTP admin server using vetis that serves `/healthz`, `/readyz`, `/metrics`,
and `/status` (with sub-routes `/status/queries` and `/status/cdc`). Replaces the
existing hand-rolled metrics HTTP server. Status data is gathered on demand via
message passing to the cache writer and CDC processor.

## Status Data Gathering — Message Passing

### Design

The admin HTTP handler sends a request on a channel to the cache subsystem, which
gathers the data from the writer and CDC processor and responds via a oneshot channel.

```
HTTP handler ──StatusRequest──> cache runtime ──> writer: build query/cache status
                                              ──> CDC: build CDC status
             <──StatusResponse── (oneshot)
```

### New message types (`src/cache/messages.rs`)

```rust
/// Request for current cache status, sent from admin HTTP handler.
pub struct StatusRequest {
    pub reply_tx: oneshot::Sender<StatusResponse>,
}

pub struct StatusResponse {
    pub cache: CacheStatusData,
    pub cdc: CdcStatusData,
    pub queries: Vec<QueryStatusData>,
}

pub struct CacheStatusData {
    pub state: String,              // "running", "restarting", "down"
    pub size_bytes: usize,
    pub size_limit_bytes: Option<usize>,
    pub generation: u64,
    pub tables_tracked: usize,
    pub policy: String,
}

pub struct CdcStatusData {
    pub state: String,              // "streaming", "disconnected"
    pub tables: Vec<String>,
    pub last_received_lsn: u64,
    pub last_flushed_lsn: u64,
    pub lag_bytes: u64,
}

pub struct QueryStatusData {
    pub fingerprint: u64,
    pub sql_preview: String,
    pub tables: Vec<String>,
    pub state: String,
    pub cached_bytes: usize,
    pub max_limit: Option<u64>,
    pub pinned: bool,
}
```

All status types derive `serde::Serialize` for JSON output.

### Channel plumbing

The cache runtime's event loop (`runtime.rs`) already selects on proxy messages and
CDC commands. Add a third channel: `mpsc::Receiver<StatusRequest>` (bounded, small
capacity like 4).

When a `StatusRequest` arrives, the writer builds `CacheStatusData` +
`Vec<QueryStatusData>` from its owned `Cache` struct.

#### CDC status via writer

CDC processor periodically sends its LSN/state to the writer via the existing
`CdcCommand` channel (new variant `CdcCommand::StatusUpdate { ... }`). Writer caches
the latest CDC state. When a status request arrives, writer has everything it needs
to build the full response.

This keeps the fan-out simple — one channel to the cache runtime, one response back.
CDC already sends messages to the writer, so this is a natural extension.

### Cache thread state (running/restarting/down)

The proxy accept loop (`ProxyCacheState` in `server.rs`) knows whether the cache
thread is alive, restarting, or down. This state is exposed via an `Arc<AtomicU8>`
that `server.rs` updates directly. The admin server reads it for `/readyz` and
includes it in `/status` responses.

This is a single enum value — lightweight enough that shared atomic state is
appropriate, and avoids routing status requests through the proxy accept loop.

## Admin HTTP Server (`src/admin.rs`)

### Setup

- Runs on its own `std::thread` with a single-threaded tokio runtime (same pattern
  as current metrics server in `metrics.rs`)
- Receives: `PrometheusHandle`, `mpsc::Sender<StatusRequest>`, `Arc<AtomicU8>`
  (cache state)
- Binds to the configured admin socket address
- Uses vetis with `tokio-rt` and `http1` features (minimal footprint)

### Endpoints

| Route | Response | Source |
|-------|----------|--------|
| `GET /healthz` | `200 "OK"` | Always returns OK if process is running |
| `GET /readyz` | `200 "OK"` or `503 "not ready"` | Checks cache state atomic: ready if `Running` |
| `GET /metrics` | Prometheus text format | `PrometheusHandle::render()` |
| `GET /status` | JSON `StatusResponse` | Send `StatusRequest`, await oneshot |
| `GET /status/queries` | JSON `Vec<QueryStatusData>` | Same request, return `.queries` |
| `GET /status/cdc` | JSON `CdcStatusData` | Same request, return `.cdc` |

### Timeout

Status requests get a timeout (e.g. 2s). If the cache thread is down or
unresponsive, the handler returns `503` with a JSON error body indicating status is
unavailable, plus the cache state from the atomic.

## Configuration

### Settings

```rust
pub struct AdminSettings {
    pub socket: SocketAddr,
}
```

- CLI: `--admin-listen 127.0.0.1:9090`
- TOML: `[admin] listen = "127.0.0.1:9090"`

### Backward compatibility with `--metrics`

When `--admin-listen` is set, the admin server serves `/metrics` and the old metrics
TCP server is not started. If only `--metrics` is set (no `--admin-listen`), the old
behavior is preserved. Both cannot be set simultaneously — startup error if both are
configured.

## File Changes

| File | Change |
|------|--------|
| `src/admin.rs` | **New** — vetis server, route handlers, status request/response |
| `src/cache/messages.rs` | Add `StatusRequest`, `StatusResponse`, and status data types |
| `src/cache/runtime.rs` | Accept status channel, select on it, dispatch to writer |
| `src/cache/writer/core.rs` | Method to build status from `Cache`; store latest `CdcStatusData` |
| `src/cache/cdc.rs` | New `CdcCommand::StatusUpdate` variant; periodically send CDC state to writer |
| `src/lib.rs` | Add `pub mod admin;` |
| `src/settings.rs` | Add `AdminSettings`, CLI parsing |
| `src/main.rs` | Create status channel, cache state atomic, start admin server thread |
| `src/proxy/server.rs` | Update cache state atomic on exit/restart/recovery |
| `src/metrics.rs` | Conditionally skip HTTP server when admin server is active |
| `Cargo.toml` | Add `vetis`, `serde_json` |

## Execution Order

1. Add status data types to `messages.rs` (with `Serialize`)
2. Add `CdcCommand::StatusUpdate` — CDC sends state to writer periodically
3. Add status-building methods to writer (`CacheWriter::status_build`)
4. Wire status channel through `cache_run` → runtime event loop
5. Add `AdminSettings` to settings/CLI
6. Build admin server (`admin.rs`) with vetis
7. Wire admin server startup into `main.rs`
8. Add cache state atomic, update from `server.rs`
9. Conditionally disable old metrics server when admin is active
10. Manual testing, unit tests for serialization

## Open Decisions

1. **CDC → writer status update frequency**: On every keepalive/batch, or on a
   separate timer (e.g. every 5s)? Keepalive is simplest since it already fires
   periodically.
2. **`--metrics` vs `--admin-listen` conflict**: Error at startup if both set.
3. **Status response when cache is down**: Return 503 with partial data (cache state
   enum + empty queries/cdc), or 200 with a `"state": "down"` field?
