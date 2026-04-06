# ADR-021: Observability Admin API

## Status
Accepted

## Context
pgcache needs operational visibility for deployment in production environments. Operators need health checks for load balancer integration, Prometheus metrics for monitoring and alerting, and on-demand status inspection of the cache subsystem and individual cached queries. The original metrics server was a hand-rolled TCP listener that only served Prometheus metrics. There was no health checking or cache introspection capability.

## Decision
Replace the hand-rolled metrics server with an HTTP admin server that serves health, metrics, and status endpoints. The server uses hyper for HTTP/1.1 and runs on a dedicated thread with its own single-threaded tokio runtime.

### Endpoints

| Route | Response | Purpose |
|-------|----------|---------|
| `GET /healthz` | `200 "OK"` | Liveness probe — returns OK if the process is running |
| `GET /readyz` | `200 "OK"` or `503 "not ready"` | Readiness probe — checks whether the cache is running |
| `GET /metrics` | Prometheus text format | Prometheus scrape endpoint with CORS support |
| `GET /status` | JSON | On-demand cache, CDC, and per-query status |

### Status Data Gathering

The `/status` endpoint gathers data from the cache subsystem via message passing. The admin HTTP handler sends a `StatusRequest` (carrying a oneshot reply channel) through an `mpsc` channel to the cache runtime. The cache writer builds the full response from its owned state and responds via the oneshot.

```
HTTP handler ──StatusRequest──> cache runtime ──> writer builds response
             <──StatusResponse── (oneshot)
```

The response includes three sections:
- **`cache`**: size, generation, policy, tables tracked, queries registered, uptime, hit/miss counts
- **`cdc`**: tracked tables, LSN positions, replication lag
- **`queries`**: per-query operational metrics (hits, misses, invalidations, evictions, latency stats, etc.)

CDC status is communicated to the writer via the existing `CdcCommand` channel — the CDC processor periodically sends its LSN and table state to the writer, which caches the latest values. This avoids fan-out from the admin handler to multiple subsystems.

### Readiness via Shared Atomic

The proxy accept loop knows whether the cache thread is alive. This state is exposed via `SharedProxyStatus`, an `Arc<AtomicU8>` wrapper that the proxy updates directly. The admin server reads it for `/readyz` — no message passing needed for this lightweight check.

### Timeout and Unavailability

Status requests have a 2-second timeout. If the cache thread is down or unresponsive, the handler returns `503` with a JSON error body. This prevents the admin server from hanging when the cache subsystem is in a bad state.

## Rationale
- **Dedicated thread** isolates the admin server from proxy and cache work — health checks remain responsive even under load
- **Message passing for status** avoids shared mutable state between the admin server and cache writer; the writer builds the response from data it already owns
- **Atomic for readiness** is appropriate for a single enum value that changes infrequently — simpler than a channel round-trip for every readiness probe
- **hyper** provides a minimal, well-tested HTTP/1.1 server without pulling in a full web framework
- **Single `/status` endpoint** returning the complete response is simpler than sub-routes and lets clients filter client-side

## Consequences

### Positive
- Load balancers can use `/healthz` and `/readyz` for health checking
- Prometheus integration via `/metrics` with CORS support for browser-based dashboards
- `/status` provides deep cache introspection without log parsing — useful for debugging and operational dashboards
- Status gathering is non-blocking to the cache hot path — bounded channel with small capacity (2) and timeout

### Negative
- Status response is all-or-nothing — no way to request just CDC or just query data without the full gather
- The 2-second timeout means `/status` can be slow when the writer is busy processing a large CDC batch

## Implementation Notes
- Admin server: `admin_server_spawn()` and `admin_server_run()` in `src/metrics.rs`
- Status types: `StatusRequest`, `StatusResponse`, `CacheStatusData`, `CdcStatusData`, `QueryStatusData`, `LatencyStats` in `src/cache/status.rs`
- Status response building: `CacheWriter::status_respond()` in `src/cache/writer/core.rs`
- Readiness state: `SharedProxyStatus` in `src/proxy/mod.rs`, updated by the proxy accept loop in `server.rs`
- Status channel: `mpsc::channel<StatusRequest>` with capacity 2, plumbed through `cache_run()` in `runtime.rs`
