# ADR-025: CacheConnection Hot Path

## Status
Accepted

## Context
The cache worker thread serves cached query results to clients — the hot path that determines cache-hit latency. Originally this used `tokio_postgres::Client` to query the cache database, which deserializes every row into Rust types and then re-serializes back to wire format for the client. For a caching proxy that simply forwards result frames, this round-trip through a typed representation is pure overhead.

The alternative considered was optimizing within `tokio_postgres` (custom row handlers, etc.), but the client library's architecture fundamentally requires decoding rows before they can be accessed. The only way to achieve zero-copy forwarding is to operate at the protocol frame level.

## Decision
Replace `tokio_postgres::Client` on the worker hot path with `CacheConnection` — a lightweight wrapper around a raw `TcpStream` with a PostgreSQL protocol codec. Key design choices:

- **Zero-copy frame forwarding**: Read framed protocol messages from the cache DB and write them directly to the client socket without deserializing row data
- **`tokio::select!` streaming**: Concurrently read frames from cache DB and flush to the client — the client receives data while the worker is still reading the next frame
- **Channel-based connection pool**: Bounded `mpsc::channel<CacheConnection>` sized at `num_workers * 2` (min 4), pre-filled at startup. No custom pool data structure needed
- **ConnectionGuard with poisoning**: RAII guard returns connections to the pool on success. On error, the connection is marked poisoned and discarded on drop to prevent stale response data from corrupting the next query
- **Buffer recycling**: Each connection carries a persistent 64KB read buffer that is reused across queries, avoiding per-query allocation
- **Early connection return**: Connection is returned to the pool immediately after the last frame is read from cache DB — before the final flush to a potentially slow client
- **Two protocol paths**: Text (simple query) and binary (extended query with binary format), both combining the `SET mem.query_generation` and `SELECT` into a single pipelined write

`tokio_postgres` is retained for non-hot-path operations: cache database reset at startup and writer thread operations where ergonomics matter more than per-row overhead.

## Rationale
- **Frame-level forwarding** is the only way to eliminate deserialize/re-serialize overhead — optimizing within a client library can't achieve this
- **Channel as pool** gives bounded capacity, async synchronization, and FIFO ordering with zero custom code
- **Poisoning** prevents a class of subtle bugs where a broken connection's partial response bleeds into the next query
- **Buffer recycling** keeps the hot path allocation-free after warmup — the read buffer grows to fit the largest response and stays there

## Consequences

### Positive
- Cache-hit latency dominated by network I/O rather than per-row processing
- No memory allocation per row on the hot path
- Write pipelining via `select!` reduces end-to-end latency for large result sets
- Connection pool is simple and deadlock-free

### Negative
- Hand-rolled protocol state machine is more fragile than a client library — protocol bugs could cause subtle issues
- Trust authentication only (acceptable since pgcache controls the cache DB)
- Under sustained errors, poisoning can drain the pool; recovered on cache restart

## Implementation Notes
`CacheConnection` in `src/pg/cache_connection.rs`, `ConnectionGuard` and worker hot path in `src/cache/worker.rs`, pool creation in `src/cache/runtime.rs`.
