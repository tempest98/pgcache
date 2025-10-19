# ADR-004: Tokio Select for Connection Multiplexing

## Status
Accepted

## Context

Each client connection must simultaneously monitor multiple asynchronous I/O sources:
1. Client messages (frontend protocol)
2. Origin database messages (backend protocol)
3. Cache replies (responses from cache subsystem)
4. Outbound writes to client, origin, and cache

The challenge is how to efficiently multiplex these I/O sources while maintaining clear control flow.

### Initial Approach: StreamMap with ProxyMode

The implementation used tokio-stream's `StreamMap` to combine client_read, origin_read, and cache_reply streams into a single multiplexed stream. The cache_reply stream was dynamically inserted into the StreamMap when cache operations started and removed on completion, using string keys ("client", "origin", "cache_reply") to identify sources. ProxyMode controlled when to read vs write.

**Problems:**
- Dynamic stream insertion/removal when transitioning to/from cache operations
- Converting `Receiver<CacheReply>` into a boxed stream created ownership complexity
- String-based stream identification in StreamMap was error-prone
- Lifecycle of cache_reply stream not obvious from code structure

## Decision

Use **tokio::select! macro** instead of StreamMap for I/O multiplexing, while keeping ProxyMode for control flow.

Key changes:
- `ProxyMode::CacheRead(Receiver<CacheReply>)` directly owns the cache receiver instead of wrapping it in a stream
- Separate `connection_select()` and `connection_select_with_cache()` methods with dedicated select! blocks for different I/O combinations
- Write buffers checked via select! guards (`if !buf.is_empty()`)
- No dynamic stream insertion/removal

## Rationale

**Clear Control Flow**: Eliminates dynamic stream manipulation. The set of I/O sources being monitored is explicit in each select! block.

**Type Safety**: `ProxyMode::CacheRead(Receiver<CacheReply>)` directly owns the receiver. Compiler ensures receiver is available when in CacheRead mode, vs runtime string lookups in StreamMap.

**Ownership Simplicity**: Cache receiver owned by ProxyMode variant, extracted via pattern matching. No boxing or stream conversion.

## Consequences

### Positive
- Simpler state transitions - no dynamic stream insertion/removal
- Better type safety with compiler-enforced mode correctness
- Clearer code - each select! block explicitly shows what it's monitoring
- Current mode and owned resources always visible in ProxyMode value

### Negative
- Some code duplication in write buffer handling across select! blocks
- Requires separate methods for different I/O source combinations

### Implementation Notes

When sending query to cache: create channel, send with reply_tx, then transition `state.proxy_mode = ProxyMode::CacheRead(reply_rx)`. Next loop iteration uses `connection_select_with_cache()` which extracts the receiver via `let ProxyMode::CacheRead(ref mut cache_rx) = self.proxy_mode`.
