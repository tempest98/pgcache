# ADR-001: Multi-threaded Architecture with Scoped Threading

## Status
Accepted

## Context

The pgcache proxy needs to handle two primary concerns simultaneously:
1. **Network proxy operations**: Accept client connections, parse PostgreSQL protocol messages, and forward traffic to origin databases
2. **Cache management**: Process query results, handle cache invalidation via CDC, and manage cached data lifecycle

Several architectural approaches were considered:

### Option 1: Multi-threaded async with global executor
- Use tokio's default multi-threaded runtime with `Send + Sync + 'static` bounds
- Share state between proxy and cache components via Arc/Mutex
- Standard approach in most async Rust applications

### Option 2: Multi-threaded with scoped threads and local executors
- Dedicated threads for proxy and cache concerns using `thread::scope()`
- Each thread runs its own `tokio::runtime::Builder::new_current_thread()` runtime
- Use `spawn_local()` to avoid `Send + Sync + 'static` requirements
- Communication between threads via channels

### Option 3: Alternative async runtimes
- Consider io_uring-based runtimes like glommio or monoio for performance
- Potentially better I/O performance but ecosystem compatibility concerns

## Decision

Chose **Option 2: Multi-threaded with scoped threads and local executors**.

Key implementation details:
- Main thread spawns a "proxy" scoped thread that manages both network operations and cache lifecycle
- Proxy thread spawns a "cache" child thread for cache management
- Proxy thread spawns multiple "worker" threads (cnxt) to handle individual client connections
- Each thread creates its own current-thread tokio runtime
- Tasks within each thread use `spawn_local()` to avoid Send bounds
- Inter-thread communication via `mpsc::channel()` for structured message passing

## Rationale

### Advantages of the chosen approach:

**1. Separation of Concerns**
- Clean isolation between network proxy logic and cache management
- Easier reasoning about component interactions and data flow
- Each thread can be optimized for its specific workload patterns

**2. Avoiding Send + Sync + 'static**
- Local executors eliminate the need for `Send + Sync + 'static` bounds on most types
- More natural Rust ownership patterns without excessive Arc/Mutex wrapping
- Borrowed references can be used more freely within each thread's context

**3. Performance Characteristics**
- Current-thread executors have lower overhead than work-stealing schedulers
- Better cache locality when tasks stay on the same thread
- Predictable scheduling behavior for latency-sensitive proxy operations

**4. Ecosystem Compatibility**
- Full compatibility with tokio ecosystem (tokio-postgres, tokio-stream, etc.)
- No concerns about library compatibility that would exist with alternative runtimes

### Technical Implementation Notes

**Thread Hierarchy**:
- Main thread → Proxy thread → Cache thread (child)
- Main thread → Proxy thread → Worker threads (children, created on-demand)

**Socket Binding Constraint**: The proxy must bind and accept connections on the same executor due to tokio's internal socket handling. This was discovered during implementation.

**Channel-based Communication**: Structured message passing between threads via typed channels (`ProxyMessage`, `CacheMessage`) provides clear interfaces and prevents shared mutable state.

**Cache Thread Restart**: When the cache thread exits (detected via failed channel sends), the proxy thread automatically restarts it with a fresh channel. Worker threads with stale cache channels enter degraded mode (pass-through proxy) and are lazily replaced as connections complete.

**Worker Thread Management**: Worker threads are created dynamically as needed and automatically restarted if they exit. The accept loop uses round-robin distribution to balance load across workers.

## Consequences

### Positive
- **Maintainability**: Clear separation makes the codebase easier to understand and modify
- **Performance**: Reduced contention and overhead compared to global thread pools
- **Type Safety**: Fewer lifetime and thread safety constraints in business logic
- **Debugging**: Thread-local state is easier to reason about and debug

### Negative
- **Complexity**: Requires more upfront architectural planning than throwing everything on a global executor
- **Channel Overhead**: Inter-thread communication has serialization/copying costs
- **Resource Usage**: Multiple tokio runtimes consume more memory than a single global runtime

### Trade-offs
- **Scalability vs Simplicity**: Architecture scales well with additional worker threads but requires careful channel management
- **Performance vs Flexibility**: Thread-local executors optimize for the common case but make dynamic work distribution more complex

## References
- [Async Rust can be a pleasure to work with (without Send + Sync + 'static)](https://emschwartz.me/async-rust-can-be-a-pleasure-to-work-with-without-send-sync-static/)
- [Local async executors and why they might be the next big thing](https://maciej.codes/2022-06-09-local-async/)
