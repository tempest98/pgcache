# ADR-023: Pinned Queries

## Status
Accepted

## Context
pgcache only caches queries after they have been seen in the request flow. For some workloads, operators know ahead of time which queries should always be cached — reference tables, configuration data, lookup tables. These queries should be pre-populated at startup, protected from eviction, and automatically re-populated after CDC invalidation without waiting for a client request.

## Decision
Add pinned queries: queries configured at startup that are pre-populated in the cache and receive special treatment throughout their lifecycle. Pinned queries are never evicted and are automatically re-admitted after CDC invalidation.

### Configuration

```toml
# Pin specific queries
pinned_queries = [
    "SELECT * FROM settings",
    "SELECT p.id, p.name FROM products p JOIN categories c ON c.id = p.category_id"
]

# Pin entire tables (syntactic sugar)
# Each table expands to: SELECT * FROM {table}
pinned_tables = ["settings", "products"]
```

Both options can coexist — `pinned_tables` entries are expanded and merged into `pinned_queries` at config build time. On the CLI, `--pinned_queries` uses semicolon separation (since SQL contains commas) and `--pinned_tables` uses comma separation.

Schema-qualified table names are supported (e.g. `analytics.events` expands to `SELECT * FROM analytics.events`).

### Registration Flow

The registration follows the existing query flow, with each layer doing what it already does:

1. **Proxy** (`pinned_queries_validate()` in `server.rs`): Parses pinned query SQL, checks cacheability via `CacheableQuery::try_new()`, computes fingerprint. Produces a `Vec<PinnedQuery>` of pre-validated entries. Invalid or uncacheable queries are logged as warnings and skipped — pgcache starts normally.

2. **Coordinator** (`pinned_queries_register()` on `QueryCache`): Sets `Loading` state in `CacheStateView`, sends `QueryCommand::Register { pinned: true }` to writer. Bypasses the admission gate (CLOCK threshold) — pinned queries go straight to `Loading` with `AdmitAction::Admit`.

3. **Writer**: Handles `Register` through the normal code path — resolves against catalog, populates from origin, transitions to `Ready`. The `pinned: bool` flag is stored on `CachedQuery` for downstream behavior.

### Eviction Protection

Pinned queries are never evicted. During `eviction_run()`, pinned queries are unconditionally bumped to a newer generation (not bounded by the CLOCK `MAX_BUMPS` limit). A `pinned_skips` counter prevents infinite loops when all remaining eviction candidates are pinned — the loop breaks, leaving the cache over its size limit but with nothing evictable.

The unbounded bump is necessary because the eviction loop starts at the minimum active generation. Skipping a query at the minimum without bumping would cause the same query to be visited on the next iteration indefinitely.

### CDC Auto-Readmit

When a CDC event invalidates a pinned query, instead of evicting (FIFO) or leaving it invalidated (CLOCK), the CDC handler sends a `QueryCommand::Readmit` back through the writer's internal channel. This cleanly separates CDC processing from population dispatch — the writer picks up the readmit on its next event loop iteration and re-populates through the existing `query_readmit()` path.

The pinned check happens **before** the FIFO/CLOCK branch in `cache_query_cdc_invalidate()`. This ordering is critical because the FIFO branch calls `cache_query_evict()` which would destroy the `CachedQuery` entry and the `pinned` flag with it.

### Subsumption Synergy

A pinned `SELECT * FROM settings` has empty constraints and no LIMIT. Per the subsumption rules, any query against `settings` with WHERE constraints will be subsumed by the pinned query — the empty constraint set is a subset of any new constraint set. This means pinning a whole table effectively caches all queries against that table.

### Interaction with Allowlist

Pinned queries are independent of the table allowlist. Pinning a query does NOT implicitly allowlist its tables. If a pinned query references tables not in the allowlist, the allowlist check in `query_dispatch()` does not apply because pinned queries are registered directly through the coordinator, not through the normal dispatch path.

### Known Limitations

Pinned queries use `["public"]` as the default search path. Schema-qualified table names in the SQL work correctly, but unqualified tables resolve against `public` only.

## Rationale
- **Reuses existing registration flow** rather than creating a parallel path — proxy validates, coordinator manages state, writer executes. The only new code is the `pinned` flag and its effects on eviction and CDC
- **`Readmit` as a separate command** keeps CDC processing fast and avoids doing population work inline during CDC event handling
- **Unconditional eviction bumps** are the simplest correct approach — pinned queries are expected to be a small fraction of total cached queries, so the cost of bumping them is negligible
- **Config-time expansion** of `pinned_tables` to `pinned_queries` keeps the downstream code simple — it only deals with fully-formed SQL strings

## Consequences

### Positive
- Reference/lookup tables can be pre-warmed at startup — first client request is a cache hit
- Pinned queries survive eviction pressure from transient query patterns
- Auto-readmit after CDC means pinned queries are self-healing — always cached or in the process of being re-populated
- Subsumption means pinning a whole table covers all filtered queries against that table

### Negative
- Pinned queries consume cache capacity that cannot be reclaimed — if too much is pinned, non-pinned queries may not have room
- If all eviction candidates are pinned, the cache can exceed its size limit with nothing evictable
- Search path limitation means unqualified tables in pinned queries only resolve against `public`
- Startup-only configuration — changing pinned queries requires a restart

## Implementation Notes
- Config: `pinned_queries`, `pinned_tables` in `src/settings.rs` with `pinned_tables_expand_and_merge()` and `pinned_queries_parse()`
- Types: `PinnedQuery` struct and `pinned: bool` on `CachedQuery` in `src/cache/types.rs`
- Messages: `pinned: bool` on `QueryCommand::Register`, `QueryCommand::Readmit` variant in `src/cache/messages.rs`
- Proxy validation: `pinned_queries_validate()` in `src/proxy/server.rs`
- Coordinator registration: `pinned_queries_register()` in `src/cache/query_cache.rs`
- Eviction protection: `eviction_run()` in `src/cache/writer/core.rs`
- CDC auto-readmit: `cache_query_cdc_invalidate()` in `src/cache/writer/cdc.rs`
- Integration tests: `tests/pinned_query_test.rs`
