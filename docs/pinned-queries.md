# Pinned Queries

## Context

pgcache currently caches queries only when they have been seen in the request flow. Users may know ahead of time that some queries are useful to have permanently cached.

**Pinned queries**: Pin specific queries at startup so they are pre-populated in cache, protected from eviction, and automatically re-populated after CDC invalidation.

---

## Configuration

### TOML (primary)

```toml
# Pin specific queries
pinned_queries = [
    "SELECT * FROM settings WHERE true",
    "SELECT p.id, p.name FROM products p JOIN categories c ON c.id = p.category_id WHERE true"
]

# Pin entire tables (syntactic sugar)
# Each table expands to: SELECT * FROM {table} WHERE true
pinned_tables = ["settings", "products"]

# Both options can coexist — pinned_tables entries are expanded and appended to pinned_queries
```

### CLI

```
--pinned_queries "SELECT * FROM settings WHERE true;SELECT * FROM products WHERE true"
--pinned_tables "settings,products"
```

- `pinned_queries`: semicolon-separated (commas cannot be used because SQL queries contain commas)
- `pinned_tables`: comma-separated table names (reuses `cache_tables_parse()`)

### Settings

- `pinned_queries: Option<Vec<String>>` on both `SettingsToml` and `Settings`
- `pinned_tables: Option<Vec<String>>` on `SettingsToml` only — expanded and merged into `pinned_queries` at config build time via `pinned_tables_expand_and_merge()`
- Schema-qualified table names are supported (e.g. `analytics.events` expands to `SELECT * FROM analytics.events WHERE true`)
- TOML array is the preferred format; CLI is a convenience for testing
- Tables in pinned queries are NOT automatically allowlisted — they are independent of the allowlist

---

## Architecture: Proxy Validates, Coordinator Registers, Writer Executes

Each layer does what it's already responsible for:

1. **Proxy** (`proxy_run`): Parses pinned query SQL, checks cacheability via `CacheableQuery::try_new(func_volatility)`, computes fingerprint. Produces a `Vec<PinnedQuery>` of pre-validated entries. Emits warnings for invalid/uncacheable queries and skips them.

2. **Coordinator** (`cache_run`/`QueryCache`): Receives validated `PinnedQuery` entries. Sets `Loading` state in `CacheStateView`, sends `QueryCommand::Register { pinned: true }` to writer.

3. **Writer**: Receives `Register` as usual — resolves, populates, tracks `pinned` flag for eviction and CDC behavior. Same code path as any other query registration.

This matches the normal query flow where the proxy handles parsing/cacheability and the coordinator handles state management.

### PinnedQuery Type

```rust
/// A pre-validated pinned query, ready for registration.
pub struct PinnedQuery {
    pub fingerprint: u64,
    pub cacheable_query: Arc<CacheableQuery>,
}
```

Defined in `src/cache/types.rs`, re-exported from `src/cache/mod.rs`.

### Proxy-Side Validation

In `proxy_run()`, after loading `func_volatility` but before `cache_create()`:

```rust
let pinned = pinned_queries_validate(&settings, &func_volatility);
let (cache_handle, cache_tx) = cache_create(scope, settings, &pinned)?;
```

`pinned_queries_validate()` lives in `src/proxy/server.rs` and iterates `settings.pinned_queries`:
1. Parse via `pg_query::parse()` -> `query_expr_convert()`. On error, emit `warn!("pinned query not parseable, skipping: {sql}")` and continue.
2. Check cacheability via `CacheableQuery::try_new(&query, &func_volatility)`. On error, emit `warn!("pinned query not cacheable, skipping: {sql}")` and continue.
3. Compute fingerprint via `query_expr_fingerprint()`
4. Collect into `Vec<PinnedQuery>`

### Startup Registration

`cache_create()` passes validated pinned queries to `cache_run()`. In `cache_run()`, after creating `QueryCache` but before entering the event loop:

```rust
qcache.pinned_queries_register(&pinned)?;
```

`pinned_queries_register()` on `QueryCache` iterates the pre-validated entries and for each:
1. Sets `Loading` state in `CacheStateView`
2. Sends `QueryCommand::Register { fingerprint, cacheable_query, search_path: vec!["public"], pinned: true }` to writer

The search path defaults to `["public"]`. Schema-qualified table names in the SQL string (e.g. `select * from analytics.events`) are supported since the schema is parsed from the query AST, but unqualified tables will resolve against `public` only. This is a known limitation.

---

## QueryCommand Changes

```rust
enum QueryCommand {
    Register {
        fingerprint: u64,
        cacheable_query: Arc<CacheableQuery>,
        search_path: Vec<String>,
        started_at: Instant,
        subsumption_tx: oneshot::Sender<SubsumptionResult>,
        admit_action: AdmitAction,
        pinned: bool,  // normal Register uses false
    },

    /// Deferred readmission for pinned queries after CDC invalidation.
    /// Sent by the CDC handler back through the writer's internal channel
    /// to avoid doing population work inline during CDC processing.
    Readmit { fingerprint: u64 },

    // ... existing variants unchanged
}
```

### CachedQuery.pinned Flag

`pinned: bool` on `CachedQuery` (default `false`). Set to `true` when the writer receives a `Register { pinned: true }`.

The writer uses this flag for:
- **Eviction protection**: Pinned queries are never evicted during `eviction_run()`
- **CDC auto-readmit**: On invalidation, a `Readmit` command is sent to re-populate instead of evicting or leaving invalidated
- **Stale cleanup skip**: Pinned invalidated entries are not cleaned up by `stale_entries_cleanup()`

---

## Eviction Protection

Pinned queries are never evicted. During `eviction_run()`, pinned queries are always bumped to a newer generation (unconditionally, not bounded by `MAX_BUMPS`). This is separate from the CLOCK reference-bit bumps which remain bounded.

```rust
// In eviction_run():
// Pinned queries are never evicted — always bump to move past them.
if query_pinned {
    self.cache_query_generation_bump(fingerprint).await?;
    pinned_skips += 1;
    if pinned_skips >= self.cache.cached_queries.len() {
        break; // all remaining candidates are pinned
    }
    continue;
}

// CLOCK second-chance: referenced queries get bumped (bounded by MAX_BUMPS)
if self.cache.cache_policy == CachePolicy::Clock && bumps < MAX_BUMPS {
    // ...
}
```

The `pinned_skips` counter prevents infinite loops when all remaining candidates are pinned — the eviction loop breaks, leaving the cache over its size limit but with nothing evictable.

Pinned bumps are unbounded because the eviction loop always starts at `generations.first()` (the minimum active generation). Skipping a query at the minimum generation without bumping it would cause an infinite loop — the same query would be visited again on the next iteration.

---

## CDC Auto-Readmit

In `cache_query_cdc_invalidate()`, the pinned check happens **before** the FIFO/CLOCK branch. This ordering is critical because the FIFO branch calls `cache_query_evict()` which would destroy the `CachedQuery` entry (and the `pinned` flag with it).

Instead of doing population work inline during CDC processing, the handler sends a `QueryCommand::Readmit` back through the writer's internal `query_tx` channel. The writer picks it up on the next event loop iteration, cleanly separating CDC processing from population dispatch.

```rust
// At the top of cache_query_cdc_invalidate(), before the policy branch:
if self.cache.cached_queries.get1(&fingerprint).is_some_and(|q| q.pinned) {
    let _ = self.query_tx.send(QueryCommand::Readmit { fingerprint });
    return Ok(());
}

// Existing FIFO/CLOCK branches follow...
```

The `Readmit` handler calls the existing `query_readmit()` path which:
1. Assigns a new generation
2. Sets state to `Loading`
3. Dispatches population work
4. On completion, state transitions to `Ready`

### Metrics

- `pgcache.cache.invalidations` — incremented by CDC handlers when queries are invalidated
- `pgcache.cache.readmissions` — incremented when `query_readmit()` executes

---

## Interaction Between Features

1. **Pinned queries are independent of the allowlist**: Pinning a query does NOT implicitly allowlist its tables. If a pinned query references tables not in the allowlist, the query will not be cached.

2. **Subsumption synergy**: A pinned `SELECT * FROM settings WHERE true` has empty constraints (no WHERE clause beyond `true`) and `has_limit = false`. Per the subsumption rules, any query against `settings` with WHERE constraints will be subsumed — the empty cached constraint set is a subset of any new constraint set.

3. **Admission gate bypass**: Pinned queries bypass the coordinator's admission gate (CLOCK threshold) because the coordinator sets `Loading` directly and sends `Register` with `AdmitAction::Admit` without going through the `Pending` state.

---

## Files Modified

| File | Change |
|------|--------|
| `src/settings.rs` | `pinned_queries: Option<Vec<String>>` on `SettingsToml` and `Settings`. CLI arg with semicolon-separated parsing via `pinned_queries_parse()`. |
| `src/cache/types.rs` | `pinned: bool` on `CachedQuery`. `PinnedQuery` struct. |
| `src/cache/mod.rs` | Re-export `PinnedQuery`. |
| `src/cache/messages.rs` | `pinned: bool` on `QueryCommand::Register`. `QueryCommand::Readmit { fingerprint }` variant. |
| `src/cache/writer/core.rs` | Store `query_tx` on `CacheWriter`. Handle `Readmit` command. Thread `pinned` through dispatch. Eviction protection with unbounded pinned bumps. Stale cleanup skip for pinned entries. |
| `src/cache/writer/query.rs` | Accept `pinned: bool` in `query_register()` and `cached_query_insert()`. `query_readmit()` made `pub(super)`. |
| `src/cache/writer/cdc.rs` | Send `Readmit` command for pinned queries before FIFO/CLOCK branch. |
| `src/proxy/server.rs` | `pinned_queries_validate()`. Pass through `cache_create()` and `cache_restart_attempt()`. |
| `src/cache/runtime.rs` | Accept `&[PinnedQuery]` in `cache_run()`. Call `pinned_queries_register()`. |
| `src/cache/query_cache.rs` | `pinned_queries_register()` method. Pass `pinned: false` in existing `query_register_send()`. |

---

## Testing

Integration tests in `tests/pinned_query_test.rs`:

1. **Cache hit on first request**: Single-table pinned query is pre-populated at startup. First client request is a cache hit with correct data.

2. **Auto-readmit after CDC**: JOIN query is pinned. CDC insert triggers invalidation. Metrics verify `cache_invalidations >= 1` and `cache_readmissions == 1`. After readmission, query returns updated data as a cache hit.

3. **Survives eviction**: Small cache size (200KB). Pinned query registered, then three non-pinned queries fill the cache triggering eviction. Pinned query remains a cache hit.

4. **Invalid SQL skipped**: Invalid pinned query SQL is logged and skipped. pgcache starts normally and serves regular queries.

Test infrastructure in `tests/util/mod.rs`:
- `TestContext::setup_pinned()` — takes pinned query string and a `before_start` closure that creates tables/data on origin before pgcache spawns
- `TestContext::setup_pinned_small_cache()` — same with configurable cache size for eviction testing

---

## Future Work

### Search Path for Pinned Queries
Pinned queries currently use `["public"]` as the default search path. Schema-qualified table names work, but unqualified tables resolve to `public` only. Revisit this to support configurable search paths or infer from origin defaults.

### Dynamic Config Reload
The current implementation supports startup-only pinning. Dynamic reload (SIGHUP, admin command) with `Pin`/`Unpin` commands and a `watch::Sender<Vec<PinnedQuery>>` channel will be added as a follow-up.
