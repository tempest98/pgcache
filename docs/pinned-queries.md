# Pinned Queries

## Context

pgcache currently caches queries only when they have been seen in the request flow. Users may know ahead of time that some queries are useful to have permanently cached.

**Pinned queries**: Pin specific queries at startup, and make sure the queries are reloaded even if updates invalidate them. Supports dynamic config changes — pinned queries can be added or removed at runtime without restarting.

---

## Pinned Queries

### Configuration

```toml
# Queries to pre-cache at startup.
pinned_queries = ["select * from settings", "select * from products where not deleted"]
```

- `pinned_queries: Option<Vec<String>>` on `Settings`
- Tables in pinned queries are NOT automatically allowlisted — they are independent of the allowlist
- At startup, if a table in a pinned query is not in the allowlist, emit a `warn!` log: the pinned query will not be cached.

### Architecture: Proxy Validates, Coordinator Registers, Writer Executes

Each layer does what it's already responsible for:

1. **Proxy** (`proxy_run`): Parses pinned query SQL, checks cacheability via `CacheableQuery::try_new(func_volatility)`, validates allowlist, computes fingerprint. Produces a `Vec<PinnedQuery>` of pre-validated entries. Emits warnings for invalid/uncacheable/non-allowlisted queries at this stage.

2. **Coordinator** (`cache_run`/`QueryCache`): Receives validated `PinnedQuery` entries. Sets `Loading` state in `CacheStateView`, sends `QueryCommand::Register { pinned: true }` to writer. No parsing, no `func_volatility` needed.

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

Defined in `src/cache/mod.rs` (or `src/cache/types.rs`). Used at the boundary between proxy and cache.

### Proxy-Side Validation

In `proxy_run()`, after loading `func_volatility` but before `cache_create()`:

```rust
// In proxy_run(), after function_volatility_load():
let pinned = pinned_queries_validate(&settings, &func_volatility);
let (cache_handle, cache_tx) = cache_create(scope, settings, pinned)?;
```

`pinned_queries_validate()` lives in `src/proxy/server.rs` and iterates `settings.pinned_queries`:
1. Parse via `pg_query::parse()` → `query_expr_convert()`. On error, emit `warn!("pinned query is not parseable — skipping: {sql}")` and continue.
2. Check cacheability via `CacheableQuery::try_new(&query, &func_volatility)`. On error, emit `warn!("pinned query is not cacheable — skipping: {sql}")` and continue.
3. Check if all tables in query are in the allowlist (if allowlist is configured). If not, emit `warn!("pinned query table '{table}' is not in cache_tables allowlist — query will not be cached: {sql}")` and continue.
4. Compute fingerprint via `query_expr_fingerprint()`
5. Collect into `Vec<PinnedQuery>`

### Startup Registration

`cache_create()` passes validated pinned queries to `cache_run()`. In `cache_run()`, after creating `QueryCache` but before entering the event loop:

```rust
// In cache_run(), after QueryCache::new():
qcache.pinned_queries_register(&pinned)?;
// Then enter select! loop
```

`pinned_queries_register()` on `QueryCache` iterates the pre-validated entries and for each:
1. Set `Loading` state in `CacheStateView`
2. Send `QueryCommand::Register { fingerprint, cacheable_query, search_path: vec!["public"], pinned: true }` to writer
3. Track fingerprint in `pinned_fingerprints: HashSet<u64>`

The search path defaults to `["public"]`. Schema-qualified table names in the SQL string (e.g. `select * from analytics.events`) are supported since the schema is parsed from the query AST, but unqualified tables will resolve against `public` only. This is a known limitation.

### Dynamic Config Changes

Pinned queries support runtime updates via config reload (triggered by SIGHUP or admin command).

**Channel**: A `tokio::sync::watch::Sender<Vec<PinnedQuery>>` is created in `proxy_run()`. The reload signal handler re-reads config, calls `pinned_queries_validate()` with the current `func_volatility`, and sends the validated set through the watch channel.

**Coordinator event loop**: Add a branch to the `select!` loop in `cache_run()` that watches for config changes:

```rust
pinned_rx.changed() => {
    let new_pinned = pinned_rx.borrow_and_update().clone();
    qcache.pinned_queries_update(&new_pinned)?;
}
```

**`pinned_queries_update()`** on `QueryCache`:
1. Compute new fingerprint set from incoming `Vec<PinnedQuery>`
2. Compare against current `pinned_fingerprints`
3. **Added pins**: If fingerprint already exists in state_view (query is already cached), send `QueryCommand::Pin { fingerprint }`. Otherwise set `Loading` state and send `QueryCommand::Register { pinned: true }`.
4. **Removed pins**: Send `QueryCommand::Unpin { fingerprint }`. The query becomes a normal evictable entry — it is NOT immediately evicted, just loses its protection.
5. Update `pinned_fingerprints`

### QueryCommand Additions

```rust
enum QueryCommand {
    Register {
        fingerprint: u64,
        cacheable_query: Arc<CacheableQuery>,
        search_path: Vec<String>,
        started_at: Instant,
        pinned: bool,  // new — normal Register uses false
    },

    /// Mark an existing cached query as pinned (for dynamic config add of already-cached query)
    Pin { fingerprint: u64 },

    /// Remove pinned status from a cached query (for dynamic config removal)
    Unpin { fingerprint: u64 },

    // ... existing variants unchanged
}
```

### CachedQuery.pinned Flag

Add `pinned: bool` to `CachedQuery` (default `false`). Set to `true` when the writer receives a `Register { pinned: true }` or `Pin` command.

The writer needs to know which queries are pinned for:
- **Eviction protection**: Skip pinned queries during `eviction_run()`
- **CDC auto-readmit**: On invalidation, immediately re-populate instead of waiting for a client hit

### Eviction: Pinned = Referenced

Pinned queries are treated like referenced queries in the CLOCK eviction algorithm — they get a generation bump instead of being evicted. This applies in both CLOCK and FIFO modes:

**CLOCK mode**: Pinned queries are bumped just like referenced queries. The existing `MAX_BUMPS` counter applies — pinned bumps count toward the same limit, preventing infinite loops.

**FIFO mode**: FIFO normally evicts unconditionally. With pinning, FIFO checks the pinned flag before evicting and bumps pinned queries instead. This reuses the `cache_query_generation_bump()` mechanism already used by CLOCK.

**Exhaustion handling**: If the eviction loop exhausts `MAX_BUMPS`, pinned queries must never be evicted. After `MAX_BUMPS`, if the current candidate is pinned, `continue` to the next candidate. If all remaining candidates are pinned (tracked via a counter), break out of the eviction loop entirely — cache is over limit but nothing can be evicted.

```rust
// In eviction_run(), unified for both CLOCK and FIFO:
if query.pinned || (self.cache.cache_policy == CachePolicy::Clock && referenced) {
    if bumps < MAX_BUMPS {
        self.cache_query_generation_bump(fingerprint).await?;
        bumps += 1;
        continue;
    }
    // MAX_BUMPS exhausted — if pinned, skip this candidate (don't evict pinned queries)
    if query.pinned {
        pinned_skips += 1;
        if pinned_skips >= candidate_count {
            break; // all remaining candidates are pinned, nothing evictable
        }
        continue; // try next candidate
    }
}
// Evict
```

### CDC Auto-Readmit

In `cache_query_cdc_invalidate()`, the pinned check must happen **before** the FIFO/CLOCK branch, since FIFO's branch calls `cache_query_evict()` which would destroy the `CachedQuery` entry (and the `pinned` flag with it):

```rust
// At the top of cache_query_cdc_invalidate(), before the policy branch:
if query.pinned {
    // Auto-readmit: re-populate immediately instead of waiting for next hit
    return self.query_readmit(fingerprint, Instant::now()).await;
}

// Existing FIFO/CLOCK branches follow...
```

This reuses the existing `query_readmit()` path which:
1. Assigns a new generation
2. Sets state to `Loading`
3. Dispatches population work
4. On completion, state transitions to `Ready`

### Coordinator State

The coordinator tracks `pinned_fingerprints: HashSet<u64>` for diffing during dynamic config updates. It does NOT need a `pinned` flag in `CachedQueryView` — pinned queries appear as normal `Ready` entries in `CacheStateView`. The subsumption mechanism (from `predicate-subsumption.md`) handles the rest — any query against a pinned table whose `SELECT *` is Ready will be subsumed at registration time.

### Config Reload Ephemeral State

Pinned status is ephemeral — it exists only in the running process, derived from configuration. On restart, `cache_database_reset()` wipes the cache database and pinned queries are re-registered from config. On dynamic config reload, the coordinator diffs against its `pinned_fingerprints` set to determine adds/removes.

### Files to Modify

| File | Change |
|------|--------|
| `src/settings.rs` | Add `pinned_queries: Option<Vec<String>>` to `SettingsToml` and `Settings`. Add CLI arg. Wire through build functions. |
| `src/cache/types.rs` | Add `pinned: bool` to `CachedQuery` struct (default `false`). Add `PinnedQuery` struct. |
| `src/cache/messages.rs` | Add `pinned: bool` to `QueryCommand::Register`. Add `QueryCommand::Pin` and `QueryCommand::Unpin` variants. |
| `src/cache/query_cache.rs` | Add `pinned_fingerprints: HashSet<u64>` to `QueryCache`. Add `pinned_queries_register()` and `pinned_queries_update()` methods. Pass `pinned: false` in existing `query_register_send()`. |
| `src/cache/runtime.rs` | Accept `Vec<PinnedQuery>` and `watch::Receiver` in `cache_run()`. Call `qcache.pinned_queries_register()` before event loop. Add watch branch to select loop. |
| `src/cache/writer/core.rs` | Handle `QueryCommand::Pin` and `QueryCommand::Unpin` in event loop. Modify `eviction_run()` to skip pinned queries. |
| `src/cache/writer/cdc.rs` | In `cache_query_cdc_invalidate()`, check `pinned` before FIFO/CLOCK branch and auto-readmit. |
| `src/cache/writer/query.rs` | In `query_register()`, accept and pass `pinned: bool` to `cached_query_insert()`. In `cached_query_insert()`, set `pinned` on `CachedQuery`. |
| `src/proxy/server.rs` | Add `pinned_queries_validate()`. Call it in `proxy_run()` after loading `func_volatility`. Pass validated queries to `cache_create()`. Create watch channel for dynamic reload. |

---

## Interaction Between Features

1. **Pinned queries are independent of the allowlist**: Pinning a query does NOT implicitly allowlist its tables. If a pinned query references tables not in the allowlist, emit a startup warning and skip registration — the pinned query will not be cached.

2. **Subsumption synergy**: A pinned `SELECT * FROM settings` has empty constraints (no WHERE clause) and `has_limit = false`. Per the subsumption rules in `predicate-subsumption.md`, any query against `settings` with WHERE constraints will be subsumed — the empty cached constraint set is a subset of any new constraint set. The new query skips origin entirely.

3. **Admission gate bypass**: Pinned queries bypass the coordinator's admission gate (CLOCK threshold) because the coordinator sets `Loading` directly and sends `Register` without going through the `Pending` state.

---

## Verification

1. **Pinned queries**: Configure `pinned_queries = ["select * from settings"]`. On startup, verify `settings` enters Loading then Ready state. Send `SELECT * FROM settings WHERE id = 1` — should be served from cache (via subsumption if implemented, or normal cache hit if same query).

2. **Non-allowlisted pinned query**: Configure a pinned query referencing a table not in `cache_tables`. Verify startup warning is emitted and query is NOT registered.

3. **Uncacheable pinned query**: Configure a pinned query with a volatile function. Verify startup warning is emitted and query is skipped.

4. **CDC auto-readmit**: With a pinned query in Ready state, INSERT/UPDATE/DELETE a row in the origin table. Verify the pinned query transitions Loading → Ready (not Invalidated or evicted).

5. **Eviction protection**: Fill cache to capacity. Verify pinned queries are not evicted while non-pinned queries are.

6. **Dynamic pin**: With pgcache running, add a query to `pinned_queries` and trigger config reload. Verify the new query enters Loading → Ready.

7. **Dynamic unpin**: Remove a query from `pinned_queries` and trigger config reload. Verify the query loses eviction protection (can now be evicted under memory pressure).
