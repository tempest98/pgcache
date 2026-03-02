# Dynamic Publication Management for CDC

## Context

pgcache currently creates its CDC publication with `FOR ALL TABLES`, meaning the replication stream delivers events for every table in the origin database. The writer then filters events client-side using a shared `active_relations` HashSet. This wastes network bandwidth and parsing effort for tables that have no cached queries.

The goal is to dynamically manage the publication's table list so that PostgreSQL filters CDC events at the source. Tables are added to the publication when queries referencing them are registered, and removed when all queries referencing them are evicted or cleaned up.

## Design

### New state in CacheWriter (`cache/writer/core.rs`)

Add two fields to `CacheWriter`:
- `publication_name: String` — from settings, needed for ALTER PUBLICATION SQL
- `publication_oids: HashSet<u32>` — tracks what's currently in the publication, used to detect when ALTER PUBLICATION is actually needed

Add a `relations_dirty: bool` flag to avoid computing the active relations set on every CDC event. Only set when queries are actually removed (eviction/failure/stale cleanup).

### `active_relations_rebuild()` returns `bool` (`cache/writer/core.rs:250`)

Change the return type to `bool`. Compute the new set, compare with current shared set, update if different, return whether it changed.

### New method: `publication_update()` (`cache/writer/core.rs`)

Compares current active relation OIDs with `publication_oids`. If different:
- Non-empty new set: `ALTER PUBLICATION {name} SET TABLE schema.table, ...`
- Empty new set: `ALTER PUBLICATION {name} DROP TABLE schema.table, ...` (dropping the old tables from `publication_oids`)

Resolves OIDs to qualified names via `cache.tables` (table metadata is never removed, so lookup is always valid). Updates `publication_oids` on success. Executes on `db_origin` (already available in the writer).

### `replication_provision()` creates empty publication (`pg/cdc.rs:25`)

Change startup to always drop and recreate the publication as empty (no `FOR ALL TABLES`). Tables are added dynamically as queries register. The replication slot is kept as-is (create if not exists).

Old: `CREATE PUBLICATION {name} FOR ALL TABLES WITH (publish_via_partition_root = true)`
New: `CREATE PUBLICATION {name} WITH (publish_via_partition_root = true)`

### Call site changes

**`cache_query_evict()` (`cache/writer/cdc.rs:285`):**
- Already calls `self.active_relations_rebuild()`
- Replace with `self.relations_dirty = true`

**`stale_entries_cleanup()` (`cache/writer/core.rs:446`):**
- Already calls `self.active_relations_rebuild()` after removing stale invalidated entries
- Replace with `self.relations_dirty = true`

**`query_failed_cleanup()` (`cache/writer/query.rs:361`):**
- Replace `self.active_relations_rebuild()` with `self.relations_dirty = true`

**`query_register()` (`cache/writer/query.rs:238`):**
- Keep `active_relations_rebuild()` call (inside `cached_query_insert()`)
- Add `publication_update().await?` when rebuild returns true
- This happens BEFORE population dispatch — critical for ordering correctness

**`query_readmit()` (`cache/writer/query.rs:283`):**
- Already calls `active_relations_rebuild()`
- Add `publication_update().await?` when rebuild returns true, before population dispatch
- Needed because readmission can reactivate tables that were removed from the publication after full eviction

**`query_command_handle()` (`cache/writer/core.rs:164`):**
- After the match block (before `state_gauges_update`), check `relations_dirty` flag
- If dirty: clear flag, call `active_relations_rebuild()`, and if changed call `publication_update().await?`

**`cdc_command_handle()` (`cache/writer/core.rs:207`):**
- Same pattern: check `relations_dirty`, rebuild if needed, update publication if changed

### CLOCK policy and CDC invalidation

Under CLOCK policy, `cache_query_cdc_invalidate()` keeps the `cached_queries` entry with `invalidated = true` for fast readmission. The relation OIDs remain in `active_relations`, so the publication does **not** shrink on CDC invalidation — only on full eviction (`cache_query_evict`) or stale cleanup (`stale_entries_cleanup`). This is correct and conservative: keeping the table in the publication means CDC events continue flowing, which is needed if the query is readmitted before the publication would otherwise be updated.

### Client-side filter retained

The `is_relation_active()` check in `cache/cdc.rs` stays as a safety net. There's a window between `ALTER PUBLICATION` and the replication stream reflecting the change where events could arrive for removed tables, or not arrive for newly added tables (the latter is handled by the ordering constraint above).

## Files to modify

1. **`pgcache/src/pg/cdc.rs`** — `replication_provision()`: drop+recreate publication as empty
2. **`pgcache/src/cache/writer/core.rs`** — struct fields, `active_relations_rebuild()` return type, new `publication_update()`, dirty flag in `cache_query_evict()` and `stale_entries_cleanup()`, dirty flag checks in command handlers
3. **`pgcache/src/cache/writer/query.rs`** — `query_register()` and `query_readmit()` publication update, `query_failed_cleanup()` dirty flag
4. **`pgcache/src/cache/writer/cdc.rs`** — no changes needed (CDC invalidation under CLOCK keeps entries; eviction is in this file but dirty flag replaces the rebuild call)

## Verification

- `cargo check` — compile check
- `cargo clippy -- -D warnings` — lint
- `cargo test --lib` — unit tests
- `cargo test --test integration_basic --no-run` — compile-check integration tests (they need a running database)
