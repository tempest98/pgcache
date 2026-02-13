# Dynamic Publication Management for CDC

## Context

pgcache currently creates its CDC publication with `FOR ALL TABLES`, meaning the replication stream delivers events for every table in the origin database. The writer then filters events client-side using a shared `active_relations` HashSet. This wastes network bandwidth and parsing effort for tables that have no cached queries.

The goal is to dynamically manage the publication's table list so that PostgreSQL filters CDC events at the source. Tables are added to the publication when queries referencing them are registered, and removed when all queries referencing them are invalidated.

## Design

### New state in CacheWriter (`cache/writer/core.rs`)

Add two fields to `CacheWriter`:
- `publication_name: String` — from settings, needed for ALTER PUBLICATION SQL
- `publication_oids: HashSet<u32>` — tracks what's currently in the publication, used to detect when ALTER PUBLICATION is actually needed

Add a `relations_dirty: bool` flag to avoid computing the active relations set on every CDC event. Only set when queries are actually removed (invalidation/failure).

### `active_relations_rebuild()` returns `bool` (`cache/writer/core.rs:242`)

Change the return type to `bool`. Compute the new set, compare with current shared set, update if different, return whether it changed.

### New method: `publication_update()` (`cache/writer/core.rs`)

Compares current active relation OIDs with `publication_oids`. If different:
- Non-empty new set: `ALTER PUBLICATION {name} SET TABLE schema.table, ...`
- Empty new set: `ALTER PUBLICATION {name} DROP TABLE schema.table, ...` (dropping the old tables)

Resolves OIDs to qualified names via `cache.tables` (table metadata is never removed, so lookup is always valid). Updates `publication_oids` on success. Executes on `db_origin` (already available in the writer).

### `replication_provision()` creates empty publication (`pg/cdc.rs:25`)

Change startup to always drop and recreate the publication as empty (no `FOR ALL TABLES`). Tables are added dynamically as queries register. The replication slot is kept as-is (create if not exists).

Old: `CREATE PUBLICATION {name} FOR ALL TABLES WITH (publish_via_partition_root = true)`
New: `CREATE PUBLICATION {name} WITH (publish_via_partition_root = true)`

### Call site changes

**`cache_query_invalidate()` (`cache/writer/cdc.rs:248`):**
- Remove `self.active_relations_rebuild()`
- Set `self.relations_dirty = true`

**`query_failed_cleanup()` (`cache/writer/query.rs:240`):**
- Remove `self.active_relations_rebuild()`
- Set `self.relations_dirty = true`

**`query_register()` (`cache/writer/query.rs:157`):**
- Keep `active_relations_rebuild()` call
- Add `publication_update().await?` when rebuild returns true
- This happens BEFORE population dispatch — critical for ordering correctness

**`query_command_handle()` (`cache/writer/core.rs:152`):**
- After the match block, check `relations_dirty` flag
- If dirty: clear flag, call `active_relations_rebuild()`, and if changed call `publication_update().await?`

**`cdc_command_handle()` (`cache/writer/core.rs:199`):**
- Same pattern: check `relations_dirty`, rebuild if needed, update publication if changed

### Client-side filter retained

The `is_relation_active()` check in `cache/cdc.rs` stays as a safety net. There's a window between `ALTER PUBLICATION` and the replication stream reflecting the change where events could arrive for removed tables, or not arrive for newly added tables (the latter is handled by the ordering constraint above).

## Files to modify

1. **`pgcache/src/pg/cdc.rs`** — `replication_provision()`: drop+recreate publication as empty
2. **`pgcache/src/cache/writer/core.rs`** — struct fields, `active_relations_rebuild()` return type, new `publication_update()`, dirty flag checks in command handlers
3. **`pgcache/src/cache/writer/query.rs`** — `query_register()` publication update, `query_failed_cleanup()` dirty flag
4. **`pgcache/src/cache/writer/cdc.rs`** — `cache_query_invalidate()` dirty flag

## Verification

- `cargo check` — compile check
- `cargo clippy -- -D warnings` — lint
- `cargo test --lib` — unit tests
- `cargo test --test integration_basic --no-run` — compile-check integration tests (they need a running database)
