# Table Allowlist

## Context

pgcache currently caches any query that passes the cacheability analysis (`CacheableQuery::try_new`). For many deployments, only a subset of tables benefit from caching — small reference tables, configuration data, product catalogs, etc.

1. **Table allowlist**: Restrict caching to queries that only reference allowlisted tables. All other queries get forwarded to origin.

---

## Feature: Table Allowlist

### Configuration

Add to `Settings` and `SettingsToml`:

```toml
# Only cache queries referencing these tables.
# Supports both unqualified ("orders") and schema-qualified ("audit.orders") names.
# If omitted or empty, all tables are cacheable (current behavior).
cache_tables = ["settings", "products", "inventory.categories"]
```

- `cache_tables: Option<Vec<String>>` — `None` means all tables cacheable (backward compatible default)
- Names are matched case-insensitively
- Supports both unqualified (`"orders"`) and schema-qualified (`"audit.orders"`) entries
- ALL tables in a query must be allowlisted for the query to be cached. A join of a allowlisted and non-allowlisted table gets forwarded.

#### Schema Qualification

Each allowlist entry is parsed into `(Option<schema>, table)`:
- `"orders"` → `(None, "orders")` — matches `orders` in any schema
- `"audit.orders"` → `(Some("audit"), "orders")` — matches only `audit.orders`

When matching a `TableNode` from the query AST:
- If the allowlist entry has a schema → match both schema and table name
- If the allowlist entry has no schema → match table name only, regardless of the query's schema qualification

This means `cache_tables = ["orders"]` matches `SELECT * FROM orders`, `SELECT * FROM public.orders`, and `SELECT * FROM audit.orders`.

To restrict to a specific schema, use the qualified form: `cache_tables = ["public.orders"]`.

### Where to Check

**In `QueryCache::query_dispatch()` at `query_cache.rs`**, before any state machine logic. This is the coordinator, which already gates admission (Pending, Loading, etc.).

The check runs early in `query_dispatch()`:
1. Extract table references from `msg.cacheable_query.query.nodes::<TableNode>()`
2. Check each against the allowlist
3. If any table is not allowlisted, forward to origin immediately — no state entry created

This is cleaner than checking at the proxy level (avoids threading allowlist through proxy code) and avoids polluting the `CacheStateView` with entries for non-allowlisted queries.

#### Traversal Coverage

`QueryExpr::nodes::<TableNode>()` traverses the full AST including:
- Direct FROM clause tables and JOINs
- CTE bodies (via `CteRef` clones in `TableSource::CteRef`)
- Subqueries in WHERE clauses (via `WhereExpr::Subquery`)
- Subqueries in SELECT list expressions

This ensures that `SELECT * FROM allowlisted WHERE EXISTS (SELECT 1 FROM non_allowlisted)` is correctly rejected.

#### Allowlist Representation

The allowlist must support runtime updates without restart. Use `Arc<ArcSwap<...>>` so the coordinator reads the current allowlist on every `query_dispatch()` call without locking, and an external update path can swap in a new allowlist atomically.

```rust
/// Parsed allowlist entry: (optional schema, table name), both lowercased.
type AllowlistEntry = (Option<String>, String);

/// Parsed allowlist: None means all tables cacheable.
type Allowlist = Option<Vec<AllowlistEntry>>;

// In QueryCache
cache_tables: Arc<ArcSwap<Allowlist>>,

fn query_allowlist_check(&self, query: &QueryExpr) -> bool {
    let allowlist = self.cache_tables.load();
    let Some(entries) = allowlist.as_ref() else {
        return true; // no allowlist = all allowed
    };
    query.nodes::<TableNode>().all(|t| {
        let table_name = t.name.to_lowercase();
        let table_schema = t.schema.as_ref().map(|s| s.to_lowercase());
        entries.iter().any(|(ws, wt)| {
            wt == &table_name
                && match ws {
                    Some(ws) => table_schema.as_deref() == Some(ws.as_str()),
                    None => true, // unqualified allowlist entry matches any schema
                }
        })
    })
}
```

If not allowlisted: `reply_forward(msg.reply_tx, msg.pipeline, msg.data)` and return.

#### Runtime Updates

The `Arc<ArcSwap<Allowlist>>` is shared between the coordinator and a control path (e.g., admin SQL command, SIGHUP handler, or config file watcher — mechanism TBD). To update:

```rust
// Parse new entries from raw strings
let new_allowlist: Allowlist = Some(allowlist_entries_parse(&new_table_names));
cache_tables.store(Arc::new(new_allowlist));
```

`ArcSwap::load()` is wait-free on the read path, so `query_dispatch()` pays no synchronization cost. Writes (allowlist updates) are infrequent and don't block readers.

When the allowlist changes, already-cached queries for now-removed tables remain in the cache until naturally evicted or invalidated by CDC. This is acceptable — the allowlist gates *admission*, not retention. Queries for newly-added tables begin caching on their next occurrence.

### Files to Modify

| File | Change |
|------|--------|
| `src/settings.rs` | Add `cache_tables: Option<Vec<String>>` to `SettingsToml` and `Settings`. Add CLI arg `--cache_tables` (comma-separated). Wire through `settings_build_with_config` and `settings_build_cli_only`. |
| `src/cache/query_cache.rs` | Add `cache_tables: Arc<ArcSwap<Allowlist>>` field to `QueryCache`. Parse entries in constructor. Add `query_allowlist_check()` helper. Check at top of `query_dispatch()`. |
| `src/metrics.rs` | Add `QUERIES_ALLOWLIST_REJECTED` constant. |

### Metrics

- Counter: `pgcache.queries.allowlist_rejected` — queries forwarded due to table allowlist

### Logging

- Log at `info` level on startup listing allowlisted tables (or "all tables cacheable" if no allowlist configured), so operators can confirm their config.

---

## Verification

1. **Unqualified allowlist**: Configure `cache_tables = ["test_table"]`. Queries against `test_table` and `public.test_table` should be cached. Queries against other tables should forward.
2. **Qualified allowlist**: Configure `cache_tables = ["public.test_table"]`. Queries against `public.test_table` should be cached. Queries against `other_schema.test_table` should forward.
3. **Mixed join**: Query joining a allowlisted and non-allowlisted table should forward.
4. **CTE coverage**: Query with a CTE referencing a non-allowlisted table should forward.
5. **Subquery coverage**: Query with `WHERE EXISTS (SELECT 1 FROM non_allowlisted)` should forward.
6. **Metric**: Check `pgcache.queries.allowlist_rejected` counter increments for rejected queries.
7. **Runtime update**: Start with `cache_tables = ["table_a"]`. Trigger a allowlist update adding `"table_b"`. Queries against `table_b` should begin caching without restart. Previously-cached `table_a` queries remain cached.
