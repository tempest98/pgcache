# ADR-020: Table Allowlist

## Status
Accepted

## Context
pgcache caches any query that passes cacheability analysis (`CacheableQuery::try_new`). In some deployments, the user may only want to cache a specific subset of tables that are logically related.

## Decision
Add an optional table allowlist (`allowed_tables`) that restricts caching to queries whose tables are all present in the list. When configured, only queries where **every** referenced table appears in the allowlist are admitted to the cache. Queries referencing any non-allowlisted table are forwarded directly to origin.

### Configuration

```toml
# Only cache queries referencing these tables.
# Supports both unqualified ("orders") and schema-qualified ("audit.orders") names.
# If omitted or empty, all tables are cacheable (current behavior).
allowed_tables = ["settings", "products", "inventory.categories"]
```

Also available as `--allowed_tables TABLE1,TABLE2,...` on the CLI. When omitted or empty, all tables are cacheable (backward compatible default).

### Schema Qualification

Each allowlist entry is parsed into `(Option<schema>, table)`:
- `"orders"` → `(None, "orders")` — matches `orders` in **any** schema
- `"audit.orders"` → `(Some("audit"), "orders")` — matches only `audit.orders`

Matching is case-insensitive. Both the allowlist entries and query table references are lowercased before comparison.

### Enforcement Point

The check runs at the top of `QueryCache::query_dispatch()` in `query_cache.rs`, before any state machine logic. This keeps the allowlist concern in the cache coordinator where admission decisions are already made, avoids threading allowlist state through proxy code, and prevents non-allowlisted queries from creating entries in `CacheStateView`.

Table references are extracted via `QueryExpr::nodes::<TableNode>()`, which traverses the full AST including FROM clause tables, JOINs, CTE bodies, and subqueries in both WHERE clauses and SELECT lists. A query like `SELECT * FROM allowlisted WHERE EXISTS (SELECT 1 FROM non_allowlisted)` is correctly rejected.

### Admission, Not Retention

The allowlist gates admission only. Queries already cached when the allowlist is changed remain in the cache until naturally evicted or invalidated by CDC. This simplifies the implementation and avoids disrupting active cache entries.

## Rationale
- **Coordinator-level check** is the simplest place to enforce the allowlist — `query_dispatch()` is the single entry point for all cacheable queries, so one check covers all paths
- **All-tables-must-match** semantics prevent partial caching of join queries where one side shouldn't be cached
- **Case-insensitive matching** with optional schema qualification handles the most common PostgreSQL naming patterns without requiring users to know the exact schema for every table
- **Backward compatible default** (no allowlist = all tables cacheable) means existing deployments are unaffected

## Consequences

### Positive
- Operators can precisely control which tables consume cache resources
- Reduces cache churn and population overhead for tables that don't benefit from caching
- Simple mental model: list the tables you want cached, everything else goes to origin

### Negative
- Users must manually maintain the allowlist as their caching needs evolve
- No wildcard or pattern matching — each table must be listed explicitly
- Runtime updates are not yet implemented; changing the allowlist currently requires a restart

## Implementation Notes
- Config: `allowed_tables: Option<Vec<String>>` in `Settings` / `SettingsToml` (`src/settings.rs`)
- Allowlist parsing and checking: `allowlist_parse()`, `query_allowlist_check()` in `src/cache/query_cache.rs`
- Metric: `pgcache.queries.allowlist_skipped` counter incremented for each rejected query (`src/metrics.rs`)
- Startup log at `info` level reports the configured allowlist or "all tables cacheable"
