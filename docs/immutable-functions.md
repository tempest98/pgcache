# Plan: Support Immutable Functions in WHERE/FROM Clauses

## Context

All functions in WHERE and FROM (join condition) clauses are currently rejected by the proxy-side cacheability check (`CacheabilityError::UnsupportedWhereClause`). This prevents caching queries like `WHERE lower(name) = 'foo'` even though `lower` is an immutable function — same inputs always produce the same output, making caching safe.

Functions in SELECT lists are already allowed (re-evaluated against cached rows at serve time). This plan extends support to immutable functions in WHERE and FROM clauses, gated by a `pg_proc` volatility lookup. Stable and volatile functions remain rejected for now.

## Architecture: Shared Function Volatility Map (Proxy-Side)

The volatility map is loaded, stored, and checked entirely on the proxy side:

1. **`proxy_run()` loads function volatilities from `pg_proc` at startup** — connects to origin temporarily, queries the catalog, builds an `Arc<HashMap<String, FunctionVolatility>>`
2. **Passed through `WorkerResources` → `connection_run` → `ConnectionState`** — follows existing pattern for `cache_sender` and `tls_acceptor`
3. **Proxy-side cacheability check consults the map** — functions gated by volatility, not by position in the query tree

The cache writer has no involvement. This keeps the volatility concern cleanly within the proxy domain.

**Why `Arc<HashMap>` (not `Arc<RwLock>` or `DashMap`):** Function volatilities don't change at runtime. Building the map once at startup and sharing it immutably is simplest — no locks, no synchronization. Unknown functions (created after startup) are conservatively treated as volatile; a pgcache restart picks them up.

## Implementation Steps

### Step 1: Add `FunctionVolatility` type and pg_proc query

**File:** `src/catalog.rs`

Add:
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionVolatility {
    Immutable,
    Stable,
    Volatile,
}
```

Add a function to query pg_proc and build the volatility map:
```rust
pub async fn function_volatility_map_load(
    client: &Client,
) -> Result<HashMap<String, FunctionVolatility>, Error> {
    // Query pg_proc for all functions, grouped by name,
    // taking worst-case volatility across overloads
}
```

SQL:
```sql
SELECT p.proname,
       MAX(CASE p.provolatile
           WHEN 'v' THEN 2
           WHEN 's' THEN 1
           ELSE 0
       END) AS worst_volatility
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE p.prokind NOT IN ('a', 'w')
GROUP BY p.proname
```

The `prokind` filter excludes aggregate (`'a'`) and window (`'w'`) functions — they can't appear as scalar functions in WHERE clauses, and including them could falsely escalate volatility for scalar functions that share a name.

Keyed by lowercase unqualified function name. Worst-case across all overloads and all schemas (conservative — if any overload of `foo` in any schema is volatile, `foo` is treated as volatile everywhere).

### Step 2: Load at proxy startup and thread to connections

**File:** `src/proxy/server.rs`

`proxy_run()` is synchronous — it creates a tokio runtime and uses `rt.block_on()`. The volatility load must happen inside the `block_on` async block, after `replication_provision` and before `cache_create` (around line 271–273):

1. Connect to origin via `pg::connect(&settings.origin, "volatility-load")`
2. Call `function_volatility_map_load(&client)`
3. Wrap in `Arc<HashMap<String, FunctionVolatility>>`
4. The client drops here (temporary connection, not kept open)
5. Log the map size: `info!("loaded {} function volatilities", map.len())`

Add the map to `WorkerResources`:
```rust
struct WorkerResources {
    cache_sender: CacheSender,
    tls_acceptor: Option<Arc<tls::TlsAcceptor>>,
    func_volatility: Arc<HashMap<String, FunctionVolatility>>,  // new
}
```

**File:** `src/proxy/connection.rs`

The full threading chain through connection code:

1. **`worker_create()`** — already passes `resources.cache_sender` and `resources.tls_acceptor` as individual parameters to `connection_run()`. Add `resources.func_volatility` as a new parameter.

2. **`connection_run()`** (line 974) — add `func_volatility: Arc<HashMap<String, FunctionVolatility>>` parameter. Clone into the `spawn_local` closure and pass to `handle_connection()`.

3. **`handle_connection()`** (line 820) — add `func_volatility: Arc<HashMap<String, FunctionVolatility>>` parameter. Pass to `ConnectionState::new()`.

4. **`ConnectionState`** — add field and accept in constructor:
```rust
func_volatility: Arc<HashMap<String, FunctionVolatility>>,
```

The map is needed on `ConnectionState` for two code paths:
- `handle_client_message()` calls `handle_query()` (simple protocol)
- `handle_parse_message()` calls `CacheableQuery::try_new()` directly (extended protocol)

### Step 3: Modify cacheability check to accept immutable functions

**File:** `src/cache/query.rs`

1. **Change `CacheableQuery` construction** from `TryFrom<&QueryExpr>` to a named constructor:
   ```rust
   impl CacheableQuery {
       pub fn try_new(
           query: &QueryExpr,
           func_volatility: &HashMap<String, FunctionVolatility>,
       ) -> Result<Self, CacheabilityError> { ... }
   }
   ```

2. **Thread the volatility map** through the private `is_cacheable_*` functions. All are internal to this module, so the change is localized. Affected functions (13 total):
   - `is_cacheable_body(body, fv)`
   - `is_cacheable_select(node, fv)`
   - `is_cacheable_set_op(op, fv)`
   - `is_supported_from(node, ctx, fv)`
   - `is_supported_join(join, ctx, fv)`
   - `is_supported_table_source(source, ctx, fv)`
   - `is_cacheable_table_subquery(sub, ctx, fv)`
   - `is_cacheable_cte_ref(cte_ref, ctx, fv)`
   - `is_cacheable_where(node, ctx, fv)`
   - `is_cacheable_expr(expr, ctx, fv)`
   - `is_cacheable_subquery_inner(query, fv)`
   - `is_cacheable_select_list(node, ctx, fv)`
   - `is_cacheable_column_expr(expr, ctx, fv)`

   **Not affected:** `join_condition_is_valid()` — validates structural patterns only (equalities, AND of equalities) and doesn't inspect leaf expressions, so it needs no volatility context.

3. **Change the `WhereExpr::Function` arm** in `is_cacheable_expr`:
   ```rust
   WhereExpr::Function { name, args } => {
       let is_immutable = matches!(
           fv.get(name.to_lowercase().as_str()),
           Some(FunctionVolatility::Immutable)
       );
       if is_immutable || matches!(ctx, ExprContext::SelectList) {
           args.iter().try_for_each(|arg| is_cacheable_expr(arg, ctx, fv))
       } else {
           Err(CacheabilityError::UnsupportedWhereClause)
       }
   }
   ```

   Logic: immutable functions are allowed in any context. Non-immutable (stable, volatile, unknown) functions are only allowed in `SelectList` context (preserving existing behavior).

4. **Add a new `CacheabilityError` variant** for diagnostics:
   ```rust
   NonImmutableFunction,
   ```
   This distinguishes "unsupported WHERE clause syntax" from "function exists but isn't immutable" — helps users understand what they can fix. Use this variant in the `WhereExpr::Function` arm instead of `UnsupportedWhereClause` when the function is found in the map but isn't immutable.

### Step 4: Update callers

**File:** `src/proxy/query.rs`

`handle_query()` — add `func_volatility: &HashMap<String, FunctionVolatility>` parameter. Change `CacheableQuery::try_from(&query)` to `CacheableQuery::try_new(&query, func_volatility)`.

**File:** `src/proxy/connection.rs`

Two call sites need updating:

1. **`handle_client_message()`** (simple protocol) — calls `handle_query()` at line 185. Pass `&self.func_volatility` as the new parameter.

2. **`handle_parse_message()`** (extended protocol) — calls `CacheableQuery::try_from(&query)` directly at line 444. Change to `CacheableQuery::try_new(&query, &self.func_volatility)`.

### Step 5: Update tests

**File:** `src/cache/query.rs` (unit tests)

The existing unit tests use `CacheableQuery::try_from`. Update to `CacheableQuery::try_new` with a test volatility map.

Add a test helper that builds a map with common functions:
- Immutable: `lower`, `upper`, `length`, `abs`, `concat`, `trim`
- Stable: `now`, `current_timestamp`
- Volatile: `random`

Note: `coalesce` is a SQL special form parsed as `CoalesceExpr`, not `FuncCall` — it never reaches `WhereExpr::Function` and doesn't belong in the volatility map.

New test cases:
- `WHERE lower(name) = 'foo'` → cacheable (immutable)
- `WHERE now() > created_at` → non-cacheable (stable, rejected)
- `WHERE unknown_func(col) = 1` → non-cacheable (unknown → conservative rejection)
- `WHERE lower(upper(name)) = 'foo'` → cacheable (nested immutable)
- `SELECT lower(name) FROM t WHERE lower(name) = 'foo'` → cacheable
- Join condition with immutable function → cacheable

## Key Files Modified

| File | Change |
|------|--------|
| `src/catalog.rs` | Add `FunctionVolatility`, `function_volatility_map_load()` |
| `src/proxy/server.rs` | Load map inside `block_on` async block, add to `WorkerResources` |
| `src/proxy/connection.rs` | Thread map through `connection_run()` → `handle_connection()` → `ConnectionState`; update `handle_client_message()` and `handle_parse_message()` call sites |
| `src/proxy/query.rs` | Accept map parameter, pass to `CacheableQuery::try_new()` |
| `src/cache/query.rs` | `CacheableQuery::try_new()`, `NonImmutableFunction` error variant, volatility-gated function check across 13 internal functions |

## Design Decisions

**Unqualified function names:** Schema is already stripped during parsing (`func_call_to_where_expr` uses `.next_back()`). The volatility map uses unqualified names with worst-case across schemas. Conservative — if `public.foo` is immutable but `other.foo` is volatile, `foo` is treated as volatile everywhere. Acceptable tradeoff for simplicity.

**Function name case:** `func_call_to_where_expr` extracts function names from the pg_query protobuf `String.sval`, which preserves whatever case the user wrote (e.g., `LOWER`, `Lower`, `lower`). PostgreSQL normalizes unquoted identifiers to lowercase, but pg_query's parser preserves the original. The volatility lookup uses `.to_lowercase()` to match against the map (which stores pg_proc's already-lowercase `proname`).

**Overload handling:** Same-name functions with different argument types may have different volatilities. We take worst-case across all overloads. Conservative and correct.

**Aggregate/window exclusion:** The pg_proc query filters out aggregate (`prokind = 'a'`) and window (`prokind = 'w'`) functions. They can't appear as scalar functions in WHERE clauses, and including them could falsely escalate volatility for scalar functions that share a name.

**Unknown functions default to volatile:** Functions not in the map (user-defined functions created after startup, extension functions) are rejected in WHERE/FROM. A pgcache restart picks them up.

**No CDC changes needed:** Immutable functions in WHERE clauses don't change invalidation semantics. If `WHERE lower(name) = 'foo'` is cached and `name` changes in the source table, CDC already invalidates based on the table reference.

**Temporary connection for loading:** The pg_proc query uses a short-lived connection to origin that's dropped after loading. This avoids keeping an extra long-lived connection and keeps the proxy's connection management simple.

## Verification

1. `cargo test --lib` — unit tests pass, including new cacheability tests
2. `cargo clippy -- -D warnings` — no warnings
3. `cargo check` — compiles clean
4. Manual verification: connect to pgcache, run `SELECT * FROM users WHERE lower(name) = 'foo'` and confirm it's cached (check logs/metrics for cache hit)

## Future Work (out of scope)

- **Stable function support:** Deferred until TTL/time-based invalidation is available
- **Argument-type-aware overload resolution:** Refine volatility lookup to consider argument types
- **Dynamic function discovery:** Lazy loading for functions created after startup
- **Schema-qualified lookup:** Preserve schema in AST for more precise volatility matching
