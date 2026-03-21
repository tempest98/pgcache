# Dynamic Runtime Configuration via HTTP Admin API

## Context

pgcache config is fully static today — parsed once from TOML + CLI at startup, passed as `&Settings` throughout. The goal is to make a subset of config adjustable at runtime via the HTTP admin API, with the TOML file always as source of truth. This enables IaC tools (Terraform, Ansible) to manage pgcache configuration without restarts.

The API model:
- `GET /config` — read current effective config (enables Terraform plan/diff)
- `PUT /config` — write partial config to TOML file + reload (single atomic call for IaC)
- `POST /config/reload` — re-read TOML file and apply dynamic changes (for file-based IaC)

## Dynamic vs Static Fields

**Dynamic** (changeable at runtime):
- `cache_size` — warm path (eviction only), stored in `Cache`
- `cache_policy` — hot path (every query dispatch), stored in `Cache` + `QueryCache`
- `admission_threshold` — hot path, stored in `Cache` + `QueryCache`
- `allowed_tables` — hot path, stored in `QueryCache` as parsed `Allowlist`
- `log_level` — startup only currently, needs `reload::Layer` for dynamic changes

**Static** (require restart):
- `origin`, `replication`, `cache` (DB connections)
- `cdc` (replication slot/publication)
- `listen`, `metrics` (bound sockets)
- `num_workers` (thread count)
- `tls_cert`, `tls_key`

**Deferred — pinned_queries**: More of a command than a config knob (requires SQL parsing, origin validation, cache registration). Better as a separate `/queries/pin` endpoint in a future phase.

## Key Design Decisions

### Lock-free config sharing via ArcSwap

Dynamic fields are read on every query dispatch (hot path). `arc_swap::ArcSwap<DynamicConfig>` gives:
- Lock-free reads via `.load()` (~1 atomic load, no CAS)
- Rare atomic swaps on config update via `.store()`
- Already the standard crate for this pattern in Rust

Both `QueryCache` (coordinator, per-worker clone) and `CacheWriter` (writer thread) hold a `ConfigHandle` wrapping `Arc<ArcSwap<DynamicConfig>>`. On each query dispatch or eviction check, they load the current config.

### Format-preserving TOML writes via toml_edit

`PUT /config` needs to update the TOML file without destroying comments, ordering, or formatting. `toml_edit` provides read-modify-write on a `DocumentMut` that preserves structure. This matters because IaC users keep config files in version control.

### Dynamic log level via tracing reload layer

Switch from static `EnvFilter` to `tracing_subscriber::reload::Layer`. Store the reload handle; on log_level change, swap the filter.

### Restart-required detection

`GET /config` reads both the in-memory running config and the TOML file. If any static field in the file differs from the running values, the response includes `restart_required: true`.

## Implementation Phases

### Phase 1: DynamicConfig type and ConfigHandle

**New file: `src/config.rs`**

```rust
/// Runtime-adjustable configuration fields.
/// Stored behind ArcSwap for lock-free reads on the hot path.
#[derive(Debug, Clone, Serialize)]
pub struct DynamicConfig {
    pub cache_size: Option<usize>,
    pub cache_policy: CachePolicy,
    pub admission_threshold: u32,
    pub allowed_tables: Option<Vec<String>>,  // raw strings (pre-parse on read)
    pub log_level: Option<String>,
}

/// Shared handle for reading/updating dynamic config. Cloneable, lock-free reads.
#[derive(Clone)]
pub struct ConfigHandle {
    inner: Arc<ArcSwap<DynamicConfig>>,
    config_path: Option<PathBuf>,
}
```

ConfigHandle methods:
- `config_load(&self) -> arc_swap::Guard<Arc<DynamicConfig>>` — hot path read
- `config_update(&self, new: DynamicConfig)` — swap in new config
- `config_path(&self) -> Option<&Path>`

**Also store the parsed `Allowlist` in a second ArcSwap** (or compute it on swap). Since `allowed_tables` needs parsing into `Vec<(Option<String>, String)>`, either:
- Parse on every load (cheap — just splitting on `.`)
- Parse once on update and store alongside raw strings

Recommendation: parse on update, store both raw and parsed in DynamicConfig.

**Files**: new `src/config.rs`, modify `src/lib.rs` to add module

### Phase 2: Track config file path

**File: `src/settings.rs`**

- Add `config_path: Option<PathBuf>` to `Settings`
- In `cli_args_parse()`, when `--config` is parsed (line ~404), store the path
- Thread it through `settings_build()` into the final `Settings`

### Phase 3: Plumb ConfigHandle into cache subsystem

**Files: `src/main.rs`, `src/proxy/server.rs`, `src/cache/runtime.rs`**

- Create `ConfigHandle` in `main.rs` after `Settings::from_args()`
- Pass to `proxy_run()` → `cache_create()` → `cache_run()` / `writer_run()`

**File: `src/cache/query_cache.rs`**

- Replace fields `cache_policy`, `admission_threshold`, `allowed_tables` with `config: ConfigHandle`
- In `query_dispatch()`: `let config = self.config.config_load();` then use `config.cache_policy`, etc.
- `query_allowlist_check()` uses the parsed allowlist from loaded config

**File: `src/cache/types.rs`**

- Replace `cache_size`, `cache_policy`, `admission_threshold` in `Cache` with `config: ConfigHandle`
- `Cache::new()` takes `ConfigHandle` instead of reading from `&Settings`

**File: `src/cache/writer/core.rs`**

- `eviction_run()`: load config from handle for `cache_size`, `cache_policy`

### Phase 4: HTTP Admin API endpoints

**File: `src/metrics.rs`** (extend existing admin server)

Add ConfigHandle to `admin_server_run()` parameters and route new endpoints:

```
GET  /config        → config_get_handle()
PUT  /config        → config_put_handle()
POST /config/reload → config_reload_handle()
```

**`GET /config`** response shape:
```json
{
  "dynamic": {
    "cache_size": 1000000,
    "cache_policy": "clock",
    "admission_threshold": 2,
    "allowed_tables": ["public.users"],
    "log_level": "info"
  },
  "restart_required": false
}
```

If config_path is available, also reads file and compares static fields to detect drift.

**`PUT /config`** accepts partial JSON:
```json
{
  "cache_size": 2000000,
  "admission_threshold": 3
}
```

Flow: validate → write to TOML file (toml_edit) → re-read file → extract dynamic fields → swap into ArcSwap → return new effective config.

**`POST /config/reload`** flow: read TOML file → extract dynamic fields → swap → return new config.

### Phase 5: TOML file read-modify-write

**File: `src/config.rs`** (add functions)

- `config_file_read(path: &Path) -> ConfigResult<SettingsToml>` — read + parse TOML
- `config_file_dynamic_update(path: &Path, changes: &DynamicConfigPatch) -> ConfigResult<DynamicConfig>` — toml_edit read-modify-write, returns new effective dynamic config
- `config_file_dynamic_extract(toml: &SettingsToml) -> DynamicConfig` — extract dynamic fields from parsed TOML

The patch type:
```rust
#[derive(Deserialize)]
pub struct DynamicConfigPatch {
    pub cache_size: Option<Option<usize>>,      // None = don't change, Some(None) = unset
    pub cache_policy: Option<CachePolicy>,
    pub admission_threshold: Option<u32>,
    pub allowed_tables: Option<Option<Vec<String>>>,
    pub log_level: Option<Option<String>>,
}
```

### Phase 6: Dynamic log level

**File: `src/main.rs`**

- Switch from `tracing_subscriber::fmt().with_env_filter(filter)` to using `reload::Layer`
- Store `reload::Handle` — pass to ConfigHandle or as a separate handle to admin server

**File: `src/config.rs`** or `src/metrics.rs`

- On config update, if log_level changed, call `reload_handle.reload(new_filter)`

## New Dependencies

```toml
arc-swap = "1"       # Lock-free atomic pointer swap
toml_edit = "0.22"   # Format-preserving TOML editing
```

`serde::Serialize` derive needed on: `DynamicConfig`, `CachePolicy`, `SslMode` (for GET /config response).

## Files Modified (summary)

| File | Changes |
|------|---------|
| `Cargo.toml` | Add arc-swap, toml_edit |
| `src/config.rs` (new) | DynamicConfig, ConfigHandle, TOML r/w, patch type |
| `src/lib.rs` | Add `pub mod config;` |
| `src/settings.rs` | Add config_path field, Serialize on CachePolicy |
| `src/main.rs` | Create ConfigHandle, reload::Layer for tracing |
| `src/metrics.rs` | Add /config endpoints, accept ConfigHandle |
| `src/proxy/server.rs` | Pass ConfigHandle to cache and admin server |
| `src/cache/runtime.rs` | Pass ConfigHandle to writer, coordinator, workers |
| `src/cache/types.rs` | Cache uses ConfigHandle instead of owned fields |
| `src/cache/query_cache.rs` | QueryCache uses ConfigHandle instead of owned fields |
| `src/cache/writer/core.rs` | Eviction reads from ConfigHandle |

## Verification

1. **Unit tests**: Test DynamicConfig swap, TOML read-modify-write preserving comments
2. **Integration test**: Start pgcache → `GET /config` → `PUT /config` with changed cache_size → `GET /config` confirms change → verify TOML file updated → verify cache eviction uses new size
3. **Restart test**: Change config via API → restart pgcache → verify config persisted from file
4. **Static field detection**: Manually edit origin.host in TOML → `GET /config` shows `restart_required: true`
5. **Log level**: `PUT /config` with `log_level: "debug"` → verify debug logs appear

## Open Questions

1. **Auth on admin API?** Production deployments will want it. Could be a follow-up phase — start with bind-address-only security (listen on localhost/internal network).
2. **Config versioning?** A generation counter on GET /config could help IaC tools detect concurrent changes. Low cost to add.
3. **Pinned queries**: Separate `/queries/pin` endpoint or part of config? Leaning toward separate endpoint given the validation complexity.
