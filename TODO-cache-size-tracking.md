# Cache Size Tracking and Eviction

## Completed

- [x] Add `cached_bytes: usize` field to `CachedQuery` in `types.rs`
- [x] Initialize `cached_bytes` to 0 in `query_register()`
- [x] Modify `query_cache_results()` to count and return byte count
- [x] Accumulate bytes in caching loop and store in `CachedQuery`

## TODO

### Per-Query Metric Reporting
- [ ] Add `cached_bytes` to metrics output for visibility into per-query cache sizes
- [ ] Consider adding query fingerprint or identifier to metrics for correlation

### Total Cache Size Tracking
- [ ] Add `total_cached_bytes: usize` to `Cache` struct
- [ ] Increment total when query caching completes
- [ ] Decrement total when query is invalidated/evicted

### LRU Tracking
- [ ] Add `last_accessed: Instant` to `CachedQuery`
- [ ] Update `last_accessed` on cache hits in worker
- [ ] Consider `access_count: u64` for LFU hybrid approach

### Eviction Logic
- [ ] Add `max_cache_bytes: usize` configuration setting
- [ ] Implement `cache_evict_check()` - called after caching a query
- [ ] Implement `cache_evict_lru()` - select and evict least recently used query
- [ ] Handle shared tables: when evicting a query, check if other queries reference same tables
  - If no other queries reference table, drop the cache table
  - If other queries exist, only remove query metadata (data remains for other queries)

### Configuration
- [ ] Add `max_cache_bytes` to settings
- [ ] Consider high/low watermarks for hysteresis (evict down to low when hitting high)

## Design Notes

### Size Estimation
Current approach counts raw value bytes from `row.get(idx)`. This:
- Accurately reflects data size
- Does not account for PostgreSQL storage overhead (indexes, page alignment)
- Will overcount when multiple queries cache data from the same table

### Eviction Granularity
Eviction is at the query level (by fingerprint). Due to shared cache tables:
- Multiple queries can reference the same origin table
- Rows are upserted into shared cache tables
- Evicting a query removes metadata but may not free disk space if other queries use the table
- Disk space is only reclaimed when the last query referencing a table is evicted

### Future: In-Memory Storage
The current PostgreSQL-backed cache will eventually use in-memory storage. The byte counting approach should remain valid as it measures data size rather than PostgreSQL-specific overhead.
