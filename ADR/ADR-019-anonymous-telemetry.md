# ADR-019: Anonymous Telemetry

## Status
Proposed

## Context
pgcache is distributed as a Docker image and potentially bare-metal binary. We have no visibility into whether instances are actually running, how they're being used, or whether users are getting value from the cache. Docker Hub pull counts don't indicate active usage. We need lightweight telemetry to understand adoption, deployment patterns, and cache effectiveness.

The telemetry must be privacy-respecting: no personal data, no query text, no schema information. It should be opt-out (enabled by default) with clear disclosure and a simple disable mechanism. Volume is expected to be low initially.

## Decision
- **Opt-out telemetry** with `PGCACHE_TELEMETRY=off` environment variable to disable
- **Ephemeral instance ID**: random UUID generated in memory at startup; no persistence across restarts — impossible to build long-term profiles
- **Startup ping + 24h heartbeat**: fire-and-forget HTTPS POST, failures silently ignored
- **Coarse metrics only**: cache table count, registered query count, hit rate percentage, queries served (rolling 24h), uptime — no exact counts, no query text, no schema info
- **Cloudflare Worker backend** writing to a Notion database, with Cloudflare rate limiting to protect Notion API
- **Single INFO log line** at startup disclosing telemetry with disable instructions and docs link

## Rationale
- **Opt-out maximizes signal** (~80-90% participation vs ~10-20% opt-in), critical at low adoption volume
- **Ephemeral ID** eliminates all tracking concerns — each restart is a new identity; active instances estimated by counting distinct IDs within a heartbeat window
- **Cloudflare Worker + Notion** reuses existing infrastructure (waitlist worker pattern) with zero new services; backend can be swapped later without client changes
- **Fire-and-forget with silent failure** ensures telemetry never impacts proxy performance or reliability
- **Coarse metrics** (hit rate %, bucketed counts) reveal whether users get value without exposing sensitive data

## Consequences

### Positive
- Visibility into active pgcache instances and deployment modes (Docker vs bare-metal)
- Understanding of cache effectiveness across deployments (hit rates, query volumes)
- PostgreSQL version distribution helps prioritize compatibility work
- Backend is decoupled from client — can migrate from Notion to a real analytics store without shipping a new pgcache version

### Negative
- Opt-out telemetry requires careful disclosure to maintain trust; privacy policy and docs must be updated
- Notion as a backend won't scale beyond hundreds of active instances; will need replacement as adoption grows
- Ephemeral IDs mean we can't track instance lifecycle across restarts (acceptable tradeoff for privacy)

## Implementation Notes
Telemetry runs as a background tokio task spawned from the admin/metrics server thread, sharing the same single-threaded runtime. Uses `reqwest` for outbound HTTPS. Heartbeat interval is hardcoded at 24h with random jitter (0-60min) to avoid thundering herd. Metrics are read from the Prometheus handle and CacheStateView at each heartbeat.
