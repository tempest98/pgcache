# ADR-007: Member Dashboard and Metrics Architecture

## Status
Proposed

## Context

pgcache needs a member-facing dashboard that allows users to:
1. Create accounts and authenticate
2. Monitor their pgcache instances
3. View comprehensive metrics including cache hit rates, query performance, and cacheability analysis

This system should dogfood pgcache itself - the dashboard's database queries will flow through pgcache instances, validating the product while building it.

### Requirements

- **Authentication**: Secure member accounts with modern auth flows
- **Multi-region**: pgcache instances deployed globally, dashboard connects to nearest
- **Metrics**: Comprehensive observability data to build user trust
- **Simplicity**: Minimal dependencies, lightweight stack
- **Cost-effective**: Free tiers or low-cost hosting for initial deployment

## Decision

### Technology Stack

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Database | Neon | Serverless PostgreSQL, good for dogfooding raw SQL through pgcache |
| Auth | BetterAuth | Framework-agnostic, works with raw pg Pool, TypeScript-native |
| ORM | None | Raw SQL with parameterized queries for simplicity and pgcache compatibility |
| Framework | None | Vanilla Cloudflare Workers fetch handler |
| Frontend hosting | Cloudflare Pages | Existing site infrastructure |
| API hosting | Cloudflare Workers | Edge deployment, TCP socket support |
| Connection pooling | Hyperdrive | Pools connections to pgcache, caching disabled |
| pgcache hosting | Fly.io | Multi-region support, ~$3-5/mo per region |
| Metrics library | `metrics` crate | Rust-native, Prometheus-compatible exporters |

### Connection Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              User Request                                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Cloudflare Pages (Static)                            │
│                         Dashboard UI (Zola-built)                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ API calls
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Cloudflare Worker                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  Route by request.cf.continent:                                      │    │
│  │    NA → HYPERDRIVE_US                                                │    │
│  │    EU → HYPERDRIVE_EU                                                │    │
│  │    AS → HYPERDRIVE_AS                                                │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ TCP (pooled)
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Hyperdrive (Connection Pooling Only)                      │
│                         Caching disabled                                     │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ TCP
┌─────────────────────────────────────────────────────────────────────────────┐
│                          pgcache (per region)                                │
│                    Transparent query caching                                 │
│                    Metrics collection                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼ TCP
┌─────────────────────────────────────────────────────────────────────────────┐
│                               Neon                                           │
│                     Serverless PostgreSQL                                    │
│                     (Single region, primary)                                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Authentication

BetterAuth with raw PostgreSQL connection, instantiated per-request due to Workers environment constraints:

```typescript
// src/lib/auth.ts
import { Client } from "pg";
import { betterAuth } from "better-auth";

export const auth = (env: Env) => {
  return betterAuth({
    database: new Client({ connectionString: env.HYPERDRIVE.connectionString }),
    secret: env.AUTH_SECRET,
    baseURL: env.AUTH_URL,
  });
};
```

Worker configuration:

```toml
# wrangler.toml
name = "pgcache-dashboard"
compatibility_flags = ["nodejs_compat"]
compatibility_date = "2024-09-23"

[[hyperdrive]]
binding = "HYPERDRIVE_US"
id = "<us-hyperdrive-id>"

[[hyperdrive]]
binding = "HYPERDRIVE_EU"
id = "<eu-hyperdrive-id>"

[[hyperdrive]]
binding = "HYPERDRIVE_AS"
id = "<as-hyperdrive-id>"
```

### Multi-Region Routing

Workers select Hyperdrive binding based on request origin:

```typescript
function getHyperdrive(env: Env, cf: IncomingRequestCfProperties): Hyperdrive {
  switch (cf.continent) {
    case "EU": return env.HYPERDRIVE_EU;
    case "AS": return env.HYPERDRIVE_AS;
    default: return env.HYPERDRIVE_US;
  }
}
```

Hyperdrive instances are configured with caching disabled since pgcache handles caching:

```bash
npx wrangler hyperdrive create pgcache-us \
  --connection-string="postgres://user:pass@pgcache-us.fly.dev:5432/dashboard" \
  --caching-disabled true
```

### Metrics Architecture

#### Metrics Collection in pgcache

Adopt the `metrics` crate with `metrics-exporter-prometheus` for standardized metric recording:

```rust
use metrics::{counter, gauge, histogram};

// Latency histograms
histogram!("pgcache_query_duration_seconds", "source" => "cache").record(duration);
histogram!("pgcache_query_duration_seconds", "source" => "origin").record(duration);
histogram!("pgcache_cache_lookup_seconds").record(duration);

// Query classification counters
counter!("pgcache_queries_total", "status" => "cache_hit").increment(1);
counter!("pgcache_queries_total", "status" => "cache_miss").increment(1);
counter!("pgcache_queries_total", "status" => "uncacheable").increment(1);
counter!("pgcache_queries_total", "status" => "unsupported").increment(1);
counter!("pgcache_queries_total", "status" => "parse_error").increment(1);

// Throughput counters
counter!("pgcache_bytes_total", "direction" => "client_in").increment(bytes);
counter!("pgcache_bytes_total", "direction" => "client_out").increment(bytes);
counter!("pgcache_bytes_total", "direction" => "origin_in").increment(bytes);
counter!("pgcache_bytes_total", "direction" => "origin_out").increment(bytes);

// CDC event counters
counter!("pgcache_cdc_events_total", "type" => "insert").increment(1);
counter!("pgcache_cdc_events_total", "type" => "update").increment(1);
counter!("pgcache_cdc_events_total", "type" => "delete").increment(1);
counter!("pgcache_invalidations_total").increment(1);

// Current state gauges
gauge!("pgcache_connections_active").set(count);
gauge!("pgcache_cached_queries").set(count);
gauge!("pgcache_cache_size_bytes").set(bytes);
gauge!("pgcache_replication_lag_bytes").set(lag);
```

#### Per-Query Statistics

Track detailed per-query metrics for dashboard insights:

```rust
struct QueryStats {
    fingerprint: String,
    sql_template: String,           // Normalized query with $1, $2...
    tables: Vec<String>,            // Tables referenced
    first_seen: Timestamp,
    last_seen: Timestamp,
    execution_count: u64,
    cache_hits: u64,
    cache_misses: u64,
    avg_latency_cache_ms: f64,
    avg_latency_origin_ms: f64,
    avg_rows_returned: f64,
    cacheable: bool,
    uncacheable_reason: Option<UncacheableReason>,
}
```

#### Uncacheable Reason Categorization

Detailed categorization enables users to understand and improve query cacheability:

```rust
enum UncacheableReason {
    // User-fixable issues
    NonDeterministicFunction { function: String },  // NOW(), RANDOM(), CURRENT_TIMESTAMP
    VolatileFunction { function: String },          // User-defined VOLATILE functions
    MutableCTE,                                      // CTE containing INSERT/UPDATE/DELETE
    WritesData,                                      // DML in subquery

    // Structural limitations
    SystemCatalogQuery,                              // Queries against pg_* tables
    TempTable,                                       // References temporary tables
    UnloggedTable,                                   // Unlogged tables (no WAL/CDC)

    // Current implementation limits
    UnsupportedJoin { details: String },
    UnsupportedSubquery { details: String },
    ComplexExpression { details: String },

    // User configuration
    TableExcluded { table: String },                 // User excluded this table
    QueryPatternExcluded,                            // User excluded this pattern
}
```

#### Metrics Endpoints

Each pgcache instance exposes two HTTP endpoints:

| Endpoint | Format | Content |
|----------|--------|---------|
| `GET /metrics` | Prometheus text | Aggregate counters, gauges, histograms |
| `GET /metrics/queries` | JSON | Per-query statistics array |

#### Metrics Collection Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                      pgcache instance                           │
│  ┌─────────────┐    ┌──────────────┐    ┌───────────────────┐  │
│  │   metrics   │───▶│ prometheus   │───▶│ :9090/metrics     │  │
│  │   crate     │    │ exporter     │    │ (Prometheus fmt)  │  │
│  └─────────────┘    └──────────────┘    └───────────────────┘  │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────┐                        ┌───────────────────┐  │
│  │ QueryStats  │───────────────────────▶│ :9090/queries     │  │
│  │  HashMap    │                        │ (JSON)            │  │
│  └─────────────┘                        └───────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼ Pull (HTTP scrape every 30-60s)
┌─────────────────────────────────────────────────────────────────┐
│               Metrics Collector (Cloudflare Worker)             │
│  - Scheduled via Cron Trigger                                   │
│  - Scrapes each registered pgcache instance                     │
│  - Writes snapshots to Neon with instance_id + timestamp        │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         Neon (PostgreSQL)                       │
│                                                                 │
│  metrics_snapshots:                                             │
│    - instance_id, timestamp, metric_name, value                 │
│    - Partitioned by time for efficient retention                │
│                                                                 │
│  query_stats:                                                   │
│    - instance_id, fingerprint, stats (JSONB)                    │
│    - Upserted on each collection                                │
│                                                                 │
│  Retention: 1 week initially (cron job prunes older data)       │
└─────────────────────────────────────────────────────────────────┘
```

### Metrics Storage Schema

```sql
-- Aggregate metrics time-series
CREATE TABLE metrics_snapshots (
    id BIGSERIAL,
    instance_id UUID NOT NULL REFERENCES instances(id),
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Counters (cumulative)
    queries_total BIGINT,
    queries_cache_hit BIGINT,
    queries_cache_miss BIGINT,
    queries_uncacheable BIGINT,
    queries_unsupported BIGINT,
    queries_parse_error BIGINT,
    bytes_client_in BIGINT,
    bytes_client_out BIGINT,
    bytes_origin_in BIGINT,
    bytes_origin_out BIGINT,
    cdc_inserts BIGINT,
    cdc_updates BIGINT,
    cdc_deletes BIGINT,
    invalidations BIGINT,

    -- Gauges (point-in-time)
    connections_active INT,
    cached_queries INT,
    cache_size_bytes BIGINT,
    replication_lag_bytes BIGINT,

    -- Histogram summaries (p50, p90, p99)
    latency_cache_p50_ms FLOAT,
    latency_cache_p90_ms FLOAT,
    latency_cache_p99_ms FLOAT,
    latency_origin_p50_ms FLOAT,
    latency_origin_p90_ms FLOAT,
    latency_origin_p99_ms FLOAT,

    PRIMARY KEY (id)
);

CREATE INDEX idx_metrics_instance_time ON metrics_snapshots(instance_id, collected_at DESC);

-- Per-query statistics (latest snapshot per query)
CREATE TABLE query_stats (
    instance_id UUID NOT NULL REFERENCES instances(id),
    fingerprint TEXT NOT NULL,
    sql_template TEXT NOT NULL,
    tables TEXT[] NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    execution_count BIGINT NOT NULL,
    cache_hits BIGINT NOT NULL,
    cache_misses BIGINT NOT NULL,
    avg_latency_cache_ms FLOAT,
    avg_latency_origin_ms FLOAT,
    avg_rows_returned FLOAT,
    cacheable BOOLEAN NOT NULL,
    uncacheable_reason JSONB,  -- {category, details}
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (instance_id, fingerprint)
);

-- Retention policy (run daily via cron)
-- DELETE FROM metrics_snapshots WHERE collected_at < NOW() - INTERVAL '7 days';
```

### pgcache Hosting

Deploy to Fly.io with multi-region support:

```bash
# Initial setup
fly launch --name pgcache-prod

# Add regions
fly regions add lhr nrt

# Scale to one instance per region
fly scale count 1 --region iad,lhr,nrt
```

Estimated cost: ~$3-5/mo per region × 3 regions = $10-15/mo for global coverage.

### Initial Deployment Regions

| Region Code | Location | Hyperdrive Binding | Fly Region |
|-------------|----------|-------------------|------------|
| US | US East | `HYPERDRIVE_US` | `iad` |
| EU | Europe West | `HYPERDRIVE_EU` | `lhr` |
| AS | Asia Pacific | `HYPERDRIVE_AS` | `nrt` |

## Consequences

### Positive

- **Dogfooding**: Dashboard queries flow through pgcache, validating the product
- **Simplicity**: No ORM or framework reduces dependencies and complexity
- **Cost-effective**: Cloudflare free tier + Fly.io small VMs + Neon free tier
- **Comprehensive metrics**: Detailed observability builds user trust
- **Global performance**: Multi-region pgcache instances minimize latency

### Negative

- **Per-request auth instantiation**: Overhead of creating BetterAuth instance per request
- **Pull-based metrics**: Delay between metric emission and collection (30-60s)
- **Storage limits**: 1-week retention initially limits historical analysis

### Trade-offs

- **TCP vs HTTP for database**: Using TCP (via Hyperdrive) enables pgcache dogfooding but requires more infrastructure than Neon's HTTP driver
- **Raw SQL vs ORM**: More boilerplate but better pgcache compatibility and transparency
- **Pull vs Push metrics**: Simpler initial implementation, can add push later for real-time needs

## Future Considerations

1. **Push-based metrics**: Add option for pgcache to push metrics for near-real-time dashboards
2. **Longer retention**: Move to TimescaleDB or time-series storage for extended history
3. **Aggregated views**: Cross-instance metrics aggregation by region or customer
4. **Alerting**: Integrate with alerting systems based on metric thresholds
5. **Query recommendations**: Use uncacheable reasons to suggest query optimizations

## References

- [BetterAuth on Cloudflare](https://hono.dev/examples/better-auth-on-cloudflare)
- [Neon Serverless Driver](https://neon.com/docs/serverless/serverless-driver)
- [Cloudflare Workers TCP Sockets](https://developers.cloudflare.com/workers/runtime-apis/tcp-sockets/)
- [Hyperdrive Connection Pooling](https://developers.cloudflare.com/hyperdrive/configuration/query-caching/)
- [metrics crate](https://docs.rs/metrics/latest/metrics/)
- [metrics-exporter-prometheus](https://crates.io/crates/metrics-exporter-prometheus)
- [Fly.io Pricing](https://fly.io/pricing/)
