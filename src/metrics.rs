use std::net::SocketAddr;
use std::sync::Arc;

use metrics::{Counter, Key, KeyName, Recorder, SharedString, Unit};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use metrics_util::registry::{AtomicStorage, Registry};

/// Metric names as constants for consistency
pub mod names {
    // Counter metrics
    pub const QUERIES_TOTAL: &str = "pgcache.queries.total";
    pub const QUERIES_CACHEABLE: &str = "pgcache.queries.cacheable";
    pub const QUERIES_UNCACHEABLE: &str = "pgcache.queries.uncacheable";
    pub const QUERIES_UNSUPPORTED: &str = "pgcache.queries.unsupported";
    pub const QUERIES_INVALID: &str = "pgcache.queries.invalid";
    pub const QUERIES_CACHE_HIT: &str = "pgcache.queries.cache_hit";
    pub const QUERIES_CACHE_MISS: &str = "pgcache.queries.cache_miss";
    pub const QUERIES_CACHE_ERROR: &str = "pgcache.queries.cache_error";

    // Histogram metrics (latency in seconds per Prometheus convention)
    pub const QUERY_LATENCY_SECONDS: &str = "pgcache.query.latency_seconds";
    pub const CACHE_LOOKUP_LATENCY_SECONDS: &str = "pgcache.cache.lookup_latency_seconds";
    pub const ORIGIN_LATENCY_SECONDS: &str = "pgcache.origin.latency_seconds";

    // Connection metrics
    pub const CONNECTIONS_TOTAL: &str = "pgcache.connections.total";
    pub const CONNECTIONS_ACTIVE: &str = "pgcache.connections.active";
    pub const CONNECTIONS_ERRORS: &str = "pgcache.connections.errors";

    // CDC/Replication metrics
    pub const CDC_EVENTS_PROCESSED: &str = "pgcache.cdc.events_processed";
    pub const CDC_INSERTS: &str = "pgcache.cdc.inserts";
    pub const CDC_UPDATES: &str = "pgcache.cdc.updates";
    pub const CDC_DELETES: &str = "pgcache.cdc.deletes";
    pub const CDC_LAG_BYTES: &str = "pgcache.cdc.lag_bytes";
    pub const CDC_LAG_SECONDS: &str = "pgcache.cdc.lag_seconds";

    // Cache performance metrics
    pub const CACHE_INVALIDATIONS: &str = "pgcache.cache.invalidations";
    pub const CACHE_EVICTIONS: &str = "pgcache.cache.evictions";

    // Cache state metrics
    pub const CACHE_QUERIES_REGISTERED: &str = "pgcache.cache.queries_registered";
    pub const CACHE_SIZE_BYTES: &str = "pgcache.cache.size_bytes";
    pub const CACHE_SIZE_LIMIT_BYTES: &str = "pgcache.cache.size_limit_bytes";
    pub const CACHE_GENERATION: &str = "pgcache.cache.generation";
    pub const CACHE_TABLES_TRACKED: &str = "pgcache.cache.tables_tracked";

    // Extended protocol metrics
    pub const PROTOCOL_SIMPLE_QUERIES: &str = "pgcache.protocol.simple_queries";
    pub const PROTOCOL_EXTENDED_QUERIES: &str = "pgcache.protocol.extended_queries";
    pub const PROTOCOL_PREPARED_STATEMENTS: &str = "pgcache.protocol.prepared_statements";
}

/// Snapshot recorder that stores counter values for programmatic access.
/// Used for exit logging and tests.
#[derive(Clone)]
struct SnapshotRecorder {
    registry: Arc<Registry<Key, AtomicStorage>>,
}

impl SnapshotRecorder {
    fn new() -> Self {
        Self {
            registry: Arc::new(Registry::new(AtomicStorage)),
        }
    }

    /// Get a point-in-time snapshot of all metrics.
    fn snapshot(&self) -> MetricsSnapshot {
        let counter_value = |name: &'static str| -> u64 {
            let key = Key::from_static_name(name);
            self.registry
                .get_counter(&key)
                .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
                .unwrap_or(0)
        };

        let total = counter_value(names::QUERIES_TOTAL);
        let cacheable = counter_value(names::QUERIES_CACHEABLE);
        let uncacheable = counter_value(names::QUERIES_UNCACHEABLE);
        let unsupported = counter_value(names::QUERIES_UNSUPPORTED);
        let invalid = counter_value(names::QUERIES_INVALID);
        let cache_hit = counter_value(names::QUERIES_CACHE_HIT);
        let cache_miss = counter_value(names::QUERIES_CACHE_MISS);
        let cache_error = counter_value(names::QUERIES_CACHE_ERROR);

        let cache_hit_rate = if cacheable > 0 {
            (cache_hit as f64 / cacheable as f64) * 100.0
        } else {
            0.0
        };

        let queries_select = total.saturating_sub(unsupported).saturating_sub(invalid);
        let cacheability_rate = if queries_select > 0 {
            (cacheable as f64 / queries_select as f64) * 100.0
        } else {
            0.0
        };

        MetricsSnapshot {
            queries_total: total,
            queries_cacheable: cacheable,
            queries_uncacheable: uncacheable,
            queries_unsupported: unsupported,
            queries_invalid: invalid,
            queries_cache_hit: cache_hit,
            queries_cache_miss: cache_miss,
            queries_cache_error: cache_error,
            cache_hit_rate,
            cacheability_rate,
        }
    }
}

impl Recorder for SnapshotRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &metrics::Metadata<'_>) -> Counter {
        self.registry
            .get_or_create_counter(key, |c| Arc::clone(c).into())
    }

    fn register_gauge(&self, key: &Key, _metadata: &metrics::Metadata<'_>) -> metrics::Gauge {
        self.registry
            .get_or_create_gauge(key, |g| Arc::clone(g).into())
    }

    fn register_histogram(
        &self,
        key: &Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        self.registry
            .get_or_create_histogram(key, |h| Arc::clone(h).into())
    }
}

/// Dual recorder that routes metrics to both Prometheus (for HTTP endpoint)
/// and a snapshot recorder (for programmatic access in tests and exit logging).
pub struct DualRecorder {
    prometheus: metrics_exporter_prometheus::PrometheusRecorder,
    snapshot: SnapshotRecorder,
}

impl DualRecorder {
    /// Install the dual recorder as the global metrics recorder.
    ///
    /// If `metrics_socket` is provided, starts an HTTP server on that address
    /// serving Prometheus metrics at `/metrics`.
    ///
    /// Returns a handle for snapshot access.
    pub fn install(metrics_socket: Option<SocketAddr>) -> Result<MetricsHandle, &'static str> {
        use std::sync::mpsc;

        let snapshot = SnapshotRecorder::new();

        // Configure Prometheus with quantiles for histograms
        let mut builder = PrometheusBuilder::new()
            .set_quantiles(&[0.5, 0.95, 0.99])
            .expect("configure prometheus quantiles");

        // Optionally bind HTTP listener
        if let Some(socket) = metrics_socket {
            builder = builder.with_http_listener(socket);
        }

        // build() requires a tokio runtime context, so we create the runtime
        // in a background thread, build the recorder there, and send it back
        let (tx, rx) = mpsc::sync_channel(1);

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("create tokio runtime for metrics exporter");

            // Enter runtime context so build() can set up the HTTP listener
            let _guard = rt.enter();

            let (prometheus, exporter_future) = match builder.build() {
                Ok(result) => result,
                Err(e) => {
                    let _ = tx.send(Err(format!("failed to build prometheus: {e:?}")));
                    return;
                }
            };

            let prometheus_handle = prometheus.handle();
            let _ = tx.send(Ok((prometheus, prometheus_handle)));

            // Run the HTTP exporter (blocks until shutdown)
            if let Err(e) = rt.block_on(exporter_future) {
                tracing::error!("prometheus exporter failed: {e:?}");
            }
        });

        let (prometheus, prometheus_handle) = rx
            .recv()
            .map_err(|_| "metrics thread failed to start")?
            .map_err(|_| "failed to build prometheus recorder")?;

        let recorder = Self {
            prometheus,
            snapshot: snapshot.clone(),
        };

        metrics::set_global_recorder(recorder).map_err(|_| "failed to install metrics recorder")?;

        Ok(MetricsHandle {
            snapshot,
            _prometheus_handle: prometheus_handle,
        })
    }
}

impl Recorder for DualRecorder {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.prometheus
            .describe_counter(key.clone(), unit, description.clone());
        self.snapshot.describe_counter(key, unit, description);
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.prometheus
            .describe_gauge(key.clone(), unit, description.clone());
        self.snapshot.describe_gauge(key, unit, description);
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.prometheus
            .describe_histogram(key.clone(), unit, description.clone());
        self.snapshot.describe_histogram(key, unit, description);
    }

    fn register_counter(&self, key: &Key, metadata: &metrics::Metadata<'_>) -> Counter {
        // Register with both recorders but return a fanout counter
        let prom_counter = self.prometheus.register_counter(key, metadata);
        let snap_counter = self.snapshot.register_counter(key, metadata);
        Arc::new(FanoutCounter::new(prom_counter, snap_counter)).into()
    }

    fn register_gauge(&self, key: &Key, metadata: &metrics::Metadata<'_>) -> metrics::Gauge {
        let prom_gauge = self.prometheus.register_gauge(key, metadata);
        let snap_gauge = self.snapshot.register_gauge(key, metadata);
        Arc::new(FanoutGauge::new(prom_gauge, snap_gauge)).into()
    }

    fn register_histogram(
        &self,
        key: &Key,
        metadata: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        // For histograms, only use Prometheus since snapshot doesn't need them
        self.prometheus.register_histogram(key, metadata)
    }
}

/// A counter that fans out to multiple underlying counters.
struct FanoutCounter {
    prometheus: Counter,
    snapshot: Counter,
}

impl FanoutCounter {
    fn new(prometheus: Counter, snapshot: Counter) -> Self {
        Self {
            prometheus,
            snapshot,
        }
    }
}

impl metrics::CounterFn for FanoutCounter {
    fn increment(&self, value: u64) {
        self.prometheus.increment(value);
        self.snapshot.increment(value);
    }

    fn absolute(&self, value: u64) {
        self.prometheus.absolute(value);
        self.snapshot.absolute(value);
    }
}

/// A gauge that fans out to multiple underlying gauges.
struct FanoutGauge {
    prometheus: metrics::Gauge,
    snapshot: metrics::Gauge,
}

impl FanoutGauge {
    fn new(prometheus: metrics::Gauge, snapshot: metrics::Gauge) -> Self {
        Self {
            prometheus,
            snapshot,
        }
    }
}

impl metrics::GaugeFn for FanoutGauge {
    fn increment(&self, value: f64) {
        self.prometheus.increment(value);
        self.snapshot.increment(value);
    }

    fn decrement(&self, value: f64) {
        self.prometheus.decrement(value);
        self.snapshot.decrement(value);
    }

    fn set(&self, value: f64) {
        self.prometheus.set(value);
        self.snapshot.set(value);
    }
}

/// Handle for accessing metrics snapshots after installation.
#[derive(Clone)]
pub struct MetricsHandle {
    snapshot: SnapshotRecorder,
    _prometheus_handle: PrometheusHandle,
}

impl MetricsHandle {
    /// Get a point-in-time snapshot of all metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        self.snapshot.snapshot()
    }
}

/// Simple recorder for tests - stores counter values without Prometheus overhead.
/// Uses metrics-util's Registry with AtomicStorage for thread-safe counters.
#[derive(Clone)]
pub struct PgCacheRecorder {
    registry: Arc<Registry<Key, AtomicStorage>>,
}

impl PgCacheRecorder {
    pub fn new() -> Self {
        Self {
            registry: Arc::new(Registry::new(AtomicStorage)),
        }
    }

    /// Install this recorder as the global metrics recorder.
    /// Returns a clone of the recorder for snapshot access.
    pub fn install() -> Result<Self, &'static str> {
        let recorder = Self::new();
        let recorder_clone = recorder.clone();
        metrics::set_global_recorder(recorder).map_err(|_| "failed to install metrics recorder")?;
        Ok(recorder_clone)
    }

    /// Get a point-in-time snapshot of all metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        let counter_value = |name: &'static str| -> u64 {
            let key = Key::from_static_name(name);
            self.registry
                .get_counter(&key)
                .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
                .unwrap_or(0)
        };

        let total = counter_value(names::QUERIES_TOTAL);
        let cacheable = counter_value(names::QUERIES_CACHEABLE);
        let uncacheable = counter_value(names::QUERIES_UNCACHEABLE);
        let unsupported = counter_value(names::QUERIES_UNSUPPORTED);
        let invalid = counter_value(names::QUERIES_INVALID);
        let cache_hit = counter_value(names::QUERIES_CACHE_HIT);
        let cache_miss = counter_value(names::QUERIES_CACHE_MISS);
        let cache_error = counter_value(names::QUERIES_CACHE_ERROR);

        let cache_hit_rate = if cacheable > 0 {
            (cache_hit as f64 / cacheable as f64) * 100.0
        } else {
            0.0
        };

        let queries_select = total.saturating_sub(unsupported).saturating_sub(invalid);
        let cacheability_rate = if queries_select > 0 {
            (cacheable as f64 / queries_select as f64) * 100.0
        } else {
            0.0
        };

        MetricsSnapshot {
            queries_total: total,
            queries_cacheable: cacheable,
            queries_uncacheable: uncacheable,
            queries_unsupported: unsupported,
            queries_invalid: invalid,
            queries_cache_hit: cache_hit,
            queries_cache_miss: cache_miss,
            queries_cache_error: cache_error,
            cache_hit_rate,
            cacheability_rate,
        }
    }
}

impl Default for PgCacheRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl Recorder for PgCacheRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &metrics::Metadata<'_>) -> Counter {
        self.registry
            .get_or_create_counter(key, |c| Arc::clone(c).into())
    }

    fn register_gauge(&self, key: &Key, _metadata: &metrics::Metadata<'_>) -> metrics::Gauge {
        self.registry
            .get_or_create_gauge(key, |g| Arc::clone(g).into())
    }

    fn register_histogram(
        &self,
        key: &Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        self.registry
            .get_or_create_histogram(key, |h| Arc::clone(h).into())
    }
}

/// Point-in-time snapshot of metrics for reporting
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub queries_total: u64,
    pub queries_cacheable: u64,
    pub queries_uncacheable: u64,
    pub queries_unsupported: u64,
    pub queries_invalid: u64,
    pub queries_cache_hit: u64,
    pub queries_cache_miss: u64,
    pub queries_cache_error: u64,
    pub cache_hit_rate: f64,
    pub cacheability_rate: f64,
}

impl std::fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "queries_total={}", self.queries_total)?;
        writeln!(f, "queries_cacheable={}", self.queries_cacheable)?;
        writeln!(f, "queries_uncacheable={}", self.queries_uncacheable)?;
        writeln!(f, "queries_unsupported={}", self.queries_unsupported)?;
        writeln!(f, "queries_invalid={}", self.queries_invalid)?;
        writeln!(f, "queries_cache_hit={}", self.queries_cache_hit)?;
        writeln!(f, "queries_cache_miss={}", self.queries_cache_miss)?;
        writeln!(f, "queries_cache_error={}", self.queries_cache_error)?;
        writeln!(f, "cache_hit_rate={:.2}%", self.cache_hit_rate)?;
        writeln!(f, "cacheability_rate={:.2}%", self.cacheability_rate)?;
        write!(f, "metrics end")
    }
}
