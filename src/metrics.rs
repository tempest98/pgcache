use std::sync::Arc;

use metrics::{Counter, Key, KeyName, Recorder, SharedString, Unit};
use metrics_util::registry::{AtomicStorage, Registry};

/// Metric names as constants for consistency
pub mod names {
    pub const QUERIES_TOTAL: &str = "pgcache.queries.total";
    pub const QUERIES_CACHEABLE: &str = "pgcache.queries.cacheable";
    pub const QUERIES_UNCACHEABLE: &str = "pgcache.queries.uncacheable";
    pub const QUERIES_UNSUPPORTED: &str = "pgcache.queries.unsupported";
    pub const QUERIES_INVALID: &str = "pgcache.queries.invalid";
    pub const QUERIES_CACHE_HIT: &str = "pgcache.queries.cache_hit";
    pub const QUERIES_CACHE_MISS: &str = "pgcache.queries.cache_miss";
    pub const QUERIES_CACHE_ERROR: &str = "pgcache.queries.cache_error";
}

/// Simple recorder that stores counter values for snapshot support.
/// Uses metrics-util's Registry with AtomicStorage for thread-safe counters.
///
/// The registry is wrapped in Arc to allow cloning and shared access
/// between the global recorder and snapshot functionality.
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
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        // Descriptions not needed for our use case
    }

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        // Descriptions not needed for our use case
    }

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        // Descriptions not needed for our use case
    }

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
