use std::sync::atomic::{AtomicU64, Ordering};

/// Metrics collector for tracking query and cache performance.
/// Uses lock-free atomic counters for minimal overhead in hot paths.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Total number of queries received from clients (Query + Execute messages)
    queries_total: AtomicU64,

    /// Number of queries determined to be cacheable
    queries_cacheable: AtomicU64,

    /// Number of queries that cannot be cached
    queries_uncacheable: AtomicU64,

    /// Number of non-SELECT statements (INSERT, UPDATE, DELETE, DDL, etc.)
    queries_unsupported: AtomicU64,

    /// Number of queries that failed to parse
    queries_invalid: AtomicU64,

    /// Number of queries served from cache (cache hits)
    queries_cache_hit: AtomicU64,

    /// Number of queries where cache check failed (cache misses)
    queries_cache_miss: AtomicU64,

    /// Number of queries where cache returned an error
    queries_cache_error: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment total queries counter
    pub fn query_increment(&self) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment cacheable queries counter
    pub fn cacheable_increment(&self) {
        self.queries_cacheable.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment uncacheable queries counter
    pub fn uncacheable_increment(&self) {
        self.queries_uncacheable.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment non-SELECT queries counter
    pub fn unsupported_increment(&self) {
        self.queries_unsupported.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment parse error counter
    pub fn invalid_increment(&self) {
        self.queries_invalid.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment cache hit counter
    pub fn cache_hit_increment(&self) {
        self.queries_cache_hit.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment cache miss counter
    pub fn cache_miss_increment(&self) {
        self.queries_cache_miss.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment cache error counter
    pub fn cache_error_increment(&self) {
        self.queries_cache_error.fetch_add(1, Ordering::Relaxed);
    }

    /// Get current metrics snapshot
    pub fn snapshot(&self) -> MetricsSnapshot {
        let total = self.queries_total.load(Ordering::Relaxed);
        let cacheable = self.queries_cacheable.load(Ordering::Relaxed);
        let uncacheable = self.queries_uncacheable.load(Ordering::Relaxed);
        let non_select = self.queries_unsupported.load(Ordering::Relaxed);
        let parse_error = self.queries_invalid.load(Ordering::Relaxed);
        let cache_hit = self.queries_cache_hit.load(Ordering::Relaxed);
        let cache_miss = self.queries_cache_miss.load(Ordering::Relaxed);
        let cache_error = self.queries_cache_error.load(Ordering::Relaxed);

        let cache_hit_rate = if cacheable > 0 {
            (cache_hit as f64 / cacheable as f64) * 100.0
        } else {
            0.0
        };

        let queries_select = total.saturating_sub(non_select).saturating_sub(parse_error);
        let cacheability_rate = if queries_select > 0 {
            (cacheable as f64 / queries_select as f64) * 100.0
        } else {
            0.0
        };

        MetricsSnapshot {
            queries_total: total,
            queries_cacheable: cacheable,
            queries_uncacheable: uncacheable,
            queries_unsupported: non_select,
            queries_invalid: parse_error,
            queries_cache_hit: cache_hit,
            queries_cache_miss: cache_miss,
            queries_cache_error: cache_error,
            cache_hit_rate,
            cacheability_rate,
        }
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
