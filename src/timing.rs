//! Per-query timing instrumentation for tracking query latency across processing stages.
//!
//! This module provides timing infrastructure to measure how long queries spend in each
//! stage of processing. It is only active when the `query-timing` feature is enabled.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use crate::metrics::names;

/// Global sequence counter for generating unique query instance IDs.
static QUERY_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Unique identifier for a query instance.
///
/// Combines the query fingerprint (identifies the query pattern) with a sequence number
/// (identifies the specific execution instance). This allows comparing timing of the
/// same query across different executions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryId {
    /// Query pattern fingerprint (hash of the SQL text or AST)
    pub fingerprint: u64,
    /// Unique sequence number for this execution instance
    pub sequence: u64,
}

impl QueryId {
    /// Create a new QueryId with the given fingerprint and a unique sequence number.
    pub fn new(fingerprint: u64) -> Self {
        Self {
            fingerprint,
            sequence: QUERY_SEQUENCE.fetch_add(1, Ordering::Relaxed),
        }
    }
}

/// Query processing outcome for timing classification.
#[derive(Debug, Clone, Copy)]
pub enum QueryOutcome {
    /// Query was served from cache
    CacheHit,
    /// Query was not in cache, forwarded to origin
    CacheMiss,
    /// Query was forwarded to origin (not cacheable or other reason)
    Forward,
    /// Query execution failed
    Error,
}

/// Computed durations for each processing stage (in nanoseconds for precision).
#[derive(Debug, Default)]
pub struct QueryDurations {
    /// Time spent parsing the query
    pub parse_ns: Option<u64>,
    /// Time from parsed to dispatched to cache
    pub dispatch_ns: Option<u64>,
    /// Time spent in cache lookup
    pub lookup_ns: Option<u64>,
    /// Time spent routing to cache hit or miss
    pub routing_ns: Option<u64>,
    /// Time spent executing query in cache worker
    pub worker_execution_ns: Option<u64>,
    /// Time spent writing response to client
    pub response_write_ns: Option<u64>,
    /// Total time from reception to completion
    pub total_ns: Option<u64>,
}

/// Per-stage timing data for a single query instance.
///
/// All timestamp fields are Option to allow incremental capture as the query
/// progresses through the system. The `received_at` field is always set.
#[derive(Debug, Clone)]
pub struct QueryTiming {
    /// Unique identifier for this query instance
    pub query_id: QueryId,

    /// When the query message was received from the client
    pub received_at: Instant,

    /// When query parsing/fingerprinting completed
    pub parsed_at: Option<Instant>,

    /// When the query was dispatched to the cache channel
    pub dispatched_at: Option<Instant>,

    /// When cache lookup completed (found Ready, Loading, or Miss)
    pub lookup_complete_at: Option<Instant>,

    /// When the cache worker started processing (cache hit path)
    pub worker_start_at: Option<Instant>,

    /// When the cache database query completed (cache hit path)
    pub query_done_at: Option<Instant>,

    /// When the response was fully written to the client (cache hit path)
    pub response_written_at: Option<Instant>,

    /// When the query was forwarded to origin (cache miss/forward path)
    pub forwarded_at: Option<Instant>,

    /// When the origin response completed (forward path)
    pub origin_response_at: Option<Instant>,

    /// Final outcome of the query
    pub outcome: Option<QueryOutcome>,
}

impl QueryTiming {
    /// Create a new QueryTiming with the given ID and current time as received_at.
    pub fn new(query_id: QueryId, received_at: Instant) -> Self {
        Self {
            query_id,
            received_at,
            parsed_at: None,
            dispatched_at: None,
            lookup_complete_at: None,
            worker_start_at: None,
            query_done_at: None,
            response_written_at: None,
            forwarded_at: None,
            origin_response_at: None,
            outcome: None,
        }
    }

    /// Calculate per-stage durations from the captured timestamps.
    pub fn durations(&self) -> QueryDurations {
        QueryDurations {
            parse_ns: self
                .parsed_at
                .map(|t| t.duration_since(self.received_at).as_nanos() as u64),
            dispatch_ns: self.dispatched_at.and_then(|d| {
                self.parsed_at
                    .map(|p| d.duration_since(p).as_nanos() as u64)
            }),
            lookup_ns: self.lookup_complete_at.and_then(|l| {
                self.dispatched_at
                    .map(|d| l.duration_since(d).as_nanos() as u64)
            }),
            routing_ns: self.worker_start_at.and_then(|l| {
                self.lookup_complete_at
                    .map(|d| l.duration_since(d).as_nanos() as u64)
            }),
            worker_execution_ns: self.query_done_at.and_then(|e| {
                self.worker_start_at
                    .map(|s| e.duration_since(s).as_nanos() as u64)
            }),
            response_write_ns: self.response_written_at.and_then(|r| {
                self.query_done_at
                    .map(|e| r.duration_since(e).as_nanos() as u64)
            }),
            total_ns: self
                .response_written_at
                .or(self.origin_response_at)
                .map(|end| end.duration_since(self.received_at).as_nanos() as u64),
        }
    }
}

/// Record timing data to Prometheus histograms and optionally trace log.
///
/// Called when a query completes (either via cache hit or forward path).
pub fn timing_record(timing: &QueryTiming) {
    let durations = timing.durations();

    // Record to Prometheus histograms (converting ns to seconds)
    if let Some(ns) = durations.parse_ns {
        metrics::histogram!(names::QUERY_STAGE_PARSE_SECONDS).record(ns as f64 / 1_000_000_000.0);
    }
    if let Some(ns) = durations.dispatch_ns {
        metrics::histogram!(names::QUERY_STAGE_DISPATCH_SECONDS)
            .record(ns as f64 / 1_000_000_000.0);
    }
    if let Some(ns) = durations.lookup_ns {
        metrics::histogram!(names::QUERY_STAGE_LOOKUP_SECONDS).record(ns as f64 / 1_000_000_000.0);
    }
    if let Some(ns) = durations.routing_ns {
        metrics::histogram!(names::QUERY_STAGE_ROUTING_SECONDS).record(ns as f64 / 1_000_000_000.0);
    }
    if let Some(ns) = durations.worker_execution_ns {
        metrics::histogram!(names::QUERY_STAGE_WORKER_EXEC_SECONDS)
            .record(ns as f64 / 1_000_000_000.0);
    }
    if let Some(ns) = durations.response_write_ns {
        metrics::histogram!(names::QUERY_STAGE_RESPONSE_WRITE_SECONDS)
            .record(ns as f64 / 1_000_000_000.0);
    }
    if let Some(ns) = durations.total_ns {
        metrics::histogram!(names::QUERY_STAGE_TOTAL_SECONDS).record(ns as f64 / 1_000_000_000.0);
    }

    // Trace-level logging for detailed per-query debugging
    if tracing::enabled!(tracing::Level::TRACE) {
        tracing::trace!(
            fingerprint = timing.query_id.fingerprint,
            sequence = timing.query_id.sequence,
            outcome = ?timing.outcome,
            total_us = durations.total_ns.map(|n| n / 1000),
            parse_us = durations.parse_ns.map(|n| n / 1000),
            dispatch_us = durations.dispatch_ns.map(|n| n / 1000),
            lookup_us = durations.lookup_ns.map(|n| n / 1000),
            routing_us = durations.routing_ns.map(|n| n / 1000),
            worker_us = durations.worker_execution_ns.map(|n| n / 1000),
            write_us = durations.response_write_ns.map(|n| n / 1000),
            "query timing breakdown"
        );
    }
}
