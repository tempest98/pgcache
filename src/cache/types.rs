use std::collections::{BTreeSet, HashSet};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use hdrhistogram::Histogram;

use iddqd::{BiHashItem, BiHashMap, IdHashItem, IdHashMap, bi_upcast, id_upcast};

use crate::{
    cache::query::CacheableQuery,
    catalog::TableMetadata,
    query::{ast::QueryExpr, constraints::QueryConstraints, resolved::ResolvedQueryExpr},
    settings::{CachePolicy, Settings},
};

/// Shared resolved query expression, wrapped in Arc to avoid deep cloning
/// on every cache hit (the coordinator→worker path).
pub type SharedResolved = Arc<ResolvedQueryExpr>;

/// State of a cached query
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachedQueryState {
    /// Seen but not yet admitted to cache (clock policy only).
    /// The u32 tracks how many times this query has been seen.
    Pending(u32),
    /// Admitted, population in progress
    Loading,
    /// Cached and serving hits
    Ready,
    /// CDC-invalidated, awaiting re-hit for fast readmission (clock policy only)
    Invalidated,
}

/// A cached query with its metadata and state
#[derive(Debug)]
pub struct CachedQuery {
    pub fingerprint: u64,
    /// Generation number assigned when query was registered (monotonically increasing)
    pub generation: u64,
    pub relation_oids: Vec<u32>,
    pub query: QueryExpr,
    pub resolved: SharedResolved,
    /// Maximum rows cached for this fingerprint.
    /// `None` = all rows cached (query seen without LIMIT, or OFFSET-only).
    /// `Some(n)` = up to `n` rows cached (max LIMIT+OFFSET across all variants seen).
    pub max_limit: Option<u64>,
    /// Estimated size of cached data in bytes (sum of raw value bytes)
    pub cached_bytes: usize,
    /// Timestamp when registration started (for latency metrics)
    pub registration_started_at: Option<Instant>,
    /// True when in Invalidated state (kept in cached_queries for metadata reuse on readmission)
    pub invalidated: bool,
    /// Pinned queries are protected from eviction and auto-readmitted after CDC invalidation.
    pub pinned: bool,
}

impl BiHashItem for CachedQuery {
    type K1<'a> = u64;
    type K2<'b> = u64;

    fn key1(&self) -> Self::K1<'_> {
        self.fingerprint
    }

    fn key2(&self) -> Self::K2<'_> {
        self.generation
    }

    bi_upcast!();
}

// impl IdHashItem for CachedQuery {
//     type Key<'a> = u64;

//     fn key(&self) -> Self::Key<'_> {
//         self.fingerprint
//     }

//     id_upcast!();
// }

/// The kind of subquery context a table was found in.
/// Determines invalidation behavior based on whether the subquery's
/// result set growing or shrinking affects the outer query.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SubqueryKind {
    /// IN, EXISTS, FROM subquery, CTE.
    /// Set growth → outer result grows → invalidate.
    /// Set shrink → outer result shrinks → skip.
    Inclusion,
    /// NOT IN (<> ALL), NOT EXISTS.
    /// Set growth → outer result shrinks → skip.
    /// Set shrink → outer result grows → invalidate.
    Exclusion,
    /// Scalar subquery returning a single value.
    /// Any change → invalidate.
    Scalar,
}

/// Whether an update query was derived from a direct table or a subquery table
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UpdateQuerySource {
    /// Table appears in the FROM clause (inner join or single table)
    FromClause,
    /// Table appears inside a subquery, CTE, or derived table
    Subquery(SubqueryKind),
    /// Table is on the terminal optional side of an outer join.
    /// Its columns don't appear in WHERE or other join conditions, so
    /// CDC INSERT/DELETE can be handled in place without invalidation.
    /// The preserved side already has the row — changes here only affect
    /// which values fill the NULL-padded columns.
    OuterJoinTerminal,
    /// Table is on the non-terminal optional side of an outer join
    /// (its columns appear in WHERE or other join conditions).
    /// CDC events trigger full query invalidation rather than row-level updates.
    OuterJoinOptional,
}

/// Query used to update cached results when data changes
#[derive(Debug, Clone)]
pub struct UpdateQuery {
    /// Fingerprint of cached query that generated this update query
    pub fingerprint: u64,
    /// Resolved AST query
    pub resolved: ResolvedQueryExpr,
    /// Complexity score (lower = simpler = more likely to match = try first)
    pub complexity: usize,
    /// Whether this table was found directly in FROM or inside a subquery
    pub source: UpdateQuerySource,
    /// WHERE clause constraints for CDC invalidation filtering
    pub constraints: QueryConstraints,
    /// Whether the parent cached query has a LIMIT (max_limit.is_some()).
    /// Used by CDC to determine if DELETE should trigger invalidation.
    pub has_limit: bool,
}

/// Collection of update queries for a specific relation
#[derive(Debug)]
pub struct UpdateQueries {
    pub relation_oid: u32,
    pub queries: Vec<UpdateQuery>,
}

impl IdHashItem for UpdateQueries {
    type Key<'a> = u32;

    fn key(&self) -> Self::Key<'_> {
        self.relation_oid
    }

    id_upcast!();
}

/// Main cache data structure containing all cached state
#[derive(Debug)]
pub struct Cache {
    pub tables: BiHashMap<TableMetadata>,
    pub update_queries: IdHashMap<UpdateQueries>,
    pub cached_queries: BiHashMap<CachedQuery>,
    /// Monotonically increasing generation counter (starts at 1)
    pub generation_counter: u64,
    /// Generations of active cached queries (for efficient min-tracking)
    pub generations: BTreeSet<u64>,
    /// Target size of cached data, if the current size is larger then
    /// queries will be invalidated to decrease cached data
    pub cache_size: Option<usize>,
    /// Size of currently cached data, updated after loading queries or purging data
    /// Actual size can drift from this value because of CDC traffic
    pub current_size: usize,
    /// Cache eviction policy
    pub cache_policy: CachePolicy,
    /// Number of times a query must be seen before admission (clock policy only)
    pub admission_threshold: u32,
}

impl Cache {
    pub fn new(settings: &Settings) -> Self {
        Self {
            tables: BiHashMap::new(),
            update_queries: IdHashMap::new(),
            cached_queries: BiHashMap::new(),
            generation_counter: 0,
            generations: BTreeSet::new(),
            cache_size: settings.cache_size,
            current_size: 0,
            cache_policy: settings.cache_policy,
            admission_threshold: settings.admission_threshold,
        }
    }

    /// Returns the minimum generation that can be safely purged.
    /// This is the highest generation that is less than all active generations
    /// or the current generation_counter if there are no active generations
    pub fn generation_purge_threshold(&self) -> u64 {
        self.generations
            .first()
            .map(|min| min.saturating_sub(1))
            .unwrap_or(self.generation_counter)
    }
}

/// Shared set of relation OIDs that have active cached queries.
/// Written by the writer thread, read by the CDC processor.
pub type ActiveRelations = Arc<ArcSwap<HashSet<u32>>>;

/// Per-query operational metrics.
///
/// All writes go through `DashMap::get_mut()`, which holds an exclusive shard lock
/// for the duration of access. This means plain `u64` fields and the `Histogram`
/// need no additional synchronization — the shard lock provides mutual exclusion.
///
/// Only two threads write: coordinator (hit/miss/subsumption counts) and
/// writer (all other fields including histogram recording from worker channel).
pub struct QueryMetrics {
    pub hit_count: u64,
    pub miss_count: u64,
    /// Nanoseconds since `CacheStateView.started_at`
    pub last_hit_at_ns: Option<NonZeroU64>,
    /// Nanoseconds since `CacheStateView.started_at` when query became Ready
    pub cached_since_ns: Option<NonZeroU64>,
    pub invalidation_count: u64,
    pub readmission_count: u64,
    pub eviction_count: u64,
    pub subsumption_count: u64,
    pub population_count: u64,
    pub last_population_duration_us: Option<NonZeroU64>,
    pub total_bytes_served: u64,
    /// Physical rows cached (sum across all branch tables)
    pub row_count: u64,
    /// Cache-hit latency distribution (1us–60s, 2 significant figures)
    pub cache_hit_latency: Histogram<u64>,
}

impl QueryMetrics {
    pub fn new() -> Self {
        Self {
            hit_count: 0,
            miss_count: 0,
            last_hit_at_ns: None,
            cached_since_ns: None,
            invalidation_count: 0,
            readmission_count: 0,
            eviction_count: 0,
            subsumption_count: 0,
            population_count: 0,
            last_population_duration_us: None,
            total_bytes_served: 0,
            row_count: 0,
            #[allow(clippy::unwrap_used)]
            cache_hit_latency: Histogram::new_with_bounds(1, 60_000_000, 2).unwrap(),
        }
    }
}

/// Metrics sent from worker to coordinator after each cache hit.
pub struct WorkerMetrics {
    pub fingerprint: u64,
    pub latency_us: u64,
    pub bytes_served: u64,
}

/// Shared cache state for coordinator lookups and writer updates.
/// Uses DashMap for per-shard locking — reads to one shard don't block
/// writes to another, eliminating the global RwLock bottleneck.
pub struct CacheStateView {
    pub cached_queries: DashMap<u64, CachedQueryView>,
    pub metrics: DashMap<u64, QueryMetrics>,
    pub started_at: Instant,
}

impl std::fmt::Debug for CacheStateView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheStateView")
            .field("cached_queries", &self.cached_queries)
            .field("metrics_len", &self.metrics.len())
            .field("started_at", &self.started_at)
            .finish()
    }
}

impl CacheStateView {
    pub fn new() -> Self {
        Self {
            cached_queries: DashMap::new(),
            metrics: DashMap::new(),
            started_at: Instant::now(),
        }
    }
}

/// Lightweight view of a cached query for coordinator lookups.
#[derive(Debug, Clone)]
pub struct CachedQueryView {
    pub state: CachedQueryState,
    /// Generation number (0 for Loading placeholder before writer assigns real value)
    pub generation: u64,
    /// Resolved query (None for Loading placeholder before writer resolves)
    pub resolved: Option<SharedResolved>,
    /// Maximum rows cached for this fingerprint (None = all rows)
    pub max_limit: Option<u64>,
    /// CLOCK reference bit — set by coordinator on cache hit, read/cleared by writer during eviction
    pub referenced: bool,
}

/// A pre-validated pinned query, ready for registration.
pub struct PinnedQuery {
    pub fingerprint: u64,
    pub cacheable_query: Arc<CacheableQuery>,
}
