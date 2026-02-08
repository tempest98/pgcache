use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use iddqd::{BiHashItem, BiHashMap, IdHashItem, IdHashMap, bi_upcast, id_upcast};

use crate::{
    catalog::TableMetadata,
    query::{ast::QueryExpr, constraints::QueryConstraints, resolved::ResolvedQueryExpr},
    settings::Settings,
};

/// State of a cached query
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachedQueryState {
    Ready,
    Loading,
}

/// A cached query with its metadata and state
#[derive(Debug)]
pub struct CachedQuery {
    pub fingerprint: u64,
    /// Generation number assigned when query was registered (monotonically increasing)
    pub generation: u64,
    pub relation_oids: Vec<u32>,
    pub query: QueryExpr,
    pub resolved: ResolvedQueryExpr,
    /// Estimated size of cached data in bytes (sum of raw value bytes)
    pub cached_bytes: usize,
    /// Timestamp when registration started (for latency metrics)
    pub registration_started_at: Option<Instant>,
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
    /// Table appears directly in FROM clause (including JOINs)
    Direct,
    /// Table appears inside a subquery, CTE, or derived table
    Subquery(SubqueryKind),
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
pub type ActiveRelations = Arc<RwLock<HashSet<u32>>>;

/// Lightweight read-only view of cache state for the coordinator.
/// Updated by the writer thread after each mutation.
#[derive(Debug, Default)]
pub struct CacheStateView {
    pub cached_queries: HashMap<u64, CachedQueryView>,
}

/// Lightweight view of a cached query for coordinator lookups.
#[derive(Debug, Clone)]
pub struct CachedQueryView {
    pub state: CachedQueryState,
    /// Generation number (0 for Loading placeholder before writer assigns real value)
    pub generation: u64,
    /// Resolved query (None for Loading placeholder before writer resolves)
    pub resolved: Option<ResolvedQueryExpr>,
}
