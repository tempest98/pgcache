use std::collections::BTreeSet;

use iddqd::{BiHashMap, IdHashItem, IdHashMap, id_upcast};

use crate::{
    catalog::TableMetadata,
    query::{ast::SelectStatement, constraints::QueryConstraints, resolved::ResolvedSelectStatement},
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
    pub state: CachedQueryState,
    pub fingerprint: u64,
    /// Generation number assigned when query was registered (monotonically increasing)
    pub generation: u64,
    pub relation_oids: Vec<u32>,
    pub select_statement: SelectStatement,
    pub resolved: ResolvedSelectStatement,
    pub constraints: QueryConstraints,
    /// Estimated size of cached data in bytes (sum of raw value bytes)
    pub cached_bytes: usize,
}

impl IdHashItem for CachedQuery {
    type Key<'a> = u64;

    fn key(&self) -> Self::Key<'_> {
        self.fingerprint
    }

    id_upcast!();
}

/// Query used to update cached results when data changes
#[derive(Debug, Clone)]
pub struct UpdateQuery {
    /// Fingerprint of cached query that generated this update query
    pub fingerprint: u64,
    /// Resolved AST query
    pub resolved: ResolvedSelectStatement,
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
    pub cached_queries: IdHashMap<CachedQuery>,
    /// Monotonically increasing generation counter (starts at 1)
    pub generation_counter: u64,
    /// Generations of active cached queries (for efficient min-tracking)
    pub generations: BTreeSet<u64>,
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            tables: BiHashMap::new(),
            update_queries: IdHashMap::new(),
            cached_queries: IdHashMap::new(),
            generation_counter: 0,
            generations: BTreeSet::new(),
        }
    }
}

impl Cache {
    /// Returns the minimum generation that can be safely purged.
    /// This is the highest generation that is less than all active generations.
    /// Returns None if there are no active cached queries.
    pub fn generation_purge_threshold(&self) -> Option<u64> {
        self.generations.first().map(|min| min.saturating_sub(1))
    }
}
