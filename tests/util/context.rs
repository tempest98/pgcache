use std::io::Error;

use postgres_types::ToSql;
use tokio_postgres::{Client, Row, SimpleQueryMessage, Statement, ToStatement};

use super::metrics::MetricsSnapshot;
use super::process::{
    PgCacheProcess, TempDBs, connect_pgcache, connect_pgcache_allowlist, connect_pgcache_clock,
    connect_pgcache_pinned, connect_pgcache_pinned_small_cache, connect_pgcache_small_cache,
    start_databases,
};

/// Test context combining all resources needed for integration tests.
/// Fields are ordered for correct drop sequence: drop clients first
/// (closing connections and allowing spawned tasks to exit), then kill
/// pgcache, and finally tear down temp databases.
pub struct TestContext {
    pub cache: Client,     // connected through pgcache proxy
    pub origin: Client,    // direct connection to origin database
    pub cache_port: u16,   // port pgcache proxy is listening on
    pub metrics_port: u16, // port for HTTP metrics endpoint
    pub pgcache: PgCacheProcess,
    pub dbs: TempDBs,
}

impl TestContext {
    pub async fn setup() -> Result<Self, Error> {
        let (dbs, origin) = start_databases().await?;
        let (pgcache, cache_port, metrics_port, cache) = connect_pgcache(&dbs).await?;
        Ok(Self {
            dbs,
            pgcache,
            cache_port,
            metrics_port,
            cache,
            origin,
        })
    }

    /// Set up a test context with a small cache to force eviction.
    pub async fn setup_small_cache(cache_size: usize) -> Result<Self, Error> {
        let (dbs, origin) = start_databases().await?;
        let (pgcache, cache_port, metrics_port, cache) =
            connect_pgcache_small_cache(&dbs, cache_size).await?;
        Ok(Self {
            cache,
            origin,
            cache_port,
            metrics_port,
            pgcache,
            dbs,
        })
    }

    /// Set up a test context with clock eviction policy.
    pub async fn setup_clock(admission_threshold: u32) -> Result<Self, Error> {
        let (dbs, origin) = start_databases().await?;
        let (pgcache, cache_port, metrics_port, cache) =
            connect_pgcache_clock(&dbs, admission_threshold).await?;
        Ok(Self {
            dbs,
            pgcache,
            cache_port,
            metrics_port,
            cache,
            origin,
        })
    }

    /// Set up a test context with a table allowlist.
    pub async fn setup_allowlist(allowed_tables: &str) -> Result<Self, Error> {
        let (dbs, origin) = start_databases().await?;
        let (pgcache, cache_port, metrics_port, cache) =
            connect_pgcache_allowlist(&dbs, allowed_tables).await?;
        Ok(Self {
            cache,
            origin,
            cache_port,
            metrics_port,
            pgcache,
            dbs,
        })
    }

    /// Set up a test context with pinned queries.
    /// The `before_start` closure runs against the origin database after tables
    /// are created but before pgcache spawns, so pinned queries can reference
    /// existing tables.
    pub async fn setup_pinned<F, Fut>(pinned_queries: &str, before_start: F) -> Result<Self, Error>
    where
        F: FnOnce(Client) -> Fut,
        Fut: std::future::Future<Output = Result<Client, Error>>,
    {
        let (dbs, origin) = start_databases().await?;
        // Run setup closure to create tables/data before pgcache starts
        let origin = before_start(origin).await?;
        let (pgcache, cache_port, metrics_port, cache) =
            connect_pgcache_pinned(&dbs, pinned_queries).await?;
        Ok(Self {
            cache,
            origin,
            cache_port,
            metrics_port,
            pgcache,
            dbs,
        })
    }

    /// Set up a test context with pinned queries and a small cache to force eviction.
    pub async fn setup_pinned_small_cache<F, Fut>(
        pinned_queries: &str,
        cache_size: usize,
        before_start: F,
    ) -> Result<Self, Error>
    where
        F: FnOnce(Client) -> Fut,
        Fut: std::future::Future<Output = Result<Client, Error>>,
    {
        let (dbs, origin) = start_databases().await?;
        let origin = before_start(origin).await?;
        let (pgcache, cache_port, metrics_port, cache) =
            connect_pgcache_pinned_small_cache(&dbs, pinned_queries, cache_size).await?;
        Ok(Self {
            cache,
            origin,
            cache_port,
            metrics_port,
            pgcache,
            dbs,
        })
    }

    /// Execute query through pgcache proxy
    pub async fn query<T>(
        &mut self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.cache
            .query(statement, params)
            .await
            .map_err(Error::other)
    }

    /// Execute query directly on origin (bypassing pgcache)
    pub async fn origin_query<T>(
        &mut self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.origin
            .query(statement, params)
            .await
            .map_err(Error::other)
    }

    /// Execute simple query through pgcache proxy
    pub async fn simple_query(&mut self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.cache.simple_query(query).await.map_err(Error::other)
    }

    /// Get metrics from pgcache HTTP endpoint
    pub async fn metrics(&mut self) -> Result<MetricsSnapshot, Error> {
        super::metrics::metrics_http_get(self.metrics_port).await
    }

    /// Prepare a statement through pgcache proxy
    pub async fn prepare(&self, query: &str) -> Result<Statement, Error> {
        self.cache.prepare(query).await.map_err(Error::other)
    }

    /// Execute query_one through pgcache proxy
    pub async fn query_one<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.cache
            .query_one(statement, params)
            .await
            .map_err(Error::other)
    }
}
