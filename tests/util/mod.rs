#![allow(dead_code)]

use std::{
    io::{Error, Read, Write, stdout},
    net::TcpListener,
    ops::{Deref, DerefMut},
    process::{Child, Command, Stdio},
    time::Duration,
};

use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use pgcache_lib::metrics::MetricsSnapshot;
use pgtemp::{PgTempDB, PgTempDBBuilder};
use postgres_types::ToSql;
use regex_lite::Regex;
use tokio::time::sleep;
use tokio_postgres::{Client, Config, NoTls, Row, SimpleQueryMessage, Statement, ToStatement};
use tokio_util::bytes::{Buf, BytesMut};

/// Guard structure that automatically kills and waits for the pgcache process on drop
/// This ensures proper cleanup even if tests panic
pub struct PgCacheProcess {
    child: Child,
}

impl PgCacheProcess {
    fn new(child: Child) -> Self {
        Self { child }
    }
}

impl Drop for PgCacheProcess {
    fn drop(&mut self) {
        // Kill the process and wait for it to exit
        // We ignore errors here because the process might already be dead
        let _ = self.child.kill();
        let _ = self.child.wait();

        //drain stdout
        let _ = std::io::copy(&mut self.stdout.as_mut().unwrap(), &mut stdout());
    }
}

impl Deref for PgCacheProcess {
    type Target = Child;

    fn deref(&self) -> &Self::Target {
        &self.child
    }
}

impl DerefMut for PgCacheProcess {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.child
    }
}

fn find_available_port() -> Result<u16, Error> {
    // Bind to port 0 to let the OS assign an available port
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    // Drop the listener to free the port
    drop(listener);
    Ok(port)
}

pub fn proxy_wait_for_ready(pgcache: &mut PgCacheProcess) -> Result<(), Error> {
    const NEEDLE: &str = "Listening to";
    //wait for listening message from proxy before proceeding
    let mut buf = BytesMut::new();
    let mut read_buf = [0u8; 1024];

    let mut stdout = pgcache.stdout.take().unwrap();
    while !String::from_utf8_lossy(&buf).contains(NEEDLE) {
        if buf.len() > NEEDLE.len() {
            buf.advance(buf.len() - NEEDLE.len());
        }
        let cnt = stdout.read(&mut read_buf).unwrap_or_default();
        if cnt == 0 {
            return Err(Error::other("Unexpected end of stdout"));
        }
        std::io::stdout().write_all(&read_buf[0..cnt])?;
        buf.extend_from_slice(&read_buf[0..cnt]);
    }
    pgcache.stdout = Some(stdout);

    Ok(())
}

pub fn proxy_metrics_get(pgcache: &mut PgCacheProcess) -> Result<MetricsSnapshot, Error> {
    kill(Pid::from_raw(pgcache.child.id() as i32), Signal::SIGUSR1).unwrap();

    const NEEDLE: &str = "metrics:";
    let mut buf = BytesMut::new();
    let mut read_buf = [0u8; 1024];

    let mut stdout = pgcache.stdout.take().unwrap();
    while !String::from_utf8_lossy(&buf).contains(NEEDLE) {
        if buf.len() > NEEDLE.len() {
            buf.advance(buf.len() - NEEDLE.len());
        }
        let cnt = stdout.read(&mut read_buf).unwrap_or_default();
        if cnt == 0 {
            return Err(Error::other("Unexpected end of stdout"));
        }
        std::io::stdout().write_all(&read_buf[0..cnt])?;
        buf.extend_from_slice(&read_buf[0..cnt]);
    }

    const END: &str = "metrics end";
    while !String::from_utf8_lossy(&buf).contains(END) {
        let cnt = stdout.read(&mut read_buf).unwrap_or_default();
        if cnt == 0 {
            return Err(Error::other("Unexpected end of stdout"));
        }
        std::io::stdout().write_all(&read_buf[0..cnt])?;
        buf.extend_from_slice(&read_buf[0..cnt]);
    }

    let re = Regex::new(NEEDLE).unwrap();
    let str = String::from_utf8_lossy(&buf);
    let mat = re.find(&str).unwrap();
    buf.advance(mat.end());

    pgcache.stdout = Some(stdout);

    let mut metrics = MetricsSnapshot {
        queries_total: 0,
        queries_cacheable: 0,
        queries_uncacheable: 0,
        queries_unsupported: 0,
        queries_invalid: 0,
        queries_cache_hit: 0,
        queries_cache_miss: 0,
        queries_cache_error: 0,
        cache_hit_rate: 0.0,
        cacheability_rate: 0.0,
    };

    let re = Regex::new(r"([a-z_]+)=([0-9.]+)").unwrap();
    for (_, [name, value]) in re
        .captures_iter(&String::from_utf8_lossy(&buf))
        .map(|c| c.extract())
    {
        match name {
            "queries_total" => metrics.queries_total = value.parse().unwrap(),
            "queries_cacheable" => metrics.queries_cacheable = value.parse().unwrap(),
            "queries_uncacheable" => metrics.queries_uncacheable = value.parse().unwrap(),
            "queries_unsupported" => metrics.queries_unsupported = value.parse().unwrap(),
            "queries_invalid" => metrics.queries_invalid = value.parse().unwrap(),
            "queries_cache_hit" => metrics.queries_cache_hit = value.parse().unwrap(),
            "queries_cache_miss" => metrics.queries_cache_miss = value.parse().unwrap(),
            "queries_cache_error" => metrics.queries_cache_error = value.parse().unwrap(),
            "cache_hit_rate" => metrics.cache_hit_rate = value.parse().unwrap(),
            "cacheability_rate" => metrics.cacheability_rate = value.parse().unwrap(),
            name => {
                dbg!(name);
                todo!();
            }
        }
    }

    Ok(metrics)
}

pub struct TempDBs {
    pub origin: PgTempDB,
    pub cache: PgTempDB,
}

/// Test context combining all resources needed for integration tests.
/// Provides convenient methods for executing queries and checking metrics.
pub struct TestContext {
    pub dbs: TempDBs,
    pub pgcache: PgCacheProcess,
    pub cache_port: u16, // port pgcache proxy is listening on
    pub cache: Client,   // connected through pgcache proxy
    pub origin: Client,  // direct connection to origin database
}

impl TestContext {
    pub async fn setup() -> Result<Self, Error> {
        let (dbs, origin) = start_databases().await?;
        let (pgcache, cache_port, cache) = connect_pgcache(&dbs).await?;
        Ok(Self {
            dbs,
            pgcache,
            cache_port,
            cache,
            origin,
        })
    }

    /// Reconnect to the pgcache proxy, getting a fresh connection.
    /// Useful after SET search_path to pick up the new search_path on connection.
    pub async fn proxy_reconnect(&mut self) -> Result<(), Error> {
        let (client, connection) = Config::new()
            .host("localhost")
            .port(self.cache_port)
            .user("postgres")
            .dbname("origin_test")
            .connect(NoTls)
            .await
            .map_err(Error::other)?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {e}");
            }
        });

        self.cache = client;
        Ok(())
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

    /// Get metrics from pgcache process
    pub fn metrics(&mut self) -> Result<MetricsSnapshot, Error> {
        proxy_metrics_get(&mut self.pgcache)
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

pub async fn start_databases() -> Result<(TempDBs, Client), Error> {
    let db = PgTempDBBuilder::new()
        .with_dbname("origin_test")
        .with_config_param("wal_level", "logical")
        .start_async()
        .await;

    let db_cache = PgTempDBBuilder::new()
        .with_dbname("cache_test")
        .start_async()
        .await;

    //set up logical replication on origin
    let (origin_client, origin_connection) = Config::new()
        .host("localhost")
        .port(db.db_port())
        .user(db.db_user())
        .dbname(db.db_name())
        .connect(NoTls)
        .await
        .map_err(Error::other)?;

    tokio::spawn(async move {
        if let Err(e) = origin_connection.await {
            eprintln!("connection error: {e}");
        }
    });

    origin_client
        .execute("CREATE PUBLICATION pub_test FOR ALL TABLES", &[])
        .await
        .map_err(Error::other)?;

    origin_client
        .query(
            "SELECT * FROM pg_create_logical_replication_slot('slot_test', 'pgoutput')",
            &[],
        )
        .await
        .map_err(Error::other)?;

    Ok((
        TempDBs {
            origin: db,
            cache: db_cache,
        },
        origin_client,
    ))
}

pub async fn connect_pgcache(dbs: &TempDBs) -> Result<(PgCacheProcess, u16, Client), Error> {
    // Find a random available port
    let listen_port = find_available_port()?;
    let listen_socket = format!("127.0.0.1:{}", listen_port);

    let child = Command::new(env!("CARGO_BIN_EXE_pgcache"))
        .arg("--config")
        .arg("tests/data/default_config.toml")
        .arg("--origin_host")
        .arg("127.0.0.1")
        .arg("--origin_port")
        .arg(dbs.origin.db_port().to_string())
        .arg("--origin_user")
        .arg(dbs.origin.db_user())
        .arg("--origin_database")
        .arg(dbs.origin.db_name())
        .arg("--cache_host")
        .arg("127.0.0.1")
        .arg("--cache_port")
        .arg(dbs.cache.db_port().to_string())
        .arg("--cache_user")
        .arg(dbs.cache.db_user())
        .arg("--cache_database")
        .arg(dbs.cache.db_name())
        .arg("--listen_socket")
        .arg(&listen_socket)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("run pgcache");

    let mut pgcache = PgCacheProcess::new(child);

    //wait to listening message from proxy before proceeding
    proxy_wait_for_ready(&mut pgcache).map_err(Error::other)?;

    let (client, connection) = Config::new()
        .host("localhost")
        .port(listen_port)
        .user("postgres")
        .dbname("origin_test")
        .connect(NoTls)
        .await
        .map_err(Error::other)?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    Ok((pgcache, listen_port, client))
}

pub async fn query<T>(
    _pgcache: &mut PgCacheProcess,
    client: &Client,
    statement: &T,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<Row>, Error>
where
    T: ?Sized + ToStatement,
{
    client.query(statement, params).await.map_err(Error::other)
}

pub async fn simple_query(
    _pgcache: &mut PgCacheProcess,
    client: &Client,
    query: &str,
) -> Result<Vec<SimpleQueryMessage>, Error> {
    client.simple_query(query).await.map_err(Error::other)
}

/// Wait for CDC events to be processed by the cache system
/// Uses a short sleep to allow logical replication changes to propagate
pub async fn wait_for_cdc() {
    // TODO: Replace with proper synchronization mechanism (polling, notification, etc.)
    sleep(Duration::from_millis(500)).await;
}

/// Wait for queries to be loaded into cache
/// Uses a short sleep to allow cache to load
pub async fn wait_cache_load() {
    // TODO: Replace with proper synchronization mechanism (polling, notification, etc.)
    sleep(Duration::from_millis(250)).await;
}

/// Extract a row from SimpleQueryMessage results at the specified index
/// Returns an error with context if the message at that index is not a Row
pub fn extract_row(
    results: &[SimpleQueryMessage],
    index: usize,
) -> Result<&tokio_postgres::SimpleQueryRow, Error> {
    match results.get(index) {
        Some(SimpleQueryMessage::Row(row)) => Ok(row),
        Some(_) => Err(Error::other(format!(
            "Expected SimpleQueryMessage::Row at index {}, found different variant",
            index
        ))),
        None => Err(Error::other(format!(
            "Index {} out of bounds for results with length {}",
            index,
            results.len()
        ))),
    }
}

/// Assert that a row contains the expected field values
/// Panics with a descriptive message if any assertion fails
pub fn assert_row_fields(row: &tokio_postgres::SimpleQueryRow, expected: &[(&str, &str)]) {
    for (field, expected_value) in expected {
        assert_eq!(
            row.get::<&str>(field),
            Some(*expected_value),
            "Field '{}' did not match expected value",
            field
        );
    }
}

/// Convenience function combining row extraction and field assertion
pub fn assert_row_at(
    results: &[SimpleQueryMessage],
    index: usize,
    expected: &[(&str, &str)],
) -> Result<(), Error> {
    let row = extract_row(results, index)?;
    assert_row_fields(row, expected);
    Ok(())
}

/// Connect directly to the cache database (bypassing pgcache proxy)
/// Useful for verifying internal cache state like indexes
pub async fn connect_cache_db(dbs: &TempDBs) -> Result<Client, Error> {
    let (client, connection) = Config::new()
        .host("localhost")
        .port(dbs.cache.db_port())
        .user(dbs.cache.db_user())
        .dbname(dbs.cache.db_name())
        .connect(NoTls)
        .await
        .map_err(Error::other)?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("cache db connection error: {e}");
        }
    });

    Ok(client)
}

/// Setup test and test_map tables with base data for constraint invalidation tests
/// Creates tables and inserts base data:
/// - test: (1, 'foo'), (2, 'bar'), (3, 'baz')
/// - test_map: (1, 1, 'alpha'), (2, 1, 'beta'), (3, 2, 'gamma')
pub async fn setup_constraint_test_tables(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query("create table test (id integer primary key, data text)", &[])
        .await?;

    ctx.query(
        "create table test_map (id serial primary key, test_id integer, data text)",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_map (test_id, data) values (1, 'alpha'), (1, 'beta'), (2, 'gamma')",
        &[],
    )
    .await?;

    Ok(())
}
