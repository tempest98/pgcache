#![allow(dead_code)]

use std::{
    io::{Error, Read, Write, stdout},
    net::TcpListener,
    ops::{Deref, DerefMut},
    path::Path,
    process::{Child, Command, Stdio},
    sync::Once,
    time::Duration,
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream as TokioTcpStream;

static CRYPTO_INIT: Once = Once::new();

/// Initialize the rustls crypto provider (required before using TLS)
fn crypto_provider_init() {
    CRYPTO_INIT.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("install crypto provider");
    });
}

use pgcache_lib::tls::{MakeRustlsConnect, tls_config_with_cert};
use pgtemp::{PgTempDB, PgTempDBBuilder};
use postgres_types::ToSql;
use tokio::time::sleep;
use tokio_postgres::{Client, Config, NoTls, Row, SimpleQueryMessage, Statement, ToStatement};
use tokio_util::bytes::{Buf, BytesMut};

/// Point-in-time snapshot of metrics for test assertions.
/// Populated by parsing metrics from the Prometheus HTTP endpoint.
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

        // Drain stdout if available
        if let Some(ref mut child_stdout) = self.child.stdout {
            let _ = std::io::copy(child_stdout, &mut stdout());
        }
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

pub struct TempDBs {
    pub origin: PgTempDB,
    pub cache: PgTempDB,
}

/// Test context combining all resources needed for integration tests.
/// Provides convenient methods for executing queries and checking metrics.
pub struct TestContext {
    pub dbs: TempDBs,
    pub pgcache: PgCacheProcess,
    pub cache_port: u16,   // port pgcache proxy is listening on
    pub metrics_port: u16, // port for HTTP metrics endpoint
    pub cache: Client,     // connected through pgcache proxy
    pub origin: Client,    // direct connection to origin database
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
        metrics_http_get(self.metrics_port).await
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
        .with_config_param("log_destination", "stderr")
        .with_config_param("log_directory", "/tmp/")
        .with_config_param("logging_collector", "on")
        .with_config_param("shared_preload_libraries", "pgcache_pgrx")
        .start_async()
        .await;

    // Set up pgcache_pgrx extension on cache database
    let (cache_client, cache_connection) = Config::new()
        .host("localhost")
        .port(db_cache.db_port())
        .user(db_cache.db_user())
        .dbname(db_cache.db_name())
        .connect(NoTls)
        .await
        .map_err(Error::other)?;

    tokio::spawn(async move {
        if let Err(e) = cache_connection.await {
            eprintln!("cache connection error: {e}");
        }
    });

    cache_client
        .execute("CREATE EXTENSION pgcache_pgrx", &[])
        .await
        .map_err(Error::other)?;

    // Set up logical replication on origin
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
        .execute(
            "CREATE PUBLICATION pub_test FOR ALL TABLES WITH (publish_via_partition_root = true)",
            &[],
        )
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

/// Spawn a pgcache process with the given extra CLI arguments.
/// Returns the process, listen port, and metrics port.
fn pgcache_spawn(
    dbs: &TempDBs,
    listen_port: u16,
    metrics_port: u16,
    extra_args: &[&str],
) -> PgCacheProcess {
    let listen_socket = format!("127.0.0.1:{}", listen_port);
    let metrics_socket = format!("127.0.0.1:{}", metrics_port);

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_pgcache"));
    cmd.arg("--config")
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
        .arg("--metrics_socket")
        .arg(&metrics_socket);

    for arg in extra_args {
        cmd.arg(arg);
    }

    let child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("run pgcache");

    PgCacheProcess::new(child)
}

/// Connect a plain TCP client to pgcache.
async fn pgcache_client_connect(listen_port: u16) -> Result<Client, Error> {
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

    Ok(client)
}

pub async fn connect_pgcache(dbs: &TempDBs) -> Result<(PgCacheProcess, u16, u16, Client), Error> {
    let listen_port = find_available_port()?;
    let metrics_port = find_available_port()?;
    let mut pgcache = pgcache_spawn(dbs, listen_port, metrics_port, &["--cache_policy", "fifo"]);
    proxy_wait_for_ready(&mut pgcache).map_err(Error::other)?;
    let client = pgcache_client_connect(listen_port).await?;
    Ok((pgcache, listen_port, metrics_port, client))
}

/// Connect to pgcache with clock eviction policy.
pub async fn connect_pgcache_clock(
    dbs: &TempDBs,
    admission_threshold: u32,
) -> Result<(PgCacheProcess, u16, u16, Client), Error> {
    let listen_port = find_available_port()?;
    let metrics_port = find_available_port()?;
    let threshold_str = admission_threshold.to_string();
    let mut pgcache = pgcache_spawn(
        dbs,
        listen_port,
        metrics_port,
        &[
            "--cache_policy",
            "clock",
            "--admission_threshold",
            &threshold_str,
        ],
    );
    proxy_wait_for_ready(&mut pgcache).map_err(Error::other)?;
    let client = pgcache_client_connect(listen_port).await?;
    Ok((pgcache, listen_port, metrics_port, client))
}

/// Connect to pgcache with TLS enabled on the proxy.
pub async fn connect_pgcache_tls(
    dbs: &TempDBs,
) -> Result<(PgCacheProcess, u16, u16, Client), Error> {
    crypto_provider_init();

    let listen_port = find_available_port()?;
    let metrics_port = find_available_port()?;
    let mut pgcache = pgcache_spawn(
        dbs,
        listen_port,
        metrics_port,
        &[
            "--tls_cert",
            "tests/data/certs/server.crt",
            "--tls_key",
            "tests/data/certs/server.key",
            "--cache_policy",
            "fifo",
        ],
    );
    proxy_wait_for_ready(&mut pgcache).map_err(Error::other)?;

    let cert_path = Path::new("tests/data/certs/server.crt");
    let tls_config = tls_config_with_cert(cert_path)?;
    let tls = MakeRustlsConnect::new(tls_config);

    let (client, connection) = Config::new()
        .host("localhost")
        .port(listen_port)
        .user("postgres")
        .dbname("origin_test")
        .connect(tls)
        .await
        .map_err(Error::other)?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    Ok((pgcache, listen_port, metrics_port, client))
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

/// Fetch metrics via HTTP from the Prometheus endpoint
pub async fn metrics_http_get(port: u16) -> Result<MetricsSnapshot, Error> {
    let mut stream = TokioTcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .map_err(Error::other)?;

    // Send HTTP GET request
    let request = "GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(Error::other)?;

    // Read response
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .map_err(Error::other)?;

    // Parse Prometheus text format
    metrics_prometheus_parse(&response)
}

/// Parse Prometheus text format into MetricsSnapshot
fn metrics_prometheus_parse(response: &str) -> Result<MetricsSnapshot, Error> {
    let mut queries_total = 0u64;
    let mut queries_cacheable = 0u64;
    let mut queries_uncacheable = 0u64;
    let mut queries_unsupported = 0u64;
    let mut queries_invalid = 0u64;
    let mut queries_cache_hit = 0u64;
    let mut queries_cache_miss = 0u64;
    let mut queries_cache_error = 0u64;

    for line in response.lines() {
        // Skip comments and empty lines
        if line.starts_with('#') || line.is_empty() {
            continue;
        }

        // Parse "metric_name value" format
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            let name = parts[0];
            let value: u64 = parts[1].parse().unwrap_or(0);

            match name {
                "pgcache_queries_total" => queries_total = value,
                "pgcache_queries_cacheable" => queries_cacheable = value,
                "pgcache_queries_uncacheable" => queries_uncacheable = value,
                "pgcache_queries_unsupported" => queries_unsupported = value,
                "pgcache_queries_invalid" => queries_invalid = value,
                "pgcache_queries_cache_hit" => queries_cache_hit = value,
                "pgcache_queries_cache_miss" => queries_cache_miss = value,
                "pgcache_queries_cache_error" => queries_cache_error = value,
                _ => {}
            }
        }
    }

    let cache_hit_rate = if queries_cacheable > 0 {
        (queries_cache_hit as f64 / queries_cacheable as f64) * 100.0
    } else {
        0.0
    };

    let queries_select = queries_total
        .saturating_sub(queries_unsupported)
        .saturating_sub(queries_invalid);
    let cacheability_rate = if queries_select > 0 {
        (queries_cacheable as f64 / queries_select as f64) * 100.0
    } else {
        0.0
    };

    Ok(MetricsSnapshot {
        queries_total,
        queries_cacheable,
        queries_uncacheable,
        queries_unsupported,
        queries_invalid,
        queries_cache_hit,
        queries_cache_miss,
        queries_cache_error,
        cache_hit_rate,
        cacheability_rate,
    })
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

/// Assert the last cacheable query was a cache miss. Returns updated snapshot.
pub async fn assert_cache_miss(
    ctx: &mut TestContext,
    before: MetricsSnapshot,
) -> Result<MetricsSnapshot, Error> {
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.queries_cache_miss, 1, "expected cache miss");
    assert_eq!(delta.queries_cache_hit, 0, "unexpected cache hit");
    Ok(after)
}

/// Assert the last cacheable query was a cache hit. Returns updated snapshot.
pub async fn assert_cache_hit(
    ctx: &mut TestContext,
    before: MetricsSnapshot,
) -> Result<MetricsSnapshot, Error> {
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.queries_cache_hit, 1, "expected cache hit");
    assert_eq!(delta.queries_cache_miss, 0, "unexpected cache miss");
    Ok(after)
}

/// Calculate metrics delta between two snapshots.
/// Useful for asserting metrics within a consolidated test where metrics accumulate.
pub fn metrics_delta(before: &MetricsSnapshot, after: &MetricsSnapshot) -> MetricsSnapshot {
    MetricsSnapshot {
        queries_total: after.queries_total - before.queries_total,
        queries_cacheable: after.queries_cacheable - before.queries_cacheable,
        queries_uncacheable: after.queries_uncacheable - before.queries_uncacheable,
        queries_unsupported: after.queries_unsupported - before.queries_unsupported,
        queries_invalid: after.queries_invalid - before.queries_invalid,
        queries_cache_hit: after.queries_cache_hit - before.queries_cache_hit,
        queries_cache_miss: after.queries_cache_miss - before.queries_cache_miss,
        queries_cache_error: after.queries_cache_error - before.queries_cache_error,
        // Rates are cumulative averages, not meaningful for deltas
        cache_hit_rate: 0.0,
        cacheability_rate: 0.0,
    }
}
