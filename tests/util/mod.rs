#![allow(dead_code)]

use std::{
    io::{Error, Read, Write, stdout},
    net::TcpListener,
    ops::{Deref, DerefMut},
    process::{Child, Command, Stdio},
    time::Duration,
};

use pgtemp::{PgTempDB, PgTempDBBuilder};
use postgres_types::ToSql;
use tokio::time::sleep;
use tokio_postgres::{Client, Config, NoTls, Row, SimpleQueryMessage, ToStatement};
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
    //wait to listening message from proxy before proceeding
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
    origin: PgTempDB,
    cache: PgTempDB,
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

pub async fn connect_pgcache(dbs: &TempDBs) -> Result<(PgCacheProcess, Client), Error> {
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

    Ok((pgcache, client))
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
    sleep(Duration::from_millis(250)).await;
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

/// Setup test and test_map tables with base data for constraint invalidation tests
/// Creates tables and inserts base data:
/// - test: (1, 'foo'), (2, 'bar'), (3, 'baz')
/// - test_map: (1, 1, 'alpha'), (2, 1, 'beta'), (3, 2, 'gamma')
pub async fn setup_constraint_test_tables(
    pgcache: &mut PgCacheProcess,
    client: &Client,
) -> Result<(), Error> {
    query(
        pgcache,
        client,
        "create table test (id integer primary key, data text)",
        &[],
    )
    .await?;

    query(
        pgcache,
        client,
        "create table test_map (id serial primary key, test_id integer, data text)",
        &[],
    )
    .await?;

    query(
        pgcache,
        client,
        "insert into test (id, data) values (1, 'foo'), (2, 'bar'), (3, 'baz')",
        &[],
    )
    .await?;

    query(
        pgcache,
        client,
        "insert into test_map (test_id, data) values (1, 'alpha'), (1, 'beta'), (2, 'gamma')",
        &[],
    )
    .await?;

    Ok(())
}
