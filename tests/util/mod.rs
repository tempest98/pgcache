#![allow(dead_code)]

use std::{
    io::Error,
    io::Read,
    process::{Child, Command, Stdio},
};

use pgtemp::{PgTempDB, PgTempDBBuilder};
use postgres_types::ToSql;
use tokio_postgres::{Client, Config, NoTls, Row, SimpleQueryMessage, ToStatement};
use tokio_util::bytes::{Buf, BytesMut};

pub fn proxy_wait_for_ready(pgcache: &mut Child) -> Result<(), String> {
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
            return Err("Unexpected end of stdout".to_owned());
        }
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

pub async fn connect_pgcache(dbs: &TempDBs) -> Result<(Child, Client), Error> {
    let mut pgcache = Command::new(env!("CARGO_BIN_EXE_pgcache"))
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
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("run pgcache");

    //wait to listening message from proxy before proceeding
    proxy_wait_for_ready(&mut pgcache).map_err(Error::other)?;

    let (client, connection) = Config::new()
        .host("localhost")
        .port(6432)
        .user("postgres")
        .dbname("origin_test")
        .connect(NoTls)
        .await
        .map_err(|e| {
            pgcache.wait().expect("exit_status");
            Error::other(e)
        })?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    Ok((pgcache, client))
}

pub async fn query<T>(
    pgcache: &mut Child,
    client: &Client,
    statement: &T,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<Row>, Error>
where
    T: ?Sized + ToStatement,
{
    let rv = client.query(statement, params).await.map_err(|e| {
        pgcache.kill().expect("pgcache killed");
        pgcache.wait().expect("exit_status");
        Error::other(e)
    })?;

    Ok(rv)
}

pub async fn simple_query(
    pgcache: &mut Child,
    client: &Client,
    query: &str,
) -> Result<Vec<SimpleQueryMessage>, Error> {
    let rv = client.simple_query(query).await.map_err(|e| {
        pgcache.kill().expect("pgcache killed");
        pgcache.wait().expect("exit_status");
        Error::other(e)
    })?;

    Ok(rv)
}
