use std::{
    io::Error,
    process::{Command, Stdio},
    time::Duration,
};

use pgtemp::PgTempDBBuilder;
use tokio::time::sleep;
use tokio_postgres::{Config, NoTls, SimpleQueryMessage};

mod util;

#[tokio::test]
async fn test_cache() -> Result<(), Error> {
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
    let (origin, origin_connection) = Config::new()
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

    origin
        .execute("CREATE PUBLICATION pub_test FOR ALL TABLES", &[])
        .await
        .map_err(Error::other)?;

    origin
        .query(
            "SELECT * FROM pg_create_logical_replication_slot('slot_test', 'pgoutput')",
            &[],
        )
        .await
        .map_err(Error::other)?;

    let mut pgcache = Command::new(env!("CARGO_BIN_EXE_pgcache"))
        .arg("--origin_host")
        .arg("127.0.0.1")
        .arg("--origin_port")
        .arg(db.db_port().to_string())
        .arg("--origin_user")
        .arg(db.db_user())
        .arg("--origin_database")
        .arg(db.db_name())
        .arg("--cache_host")
        .arg("127.0.0.1")
        .arg("--cache_port")
        .arg(db_cache.db_port().to_string())
        .arg("--cache_user")
        .arg(db_cache.db_user())
        .arg("--cache_database")
        .arg(db_cache.db_name())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("run pgcache");

    //wait to listening message from proxy before proceeding
    util::proxy_wait_for_ready(&mut pgcache);
    // sleep(Duration::from_secs(2)).await;

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

    client
        .query("create table test (id integer primary key, data text)", &[])
        .await
        .map_err(|e| {
            pgcache.kill().expect("pgcache killed");
            pgcache.wait().expect("exit_status");
            Error::other(e)
        })?;

    client
        .query(
            "insert into test (id, data) values (1, 'foo'), (2, 'bar')",
            &[],
        )
        .await
        .map_err(|e| {
            pgcache.kill().expect("pgcache killed");
            pgcache.wait().expect("exit_status");
            Error::other(e)
        })?;

    let res = client
        .simple_query("select id, data from test where data = 'foo'")
        .await
        .map_err(|e| {
            pgcache.kill().expect("pgcache killed");
            pgcache.wait().expect("exit_status");
            Error::other(e)
        })?;

    assert_eq!(res.len(), 3);
    let SimpleQueryMessage::Row(row) = &res[1] else {
        panic!("exepcted SimpleQueryMessage::Row");
    };
    assert_eq!(row.get::<&str>("id"), Some("1"));
    assert_eq!(row.get::<&str>("data"), Some("foo"));

    let res = client
        .simple_query("select id, data from test where data = 'foo'")
        .await
        .map_err(|e| {
            pgcache.kill().expect("pgcache killed");
            pgcache.wait().expect("exit_status");
            Error::other(e)
        })?;

    assert_eq!(res.len(), 3);
    let SimpleQueryMessage::Row(row) = &res[1] else {
        panic!("exepcted SimpleQueryMessage::Row");
    };
    assert_eq!(row.get::<&str>("id"), Some("1"));
    assert_eq!(row.get::<&str>("data"), Some("foo"));

    origin
        .query(
            "insert into test (id, data) values (3, 'foo'), (4, 'bar')",
            &[],
        )
        .await
        .map_err(|e| {
            pgcache.kill().expect("pgcache killed");
            pgcache.wait().expect("exit_status");
            Error::other(e)
        })?;

    sleep(Duration::from_millis(250)).await; //is there a better way to do this?

    let res = client
        .simple_query("select id, data from test where data = 'foo'")
        .await
        .map_err(|e| {
            pgcache.kill().expect("pgcache killed");
            pgcache.wait().expect("exit_status");
            Error::other(e)
        })?;

    assert_eq!(res.len(), 4);
    let SimpleQueryMessage::Row(row) = &res[1] else {
        panic!("exepcted SimpleQueryMessage::Row");
    };
    assert_eq!(row.get::<&str>("id"), Some("1"));
    assert_eq!(row.get::<&str>("data"), Some("foo"));

    let SimpleQueryMessage::Row(row) = &res[2] else {
        panic!("exepcted SimpleQueryMessage::Row");
    };
    assert_eq!(row.get::<&str>("id"), Some("3"));
    assert_eq!(row.get::<&str>("data"), Some("foo"));

    pgcache.kill().expect("command killed");
    pgcache.wait().expect("exit_status");

    Ok(())
}
