use std::{
    io::{Error, Read},
    process::Command,
    process::Stdio,
};

use pgtemp::PgTempDBBuilder;
use tokio_postgres::{Config, NoTls, SimpleQueryMessage};

#[tokio::test]
async fn test_proxy() -> Result<(), Error> {
    let db = PgTempDBBuilder::new()
        .with_dbname("origin_test")
        .start_async()
        .await;

    dbg!(db.connection_uri());
    dbg!(db.db_port());
    dbg!(db.data_dir());
    dbg!(env!("CARGO_BIN_EXE_pgcache"));

    let mut pgcache = Command::new(env!("CARGO_BIN_EXE_pgcache"))
        .arg("--origin_host")
        .arg("127.0.0.1")
        .arg("--origin_port")
        .arg(db.db_port().to_string())
        .arg("--origin_user")
        .arg(db.db_user())
        .arg("--origin_database")
        .arg(db.db_name())
        .stdout(Stdio::piped())
        // .stderr(Stdio::null())
        .spawn()
        .expect("run pgcache");

    //wait to read some data to make sure pgcache process is initialized before proceeding
    let mut buf = [0u8; 256];
    let _ = pgcache.stdout.take().unwrap().read(&mut buf);

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

    //task to process connection to cache pg db
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    client
        .query("create table test (id integer, data text)", &[])
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

    pgcache.kill().expect("command killed");
    pgcache.wait().expect("exit_status");
    Ok(())
}
