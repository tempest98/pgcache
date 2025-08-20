use std::{io::Error, time::Duration};

use tokio::time::sleep;
use tokio_postgres::SimpleQueryMessage;

use crate::util::{connect_pgcache, query, simple_query, start_databases};

mod util;

#[tokio::test]
async fn test_cache_join() -> Result<(), Error> {
    let (dbs, origin) = start_databases().await?;
    let (mut pgcache, client) = connect_pgcache(&dbs).await?;

    query(
        &mut pgcache,
        &client,
        "create table test (id integer primary key, data text)",
        &[],
    )
    .await?;

    query(
        &mut pgcache,
        &client,
        "create table test_map (id serial primary key, test_id integer, data text)",
        &[],
    )
    .await?;

    query(
        &mut pgcache,
        &client,
        "insert into test (id, data) values (1, 'foo'), (2, 'bar')",
        &[],
    )
    .await?;

    query(
        &mut pgcache,
        &client,
        "insert into test_map (test_id, data) values \
        (1, 'foo'), \
        (1, 'bar'), \
        (1, 'baz'), \
        (2, 'foo'), \
        (2, 'bar'), \
        (2, 'baz')",
        &[],
    )
    .await?;

    let _ = simple_query(
        &mut pgcache,
        &client,
        "select t.id, t.data as test_data, tm.test_id, tm.data as map_data \
        from test t join test_map tm on tm.test_id = t.id where t.id = 1
        order by tm.id;",
    )
    .await?;

    let res = simple_query(
        &mut pgcache,
        &client,
        "select t.id, t.data as test_data, tm.test_id, tm.data as map_data \
        from test t join test_map tm on tm.test_id = t.id where t.id = 1
        order by tm.id;",
    )
    .await?;

    assert_eq!(res.len(), 5);
    let SimpleQueryMessage::Row(row) = &res[1] else {
        panic!("exepcted SimpleQueryMessage::Row");
    };
    assert_eq!(row.get::<&str>("id"), Some("1"));
    assert_eq!(row.get::<&str>("test_data"), Some("foo"));
    assert_eq!(row.get::<&str>("test_id"), Some("1"));
    assert_eq!(row.get::<&str>("map_data"), Some("foo"));

    let SimpleQueryMessage::Row(row) = &res[2] else {
        panic!("exepcted SimpleQueryMessage::Row");
    };
    assert_eq!(row.get::<&str>("id"), Some("1"));
    assert_eq!(row.get::<&str>("test_data"), Some("foo"));
    assert_eq!(row.get::<&str>("test_id"), Some("1"));
    assert_eq!(row.get::<&str>("map_data"), Some("bar"));

    let SimpleQueryMessage::Row(row) = &res[3] else {
        panic!("exepcted SimpleQueryMessage::Row");
    };
    assert_eq!(row.get::<&str>("id"), Some("1"));
    assert_eq!(row.get::<&str>("test_data"), Some("foo"));
    assert_eq!(row.get::<&str>("test_id"), Some("1"));
    assert_eq!(row.get::<&str>("map_data"), Some("baz"));

    query(
        &mut pgcache,
        &origin,
        "update test set id = 10 where id = 1",
        &[],
    )
    .await?;

    query(
        &mut pgcache,
        &origin,
        "update test set id = 1 where id = 10",
        &[],
    )
    .await?;

    sleep(Duration::from_millis(250)).await; //is there a better way to do this?

    let res = simple_query(
        &mut pgcache,
        &client,
        "select t.id, t.data as test_data, tm.test_id, tm.data as map_data \
        from test t join test_map tm on tm.test_id = t.id where t.id = 1
        order by tm.id;",
    )
    .await?;

    assert_eq!(res.len(), 5);
    let SimpleQueryMessage::Row(row) = &res[1] else {
        panic!("exepcted SimpleQueryMessage::Row");
    };
    assert_eq!(row.get::<&str>("id"), Some("1"));
    assert_eq!(row.get::<&str>("test_data"), Some("foo"));
    assert_eq!(row.get::<&str>("test_id"), Some("1"));
    assert_eq!(row.get::<&str>("map_data"), Some("foo"));

    let SimpleQueryMessage::Row(row) = &res[2] else {
        panic!("exepcted SimpleQueryMessage::Row");
    };
    assert_eq!(row.get::<&str>("id"), Some("1"));
    assert_eq!(row.get::<&str>("test_data"), Some("foo"));
    assert_eq!(row.get::<&str>("test_id"), Some("1"));
    assert_eq!(row.get::<&str>("map_data"), Some("bar"));

    let SimpleQueryMessage::Row(row) = &res[3] else {
        panic!("exepcted SimpleQueryMessage::Row");
    };
    assert_eq!(row.get::<&str>("id"), Some("1"));
    assert_eq!(row.get::<&str>("test_data"), Some("foo"));
    assert_eq!(row.get::<&str>("test_id"), Some("1"));
    assert_eq!(row.get::<&str>("map_data"), Some("baz"));

    pgcache.kill().expect("command killed");
    pgcache.wait().expect("exit_status");

    Ok(())
}
