use std::io::Error;

use tokio_postgres::SimpleQueryMessage;

use crate::util::{connect_pgcache, query, simple_query, start_databases};

mod util;

#[tokio::test]
async fn test_proxy() -> Result<(), Error> {
    let (dbs, _origin) = start_databases().await?;
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
        "insert into test (id, data) values (1, 'foo'), (2, 'bar')",
        &[],
    )
    .await?;

    let res = simple_query(
        &mut pgcache,
        &client,
        "select id, data from test where data = 'foo'",
    )
    .await?;

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
