use std::io::Error;

use crate::util::{assert_row_at, connect_pgcache, query, simple_query, start_databases};

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
    assert_row_at(&res, 1, &[("id", "1"), ("data", "foo")])?;

    pgcache.kill().expect("command killed");
    pgcache.wait().expect("exit_status");
    Ok(())
}
