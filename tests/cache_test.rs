use std::io::Error;

use crate::util::{
    assert_row_at, connect_pgcache, proxy_metrics_get, query, simple_query, start_databases,
    wait_cache_load, wait_for_cdc,
};

mod util;

#[tokio::test]
async fn test_cache_simple() -> Result<(), Error> {
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

    wait_cache_load().await;

    let res = simple_query(
        &mut pgcache,
        &client,
        "select id, data from test where data = 'foo'",
    )
    .await?;

    assert_eq!(res.len(), 3);
    assert_row_at(&res, 1, &[("id", "1"), ("data", "foo")])?;

    query(
        &mut pgcache,
        &origin,
        "insert into test (id, data) values (3, 'foo'), (4, 'bar')",
        &[],
    )
    .await?;

    wait_for_cdc().await;

    let res = simple_query(
        &mut pgcache,
        &client,
        "select id, data from test where data = 'foo'",
    )
    .await?;

    assert_eq!(res.len(), 4);
    assert_row_at(&res, 1, &[("id", "1"), ("data", "foo")])?;
    assert_row_at(&res, 2, &[("id", "3"), ("data", "foo")])?;

    let metrics = proxy_metrics_get(&mut pgcache).map_err(Error::other)?;
    assert_eq!(metrics.queries_total, 5);
    assert_eq!(metrics.queries_cacheable, 3);
    assert_eq!(metrics.queries_uncacheable, 2);
    assert_eq!(metrics.queries_unsupported, 2);
    assert_eq!(metrics.queries_cache_hit, 2);
    assert_eq!(metrics.queries_cache_miss, 1);

    Ok(())
}

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

    wait_for_cdc().await;

    let query_str = "select t.id, t.data as test_data, tm.test_id, tm.data as map_data \
        from test t join test_map tm on tm.test_id = t.id where t.id = 1
        order by tm.id;";

    // First query to populate cache
    let _ = simple_query(&mut pgcache, &client, query_str).await?;

    wait_cache_load().await;

    // Second query should hit cache
    let res = simple_query(&mut pgcache, &client, query_str).await?;

    assert_eq!(res.len(), 5);
    assert_row_at(
        &res,
        1,
        &[
            ("id", "1"),
            ("test_data", "foo"),
            ("test_id", "1"),
            ("map_data", "foo"),
        ],
    )?;
    assert_row_at(
        &res,
        2,
        &[
            ("id", "1"),
            ("test_data", "foo"),
            ("test_id", "1"),
            ("map_data", "bar"),
        ],
    )?;
    assert_row_at(
        &res,
        3,
        &[
            ("id", "1"),
            ("test_data", "foo"),
            ("test_id", "1"),
            ("map_data", "baz"),
        ],
    )?;

    // Trigger CDC events by modifying the test table
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

    wait_for_cdc().await;

    // Query after CDC should still return correct results
    let res = simple_query(&mut pgcache, &client, query_str).await?;
    dbg!(&res);
    assert_eq!(res.len(), 5);
    assert_row_at(
        &res,
        1,
        &[
            ("id", "1"),
            ("test_data", "foo"),
            ("test_id", "1"),
            ("map_data", "foo"),
        ],
    )?;
    assert_row_at(
        &res,
        2,
        &[
            ("id", "1"),
            ("test_data", "foo"),
            ("test_id", "1"),
            ("map_data", "bar"),
        ],
    )?;
    assert_row_at(
        &res,
        3,
        &[
            ("id", "1"),
            ("test_data", "foo"),
            ("test_id", "1"),
            ("map_data", "baz"),
        ],
    )?;

    let metrics = proxy_metrics_get(&mut pgcache).map_err(Error::other)?;
    assert_eq!(metrics.queries_total, 7);
    assert_eq!(metrics.queries_cacheable, 3);
    assert_eq!(metrics.queries_uncacheable, 4);
    assert_eq!(metrics.queries_unsupported, 4);
    assert_eq!(metrics.queries_invalid, 0);
    assert_eq!(metrics.queries_cache_hit, 1);
    assert_eq!(metrics.queries_cache_miss, 2);

    Ok(())
}
