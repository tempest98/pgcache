//! Integration test for `count(*) FILTER (WHERE ...)` aggregates: each
//! FILTER predicate must survive AST round-trip so distinct columns return
//! distinct counts.

use std::io::Error;

use crate::util::{TestContext, metrics_delta, wait_cache_load, wait_for_cdc};

mod util;

/// Two FILTER predicates on the same table must produce distinct counts
/// through pgcache; verifies the result both before and after a CDC rebuild.
#[tokio::test]
async fn test_filter_aggregate_two_predicates() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE posts (\
            id integer primary key,\
            owneruserid integer not null,\
            posttypeid integer not null\
         )",
        &[],
    )
    .await?;

    // owneruserid=8 has 10 questions and 1 answer; user 99 has 9 noise rows
    // so the WHERE clause actually narrows.
    for id in 1..=10 {
        ctx.query(
            "INSERT INTO posts (id, owneruserid, posttypeid) VALUES ($1, 8, 1)",
            &[&id],
        )
        .await?;
    }
    ctx.query(
        "INSERT INTO posts (id, owneruserid, posttypeid) VALUES (11, 8, 2)",
        &[],
    )
    .await?;
    for id in 12..=20 {
        ctx.query(
            "INSERT INTO posts (id, owneruserid, posttypeid) VALUES ($1, 99, 1)",
            &[&id],
        )
        .await?;
    }

    let sql = "SELECT count(*) FILTER (WHERE posttypeid = 1) AS questions, \
               count(*) FILTER (WHERE posttypeid = 2) AS answers \
               FROM posts WHERE owneruserid = 8";

    // Ground truth from origin.
    let origin = ctx.origin.query_one(sql, &[]).await.map_err(Error::other)?;
    assert_eq!(origin.get::<_, i64>("questions"), 10);
    assert_eq!(origin.get::<_, i64>("answers"), 1);

    // First pgcache query — miss; forwards to origin while population runs.
    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(row.get::<_, i64>("questions"), 10);
    assert_eq!(row.get::<_, i64>("answers"), 1);

    wait_cache_load().await;

    // Second query — cache hit via source-row eval.
    let m1 = ctx.metrics().await?;
    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(
        row.get::<_, i64>("questions"),
        10,
        "FILTER predicate (posttypeid=1) must survive AST round-trip"
    );
    assert_eq!(
        row.get::<_, i64>("answers"),
        1,
        "FILTER predicate (posttypeid=2) must survive AST round-trip"
    );
    let m2 = ctx.metrics().await?;
    let d = metrics_delta(&m1, &m2);
    assert_eq!(d.queries_cache_hit, 1, "expected cache hit on second query");

    // Trigger MV first-build, then wait for it to land.
    let _ = ctx.query_one(sql, &[]).await?;
    wait_cache_load().await;

    let m3 = ctx.metrics().await?;
    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(row.get::<_, i64>("questions"), 10);
    assert_eq!(row.get::<_, i64>("answers"), 1);
    let m4 = ctx.metrics().await?;
    let d = metrics_delta(&m3, &m4);
    assert!(
        d.cache_mv_hits >= 1 || d.cache_mv_fallthrough >= 1,
        "fourth query must be served from the cache (delta: {d:?})"
    );

    ctx.origin_query(
        "INSERT INTO posts (id, owneruserid, posttypeid) VALUES (21, 8, 1)",
        &[],
    )
    .await?;
    wait_for_cdc().await;
    wait_cache_load().await;

    let row = ctx.query_one(sql, &[]).await?;
    assert_eq!(
        row.get::<_, i64>("questions"),
        11,
        "CDC INSERT should advance the FILTER (posttypeid=1) count"
    );
    assert_eq!(
        row.get::<_, i64>("answers"),
        1,
        "CDC INSERT on a posttypeid=1 row must not affect the answers count"
    );

    Ok(())
}
