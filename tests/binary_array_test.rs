//! End-to-end regression test for **PGC-103**.
//!
//! Tokio-postgres encodes `Vec<i32>` bindings as binary `int4[]` by
//! default, and the previous catch-all UTF-8 fallback in
//! `binary_parameter_to_literal` silently corrupted the deparsed SQL
//! whenever the array header bytes (mostly NUL) happened to be valid
//! UTF-8. The fix decodes binary arrays into a proper PG text array
//! literal (`'{1,3}'::int4[]`) so the query is fully cacheable.
//!
//! This test exercises the full proxy path: tokio-postgres on the client
//! side → pgcache parameter substitution → origin → cache population →
//! second-call cache hit. The unit-level companions live in
//! `src/query/transform/parameters.rs::tests::test_binary_int4_array_*`.

use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, wait_cache_load};

mod util;

/// `WHERE id = ANY($1)` with a binary `int4[]` parameter — the exact path
/// PGC-103 was about. First call populates, second call hits cache.
/// Before the fix this either errored with "error encoding message to
/// server" or returned wrong rows; both are visible end-to-end in the
/// assertions below.
#[tokio::test]
async fn test_binary_int4_array_in_any_clause() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table widgets (id integer primary key, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "insert into widgets (id, name) \
         values (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')",
        &[],
    )
    .await?;

    let stmt = ctx
        .prepare("select id, name from widgets where id = any($1) order by id")
        .await?;

    // tokio-postgres binds Vec<i32> as binary int4[] (oid 1007).
    let ids: Vec<i32> = vec![1, 3];

    let m = ctx.metrics().await?;
    let rows = ctx.query(&stmt, &[&ids]).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[0].get::<_, &str>("name"), "a");
    assert_eq!(rows[1].get::<_, i32>("id"), 3);
    assert_eq!(rows[1].get::<_, &str>("name"), "c");
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // Second execution with the same array — same fingerprint, cache hit.
    let rows = ctx.query(&stmt, &[&ids]).await?;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>("id"), 1);
    assert_eq!(rows[1].get::<_, i32>("id"), 3);
    let _ = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}
