#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

use std::io::Error;

use crate::util::{
    TestContext, assert_cache_hit, assert_cache_miss, assert_row_at, extract_row, wait_cache_load,
};

mod util;

/// Test that SELECT * returns columns in the same order from cache as from origin.
///
/// Regression test: BiHashMap iteration order is hash-based, not position-based,
/// so star expansion during query resolution could return columns in wrong order.
#[tokio::test]
async fn test_select_star_column_order() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Use enough columns that hash order is very likely to differ from position order
    ctx.query(
        "create table test_col_order (
            id integer primary key,
            alpha text,
            bravo text,
            charlie text,
            delta text,
            echo text,
            foxtrot text
        )",
        &[],
    )
    .await?;

    ctx.query(
        "insert into test_col_order (id, alpha, bravo, charlie, delta, echo, foxtrot)
         values (1, 'a', 'b', 'c', 'd', 'e', 'f')",
        &[],
    )
    .await?;

    let expected_columns = [
        "id", "alpha", "bravo", "charlie", "delta", "echo", "foxtrot",
    ];

    // First query — cache miss, served from origin
    let m = ctx.metrics().await?;
    let res = ctx
        .simple_query("select * from test_col_order where id = 1")
        .await?;

    let origin_row = extract_row(&res, 1)?;
    let origin_columns: Vec<&str> = origin_row.columns().iter().map(|c| c.name()).collect();
    assert_eq!(
        origin_columns, expected_columns,
        "Origin should return columns in table definition order"
    );
    assert_row_at(
        &res,
        1,
        &[
            ("id", "1"),
            ("alpha", "a"),
            ("bravo", "b"),
            ("charlie", "c"),
            ("delta", "d"),
            ("echo", "e"),
            ("foxtrot", "f"),
        ],
    )?;
    let m = assert_cache_miss(&mut ctx, m).await?;

    wait_cache_load().await;

    // Second query — cache hit, served from cache
    let res = ctx
        .simple_query("select * from test_col_order where id = 1")
        .await?;

    let cache_row = extract_row(&res, 1)?;
    let cache_columns: Vec<&str> = cache_row.columns().iter().map(|c| c.name()).collect();
    assert_eq!(
        cache_columns, expected_columns,
        "Cache should return columns in same order as origin (table definition order)"
    );
    assert_row_at(
        &res,
        1,
        &[
            ("id", "1"),
            ("alpha", "a"),
            ("bravo", "b"),
            ("charlie", "c"),
            ("delta", "d"),
            ("echo", "e"),
            ("foxtrot", "f"),
        ],
    )?;
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}
