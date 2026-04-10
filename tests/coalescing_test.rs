#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

use std::io::Error;

use crate::util::{TestContext, metrics_delta, wait_cache_load, wait_for_cdc};

mod util;

/// Test that concurrent queries for the same fingerprint during Loading are coalesced.
///
/// Fires N concurrent queries while population is in progress. At least some should
/// be coalesced (queued during Loading, then served from cache when Ready arrives)
/// rather than individually forwarded to origin.
#[tokio::test]
async fn test_request_coalescing() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    // Create a table with enough rows that population takes measurable time
    ctx.query(
        "CREATE TABLE coalesce_test (id INTEGER PRIMARY KEY, data TEXT)",
        &[],
    )
    .await?;

    let mut values = Vec::new();
    for i in 1..=500 {
        values.push(format!("({i}, 'row_{i}')"));
    }
    let insert_sql = format!(
        "INSERT INTO coalesce_test (id, data) VALUES {}",
        values.join(", ")
    );
    ctx.query(&insert_sql as &str, &[]).await?;

    wait_for_cdc().await;

    // Open multiple proxy connections before firing queries
    let num_clients = 10;
    let mut clients = Vec::with_capacity(num_clients);
    for _ in 0..num_clients {
        clients.push(ctx.proxy_client_connect().await?);
    }

    let m_before = ctx.metrics().await?;

    // Fire all queries concurrently — the first to arrive triggers Loading,
    // the rest should be coalesced while population is in progress.
    let query_str = "SELECT id, data FROM coalesce_test WHERE id <= 100";
    let mut handles = Vec::with_capacity(num_clients);
    for client in clients {
        let q = query_str.to_owned();
        handles.push(tokio::spawn(async move {
            client.simple_query(&q).await
        }));
    }

    // Wait for all queries to complete
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "concurrent query failed: {result:?}");
    }

    // Wait for population to finish and metrics to settle
    wait_cache_load().await;

    let m_after = ctx.metrics().await?;
    let delta = metrics_delta(&m_before, &m_after);

    // All N queries should have been processed
    assert_eq!(
        delta.queries_cacheable, num_clients as u64,
        "all queries should be cacheable"
    );

    // At least some queries should have been coalesced (served from cache via the
    // waiting queue rather than individually forwarded to origin). The exact number
    // depends on timing — we can't guarantee all N-1 are coalesced, but with 10
    // concurrent queries and a 500-row population, we should reliably get some.
    assert!(
        delta.cache_coalesce_served > 0,
        "expected some coalesced requests, got 0 (delta: {delta:?})"
    );

    // Coalesced queries are cache hits (served from cache after population)
    assert!(
        delta.queries_cache_hit > 0,
        "expected cache hits from coalesced requests"
    );

    // Only 1 query should be a cache miss (the first one that triggered population)
    assert_eq!(
        delta.queries_cache_miss, 1,
        "only the first query should be a cache miss"
    );

    Ok(())
}
