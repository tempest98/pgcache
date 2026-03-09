#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

use std::io::Error;

use crate::util::{TestContext, http_get, wait_cache_load};

mod util;

/// /healthz always returns 200 OK
#[tokio::test]
async fn test_healthz_returns_ok() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (status, body) = http_get(ctx.metrics_port, "/healthz").await?;
    assert_eq!(status, 200);
    assert_eq!(body, "OK");

    Ok(())
}

/// /readyz returns 200 when the proxy is in normal mode
#[tokio::test]
async fn test_readyz_returns_ok_when_healthy() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (status, body) = http_get(ctx.metrics_port, "/readyz").await?;
    assert_eq!(status, 200);
    assert_eq!(body, "OK");

    Ok(())
}

/// /metrics returns 200 with Prometheus text format
#[tokio::test]
async fn test_metrics_returns_prometheus_format() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (status, body) = http_get(ctx.metrics_port, "/metrics").await?;
    assert_eq!(status, 200);
    // Prometheus text format contains TYPE/HELP comment lines
    assert!(
        body.contains("# TYPE") || body.contains("# HELP") || body.contains("pgcache"),
        "expected Prometheus text format, got: {body}"
    );

    Ok(())
}

/// /status returns 200 with valid JSON containing cache, cdc, and queries fields
#[tokio::test]
async fn test_status_returns_json() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (status, body) = http_get(ctx.metrics_port, "/status").await?;
    assert_eq!(status, 200, "status body: {body}");

    let json: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| Error::other(format!("invalid JSON: {e}\nbody: {body}")))?;

    assert!(json.get("cache").is_some(), "missing 'cache' field");
    assert!(json.get("cdc").is_some(), "missing 'cdc' field");
    assert!(json.get("queries").is_some(), "missing 'queries' field");

    Ok(())
}

/// /status reflects cached queries after they are loaded
#[tokio::test]
async fn test_status_shows_cached_queries() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "CREATE TABLE status_test (id integer PRIMARY KEY, name text)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO status_test (id, name) VALUES (1, 'alice')",
        &[],
    )
    .await?;

    // Cache miss — registers the query
    ctx.simple_query("SELECT id, name FROM status_test WHERE id = 1")
        .await?;
    wait_cache_load().await;

    let (status, body) = http_get(ctx.metrics_port, "/status").await?;
    assert_eq!(status, 200, "status body: {body}");

    let json: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| Error::other(format!("invalid JSON: {e}\nbody: {body}")))?;

    let queries = json["queries"]
        .as_array()
        .expect("queries should be an array");
    assert!(
        !queries.is_empty(),
        "expected at least one cached query in status"
    );

    // Verify query entry has expected fields
    let q = &queries[0];
    assert!(q.get("fingerprint").is_some(), "missing fingerprint");
    assert!(q.get("sql_preview").is_some(), "missing sql_preview");
    assert!(q.get("tables").is_some(), "missing tables");
    assert!(q.get("state").is_some(), "missing state");

    Ok(())
}

/// Unknown paths return 404
#[tokio::test]
async fn test_unknown_path_returns_404() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (status, body) = http_get(ctx.metrics_port, "/nonexistent").await?;
    assert_eq!(status, 404);
    assert_eq!(body, "Not Found");

    Ok(())
}

/// /status cache field contains expected structure
#[tokio::test]
async fn test_status_cache_fields() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (status, body) = http_get(ctx.metrics_port, "/status").await?;
    assert_eq!(status, 200);

    let json: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| Error::other(format!("invalid JSON: {e}\nbody: {body}")))?;

    let cache = &json["cache"];
    assert!(
        cache.get("size_bytes").is_some(),
        "missing cache.size_bytes"
    );
    assert!(
        cache.get("generation").is_some(),
        "missing cache.generation"
    );
    assert!(
        cache.get("tables_tracked").is_some(),
        "missing cache.tables_tracked"
    );
    assert!(cache.get("policy").is_some(), "missing cache.policy");

    Ok(())
}

/// /status cdc field contains expected structure
#[tokio::test]
async fn test_status_cdc_fields() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;

    let (status, body) = http_get(ctx.metrics_port, "/status").await?;
    assert_eq!(status, 200);

    let json: serde_json::Value = serde_json::from_str(&body)
        .map_err(|e| Error::other(format!("invalid JSON: {e}\nbody: {body}")))?;

    let cdc = &json["cdc"];
    assert!(cdc.get("tables").is_some(), "missing cdc.tables");
    assert!(
        cdc.get("last_received_lsn").is_some(),
        "missing cdc.last_received_lsn"
    );
    assert!(
        cdc.get("last_flushed_lsn").is_some(),
        "missing cdc.last_flushed_lsn"
    );
    assert!(cdc.get("lag_bytes").is_some(), "missing cdc.lag_bytes");

    Ok(())
}
