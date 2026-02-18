use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Instant;

use tokio::{
    runtime::Builder,
    sync::{
        mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel},
        oneshot,
    },
    task::{LocalSet, spawn_local},
};
use tokio_postgres::{Config, NoTls};
use tracing::{debug, error, instrument};

use crate::{
    cache::{
        CacheError, CacheResult, MapIntoReport, ReportExt,
        cdc::CdcProcessor,
        messages::{CacheReply, CdcCommand, ProxyMessage},
        query_cache::{QueryCache, QueryRequest, WorkerRequest},
        types::{ActiveRelations, CacheStateView},
        worker::handle_cached_query,
        writer::writer_run,
    },
    metrics::names,
    pg::cache_connection::CacheConnection,
    settings::Settings,
};

const DEFAULT_POOL_SIZE: usize = 8;

/// Handles a proxy message by converting it to a query request and dispatching it
async fn handle_proxy_message(qcache: &mut QueryCache, proxy_msg: ProxyMessage) {
    match proxy_msg.message.into_query_data() {
        Ok(query_data) => {
            let request = QueryRequest {
                query_type: query_data.query_type,
                data: query_data.data,
                cacheable_query: query_data.cacheable_query,
                result_formats: query_data.result_formats,
                client_socket: proxy_msg.client_socket,
                reply_tx: proxy_msg.reply_tx,
                search_path: proxy_msg.search_path,
                timing: proxy_msg.timing,
            };
            if let Err(e) = qcache.query_dispatch(request).await {
                error!("query dispatch failed: {e}");
            }
        }
        Err((e, data)) => {
            debug!("forwarding to origin due to parameter conversion error: {e}");
            // Forward to origin when parameter conversion fails
            let _ = proxy_msg.reply_tx.send(CacheReply::Forward(data));
        }
    }
}

/// Handles a worker request by executing the query and sending the reply
async fn handle_worker_request(
    conn: CacheConnection,
    return_tx: Sender<CacheConnection>,
    mut msg: WorkerRequest,
) {
    debug!("cache worker task spawn");

    // Record worker start time
    msg.timing.worker_start_at = Some(Instant::now());

    let reply = match handle_cached_query(conn, return_tx, &mut msg).await {
        Ok(_) => CacheReply::Complete(msg.data, Some(msg.timing)),
        Err(e) => {
            error!("handle_cached_query failed: {e}");
            CacheReply::Error(msg.data)
        }
    };

    if msg.reply_tx.send(reply).is_err() {
        error!("failed to send reply: no receiver");
    }

    debug!("cache worker task done");
}

/// Creates cache database connections and returns them as a channel pair.
/// Connections are immediately available in the receiver.
async fn connection_pool_create(
    settings: &Settings,
    size: usize,
) -> CacheResult<(Sender<CacheConnection>, Receiver<CacheConnection>)> {
    let (tx, rx) = channel(size);

    for i in 0..size {
        debug!(
            "Creating connection {}/{} to cache db at {}:{}",
            i + 1,
            size,
            settings.cache.host,
            settings.cache.port
        );

        let conn = CacheConnection::connect(&settings.cache)
            .await
            .attach_loc("creating cache connection")?;

        tx.send(conn).await.map_err(|_| CacheError::NoConnection)?;
    }

    debug!("Created {} connections", size);
    Ok((tx, rx))
}

/// Reset the cache database by dropping and recreating it
fn cache_database_reset(settings: &Settings) -> CacheResult<()> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_into_report::<CacheError>()?;

    rt.block_on(async {
        // Connect to postgres maintenance database
        let (admin_client, admin_conn) = Config::new()
            .host(&settings.cache.host)
            .port(settings.cache.port)
            .user(&settings.cache.user)
            .dbname("postgres")
            .connect(NoTls)
            .await
            .map_into_report::<CacheError>()?;

        tokio::spawn(async move {
            if let Err(e) = admin_conn.await {
                error!("admin connection error: {e}");
            }
        });

        let db_name = &settings.cache.database;
        debug!("resetting cache database: {db_name}");

        // Terminate existing connections to the database
        admin_client
            .execute(
                &format!(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                     WHERE datname = '{db_name}' AND pid <> pg_backend_pid()"
                ),
                &[],
            )
            .await
            .map_into_report::<CacheError>()
            .attach_loc("terminating existing connections")?;

        admin_client
            .execute(&format!("DROP DATABASE IF EXISTS {db_name}"), &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("dropping cache database")?;

        admin_client
            .execute(&format!("CREATE DATABASE {db_name}"), &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("creating cache database")?;

        // Connect to fresh cache database and create extension
        let (cache_client, cache_conn) = Config::new()
            .host(&settings.cache.host)
            .port(settings.cache.port)
            .user(&settings.cache.user)
            .dbname(db_name)
            .connect(NoTls)
            .await
            .map_into_report::<CacheError>()?;

        tokio::spawn(async move {
            if let Err(e) = cache_conn.await {
                error!("cache connection error: {e}");
            }
        });

        cache_client
            .execute("CREATE EXTENSION pg_stat_statements", &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("creating pg_stat_statements extension")?;
        cache_client
            .execute("CREATE EXTENSION pgcache_pgrx", &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("creating pgcache_pgrx extension")?;

        Ok(())
    })
}

/// Main cache runtime - handles proxy queries and CDC events
#[instrument(skip_all)]
pub fn cache_run(settings: &Settings, cache_rx: Receiver<ProxyMessage>) -> CacheResult<()> {
    // Reset cache database before starting anything
    cache_database_reset(settings).attach_loc("resetting cache database")?;

    thread::scope(|scope| {
        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_into_report::<CacheError>()?;

        // Create shared state view for coordinator to read cache state
        let state_view = Arc::new(RwLock::new(CacheStateView::default()));

        // Shared set of active relation OIDs (writer writes, CDC reads)
        let active_relations: ActiveRelations =
            Arc::new(RwLock::new(std::collections::HashSet::new()));

        // Spawn writer thread (owns Cache, serializes all mutations)
        // Two channels: one for query registration, one for CDC commands
        let (query_tx, query_rx) = unbounded_channel();
        let (cdc_cmd_tx, cdc_cmd_rx) = unbounded_channel();
        let state_view_writer = Arc::clone(&state_view);
        let active_relations_writer = Arc::clone(&active_relations);
        let settings_writer = settings.clone();
        let _writer_handle = thread::Builder::new()
            .name("cache writer".to_owned())
            .spawn_scoped(scope, move || {
                writer_run(
                    &settings_writer,
                    query_rx,
                    cdc_cmd_rx,
                    state_view_writer,
                    active_relations_writer,
                )
            })
            .map_into_report::<CacheError>()
            .attach_loc("spawning writer thread")?;

        // Spawn worker thread (executes cached queries - read-only)
        let (worker_tx, worker_rx) = unbounded_channel();
        let _worker_handle = thread::Builder::new()
            .name("cache worker".to_owned())
            .spawn_scoped(scope, || worker_run(settings, worker_rx))
            .map_into_report::<CacheError>()
            .attach_loc("spawning worker thread")?;

        // Spawn CDC thread -- sends CdcCommand directly to writer
        let (cdc_exit_tx, mut cdc_exit_rx) = oneshot::channel::<()>();
        let active_relations_cdc = Arc::clone(&active_relations);
        let _cdc_handle = thread::Builder::new()
            .name("cdc worker".to_owned())
            .spawn_scoped(scope, move || {
                // cdc_exit_tx drops when this closure returns, signaling the runtime
                let _cdc_exit = cdc_exit_tx;
                cdc_run(settings, cdc_cmd_tx, active_relations_cdc)
            })
            .map_into_report::<CacheError>()
            .attach_loc("spawning CDC thread")?;

        debug!("cache loop");
        rt.block_on(async {
            let qcache = QueryCache::new(
                settings,
                query_tx.clone(),
                worker_tx,
                Arc::clone(&state_view),
            )
            .await
            .attach_loc("creating query cache")?;

            LocalSet::new()
                .run_until(async move {
                    let mut cache_rx = cache_rx;
                    loop {
                        // Block until at least one message arrives
                        tokio::select! {
                            _ = query_tx.closed() => {
                                error!("writer thread exited unexpectedly");
                                return Err(CacheError::WriterFailure.into());
                            }
                            _ = &mut cdc_exit_rx => {
                                error!("CDC thread exited unexpectedly");
                                return Err(CacheError::CdcFailure.into());
                            }
                            msg = cache_rx.recv() => {
                                match msg {
                                    Some(proxy_msg) => {
                                        let mut qcache = qcache.clone();
                                        spawn_local(async move {
                                            handle_proxy_message(&mut qcache, proxy_msg).await;
                                        });
                                    }
                                    None => {
                                        debug!("proxy channel closed");
                                        break;
                                    }
                                }
                            }
                        }

                        metrics::gauge!(names::CACHE_PROXY_MESSAGE_QUEUE)
                            .set(cache_rx.len() as f64);
                    }

                    debug!("cache loop exiting");
                    Ok(())
                })
                .await
        })
    })
}

/// Worker runtime - executes cached queries against the database
fn worker_run(
    settings: &Settings,
    mut worker_rx: UnboundedReceiver<WorkerRequest>,
) -> CacheResult<()> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_into_report::<CacheError>()?;

    debug!("worker loop");
    rt.block_on(async {
        let (conn_tx, mut conn_rx) = connection_pool_create(settings, DEFAULT_POOL_SIZE)
            .await
            .attach_loc("creating connection pool")?;

        LocalSet::new()
            .run_until(async move {
                loop {
                    // Block for at least one request (track wait time)
                    let wait_start = Instant::now();
                    let Some(msg) = worker_rx.recv().await else {
                        break;
                    };
                    metrics::histogram!(names::CACHE_WORKER_WAIT_SECONDS)
                        .record(wait_start.elapsed().as_secs_f64());

                    // Wait for an available connection (track wait time only when blocking)
                    let conn = if let Ok(conn) = conn_rx.try_recv() {
                        conn
                    } else {
                        let conn_wait_start = Instant::now();
                        let Some(conn) = conn_rx.recv().await else {
                            return Err(CacheError::NoConnection.into());
                        };
                        metrics::histogram!(names::CACHE_WORKER_CONN_WAIT_SECONDS)
                            .record(conn_wait_start.elapsed().as_secs_f64());
                        conn
                    };

                    // Spawn task with both the request and connection
                    let return_tx = conn_tx.clone();
                    spawn_local(async move {
                        handle_worker_request(conn, return_tx, msg).await;
                    });

                    metrics::gauge!(names::CACHE_WORKER_QUEUE).set(worker_rx.len() as f64);
                }

                Ok(())
            })
            .await
    })
}

/// CDC runtime - processes change data capture events.
/// The CDC processor should run indefinitely, so any exit is considered a failure.
fn cdc_run(
    settings: &Settings,
    cdc_tx: UnboundedSender<CdcCommand>,
    active_relations: ActiveRelations,
) -> CacheResult<()> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_into_report::<CacheError>()?;

    debug!("cdc loop");
    rt.block_on(async {
        let mut cdc = CdcProcessor::new(settings, cdc_tx, active_relations)
            .await
            .attach_loc("initializing CDC processor")?;

        // CDC should run forever - any return (Ok or Err) is unexpected
        match cdc.run().await {
            Ok(()) => {
                error!("cdc.run() exited unexpectedly without error");
            }
            Err(e) => {
                error!("cdc.run() failed: {e}");
            }
        }

        Err(CacheError::CdcFailure.into())
    })
}
