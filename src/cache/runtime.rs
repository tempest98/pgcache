use std::sync::{Arc, RwLock};
use std::thread;

use tokio::{
    runtime::Builder,
    sync::mpsc::{Receiver, UnboundedReceiver, UnboundedSender},
    task::{LocalSet, spawn_local},
};
use tokio_postgres::{Config, NoTls};
use tracing::{debug, error, instrument};

use crate::{
    cache::{
        CacheError, CacheResult, MapIntoReport, ReportExt,
        cdc::CdcProcessor,
        messages::{CacheReply, CdcMessage, ProxyMessage, WriterCommand},
        query_cache::{QueryCache, QueryRequest, WorkerRequest},
        types::CacheStateView,
        worker::CacheWorker,
        writer::writer_run,
    },
    settings::Settings,
};

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
            };
            if let Err(e) = qcache.query_dispatch(request).await {
                error!("query dispatch failed: {e}");
            }
        }
        Err((e, data)) => {
            debug!("forwarding to origin due to parameter conversion error: {e}");
            // Forward to origin when parameter conversion fails
            let _ = proxy_msg.reply_tx.send(CacheReply::Forward(data)).await;
        }
    }
}

/// Handles a CDC message by forwarding to the writer thread
fn handle_cdc_message(writer_tx: &UnboundedSender<WriterCommand>, msg: CdcMessage) {
    let cmd = match msg {
        CdcMessage::Register(table_metadata) => WriterCommand::TableRegister(table_metadata),
        CdcMessage::Insert(relation_oid, row_data) => WriterCommand::CdcInsert {
            relation_oid,
            row_data,
        },
        CdcMessage::Update(update) => WriterCommand::CdcUpdate {
            relation_oid: update.relation_oid,
            key_data: update.key_data,
            row_data: update.row_data,
        },
        CdcMessage::Delete(relation_oid, row_data) => WriterCommand::CdcDelete {
            relation_oid,
            row_data,
        },
        CdcMessage::Truncate(relation_oids) => WriterCommand::CdcTruncate { relation_oids },
        CdcMessage::RelationCheck(relation_oid, reply_tx) => WriterCommand::RelationCheck {
            relation_oid,
            response_tx: reply_tx,
        },
    };

    if let Err(e) = writer_tx.send(cmd) {
        error!("failed to send to writer: {e}");
    }
}

/// Handles a worker request by executing the query and sending the reply
async fn handle_worker_request(worker: CacheWorker, mut msg: WorkerRequest) {
    debug!("cache worker task spawn");

    let reply = match worker.handle_cached_query(&mut msg).await {
        Ok(_) => CacheReply::Complete(msg.data),
        Err(e) => {
            error!("handle_cached_query failed: {e}");
            CacheReply::Error(msg.data)
        }
    };

    if msg.reply_tx.send(reply).await.is_err() {
        error!("failed to send reply: no receiver");
    }

    debug!("cache worker task done");
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

        // Spawn writer thread (owns Cache, serializes all mutations)
        let (writer_tx, writer_rx) = tokio::sync::mpsc::unbounded_channel();
        let state_view_writer = Arc::clone(&state_view);
        let settings_writer = settings.clone();
        let _writer_handle = thread::Builder::new()
            .name("cache writer".to_owned())
            .spawn_scoped(scope, move || {
                writer_run(&settings_writer, writer_rx, state_view_writer)
            })
            .map_into_report::<CacheError>()
            .attach_loc("spawning writer thread")?;

        // Spawn worker thread (executes cached queries - read-only)
        let (worker_tx, worker_rx) = tokio::sync::mpsc::unbounded_channel();
        let _worker_handle = thread::Builder::new()
            .name("cache worker".to_owned())
            .spawn_scoped(scope, || worker_run(settings, worker_rx))
            .map_into_report::<CacheError>()
            .attach_loc("spawning worker thread")?;

        // Spawn CDC thread
        let (cdc_tx, mut cdc_rx) = tokio::sync::mpsc::unbounded_channel();
        let _cdc_handle = thread::Builder::new()
            .name("cdc worker".to_owned())
            .spawn_scoped(scope, move || cdc_run(settings, cdc_tx))
            .map_into_report::<CacheError>()
            .attach_loc("spawning CDC thread")?;

        debug!("cache loop");
        rt.block_on(async {
            let qcache = QueryCache::new(
                settings,
                writer_tx.clone(),
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
                            _ = writer_tx.closed() => {
                                error!("writer thread exited unexpectedly");
                                return Err(CacheError::WriterFailure.into());
                            }
                            msg = cdc_rx.recv() => {
                                match msg {
                                    Some(msg) => handle_cdc_message(&writer_tx, msg),
                                    None => {
                                        error!("CDC channel closed unexpectedly");
                                        return Err(CacheError::CdcFailure.into());
                                    }
                                }
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

                        // Drain all immediately available messages without parking
                        while let Ok(msg) = cdc_rx.try_recv() {
                            handle_cdc_message(&writer_tx, msg);
                        }
                        while let Ok(proxy_msg) = cache_rx.try_recv() {
                            let mut qcache = qcache.clone();
                            spawn_local(async move {
                                handle_proxy_message(&mut qcache, proxy_msg).await;
                            });
                        }
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
        let worker = CacheWorker::new(settings)
            .await
            .attach_loc("creating cache worker")?;

        LocalSet::new()
            .run_until(async move {
                loop {
                    // Block for at least one message
                    let Some(msg) = worker_rx.recv().await else {
                        break;
                    };

                    // Process first message
                    let w = worker.clone();
                    spawn_local(async move {
                        handle_worker_request(w, msg).await;
                    });

                    // Drain all immediately available messages without parking
                    while let Ok(msg) = worker_rx.try_recv() {
                        let w = worker.clone();
                        spawn_local(async move {
                            handle_worker_request(w, msg).await;
                        });
                    }
                }

                Ok(())
            })
            .await
    })
}

/// CDC runtime - processes change data capture events.
/// The CDC processor should run indefinitely, so any exit is considered a failure.
fn cdc_run(settings: &Settings, cdc_tx: UnboundedSender<CdcMessage>) -> CacheResult<()> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_into_report::<CacheError>()?;

    debug!("cdc loop");
    rt.block_on(async {
        let mut cdc = CdcProcessor::new(settings, cdc_tx)
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
