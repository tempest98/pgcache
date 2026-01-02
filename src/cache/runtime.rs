use std::sync::{Arc, RwLock};
use std::thread;

use tokio::{
    runtime::Builder,
    sync::mpsc::{Receiver, UnboundedReceiver, UnboundedSender},
    task::{LocalSet, spawn_local},
};
use tokio_stream::{StreamExt, wrappers::ReceiverStream, wrappers::UnboundedReceiverStream};
use tracing::{debug, error, instrument};

use crate::{
    cache::{
        CacheError,
        cdc::CdcProcessor,
        messages::{CacheReply, CdcMessage, ProxyMessage, StreamSource, WriterCommand},
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
        CdcMessage::Insert(relation_oid, row_data) => {
            WriterCommand::CdcInsert {
                relation_oid,
                row_data,
            }
        }
        CdcMessage::Update(update) => WriterCommand::CdcUpdate {
            relation_oid: update.relation_oid,
            key_data: update.key_data,
            row_data: update.row_data,
        },
        CdcMessage::Delete(relation_oid, row_data) => {
            WriterCommand::CdcDelete {
                relation_oid,
                row_data,
            }
        }
        CdcMessage::Truncate(relation_oids) => WriterCommand::CdcTruncate { relation_oids },
        CdcMessage::RelationCheck(relation_oid, reply_tx) => {
            WriterCommand::RelationCheck {
                relation_oid,
                response_tx: reply_tx,
            }
        }
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

/// Main cache runtime - handles proxy queries and CDC events
#[instrument(skip_all)]
pub fn cache_run(settings: &Settings, cache_rx: Receiver<ProxyMessage>) -> Result<(), CacheError> {
    thread::scope(|scope| {
        let rt = Builder::new_current_thread().enable_all().build()?;

        // Create shared state view for coordinator to read cache state
        let state_view = Arc::new(RwLock::new(CacheStateView::default()));

        // Spawn writer thread (owns Cache, serializes all mutations)
        let (writer_tx, writer_rx) = tokio::sync::mpsc::unbounded_channel();
        let state_view_writer = Arc::clone(&state_view);
        let settings_writer = settings.clone();
        let _writer_handle = thread::Builder::new()
            .name("cache writer".to_owned())
            .spawn_scoped(scope, move || writer_run(&settings_writer, writer_rx, state_view_writer))?;

        // Spawn worker thread (executes cached queries - read-only)
        let (worker_tx, worker_rx) = tokio::sync::mpsc::unbounded_channel();
        let _worker_handle = thread::Builder::new()
            .name("cache worker".to_owned())
            .spawn_scoped(scope, || worker_run(settings, worker_rx))?;

        // Spawn CDC thread
        let (cdc_tx, cdc_rx) = tokio::sync::mpsc::unbounded_channel();
        let cdc_handle = thread::Builder::new()
            .name("cdc worker".to_owned())
            .spawn_scoped(scope, move || cdc_run(settings, cdc_tx))?;

        let cache_rx_mapped = ReceiverStream::new(cache_rx).map(StreamSource::Proxy);
        let cdc_rx_mapped = UnboundedReceiverStream::new(cdc_rx).map(StreamSource::Cdc);

        let mut stream = cache_rx_mapped.merge(cdc_rx_mapped);

        debug!("cache loop");
        rt.block_on(async {
            let qcache = QueryCache::new(settings, writer_tx.clone(), worker_tx, Arc::clone(&state_view)).await?;

            LocalSet::new()
                .run_until(async move {
                    while let Some(src) = stream.next().await {
                        match src {
                            StreamSource::Proxy(proxy_msg) => {
                                let mut qcache = qcache.clone();
                                spawn_local(async move {
                                    handle_proxy_message(&mut qcache, proxy_msg).await;
                                });
                            }
                            StreamSource::Cdc(msg) => {
                                // Forward CDC messages directly to writer
                                handle_cdc_message(&writer_tx, msg);
                            }
                        }

                        if cdc_handle.is_finished() {
                            return Err(CacheError::CdcFailure);
                        }
                    }

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
) -> Result<(), CacheError> {
    let rt = Builder::new_current_thread().enable_all().build()?;

    debug!("worker loop");
    rt.block_on(async {
        let worker = CacheWorker::new(settings).await?;

        LocalSet::new()
            .run_until(async move {
                while let Some(msg) = worker_rx.recv().await {
                    let worker = worker.clone();
                    spawn_local(async move {
                        handle_worker_request(worker, msg).await;
                    });
                }

                Ok(())
            })
            .await
    })
}

/// CDC runtime - processes change data capture events.
/// The CDC processor should run indefinitely, so any exit is considered a failure.
fn cdc_run(settings: &Settings, cdc_tx: UnboundedSender<CdcMessage>) -> Result<(), CacheError> {
    let rt = Builder::new_current_thread().enable_all().build()?;

    debug!("cdc loop");
    rt.block_on(async {
        let mut cdc = CdcProcessor::new(settings, cdc_tx).await?;

        // CDC should run forever - any return (Ok or Err) is unexpected
        match cdc.run().await {
            Ok(()) => {
                error!("cdc.run() exited unexpectedly without error");
            }
            Err(e) => {
                error!("cdc.run() failed: {e}");
            }
        }

        Err(CacheError::CdcFailure)
    })
}
