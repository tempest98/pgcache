#![allow(dead_code)]

use std::collections::HashSet;
use std::time::{Duration, Instant, UNIX_EPOCH};

use iddqd::BiHashMap;
use postgres_replication::{
    LogicalReplicationStream,
    protocol::{
        BeginBody, CommitBody, DeleteBody, InsertBody, LogicalReplicationMessage, OriginBody,
        PrimaryKeepAliveBody, RelationBody, ReplicationMessage, TruncateBody, TypeBody, UpdateBody,
        XLogDataBody,
    },
};
use postgres_types::PgLsn;
use tokio::{
    pin,
    time::{Interval, interval},
};
use tokio_postgres::config::ReplicationMode;
use tokio_postgres::{Client, Config, Error, NoTls};
use tokio_stream::StreamExt;
use tokio_util::bytes::Bytes;

use tokio::sync::{mpsc::UnboundedSender, oneshot};
use tracing::{debug, error};

use crate::{
    catalog::{ColumnMetadata, TableMetadata},
    settings::Settings,
};

use super::{
    CacheError,
    messages::{CdcMessage, CdcMessageUpdate},
};

/// Handles Change Data Capture (CDC) processing from PostgreSQL logical replication.
/// Processes replication messages and synchronizes changes with the cache database.
pub struct CdcProcessor {
    cdc_client: Client,
    publication_name: String,
    slot_name: String,

    cdc_tx: UnboundedSender<CdcMessage>,
    active_relations: HashSet<u32>,

    last_received_lsn: u64,
    last_applied_lsn: u64,
    keep_alive_timer: Interval,
    last_keep_alive_sent: Option<Instant>,
    keep_alive_sent_count: u64,
}

impl CdcProcessor {
    /// Creates a new CdcProcessor with the provided CDC client and cache.
    pub async fn new(
        settings: &Settings,
        cdc_tx: UnboundedSender<CdcMessage>,
    ) -> Result<Self, CacheError> {
        let (origin_cdc_client, origin_cdc_connection) = Config::new()
            .host(&settings.origin.host)
            .port(settings.origin.port)
            .user(&settings.origin.user)
            .dbname(&settings.origin.database)
            .replication_mode(ReplicationMode::Logical)
            .connect(NoTls)
            .await?;

        //task to process connection to cache pg db
        tokio::spawn(async move {
            if let Err(e) = origin_cdc_connection.await {
                error!("connection error: {e}");
            }
        });

        // Create keep-alive timer with 30-second interval (hardcoded for POC)
        let mut timer = interval(Duration::from_secs(30));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        Ok(Self {
            cdc_client: origin_cdc_client,
            publication_name: settings.cdc.publication_name.clone(),
            slot_name: settings.cdc.slot_name.clone(),
            cdc_tx,
            active_relations: HashSet::new(),
            last_received_lsn: 0,
            last_applied_lsn: 0,
            keep_alive_timer: timer,
            last_keep_alive_sent: None,
            keep_alive_sent_count: 0,
        })
    }

    /// Starts the CDC replication stream and processes incoming messages.
    /// Uses tokio::select! for concurrent message processing and periodic keep-alives.
    pub async fn run(&mut self) -> Result<(), Error> {
        // Start replication stream
        let slot = self.slot_name.as_str();
        let publ = self.publication_name.as_str();
        let query = format!(
            "START_REPLICATION SLOT {slot} LOGICAL 0/0 (proto_version '4', publication_names '{publ}')"
        );

        let copy_stream = self.cdc_client.copy_both_simple::<Bytes>(&query).await?;

        let stream = LogicalReplicationStream::new(copy_stream);
        pin!(stream);

        debug!("Starting CDC replication stream with keep-alive intervals");

        loop {
            tokio::select! {
                // Handle incoming replication messages
                msg_result = stream.next() => {
                    match msg_result {
                        Some(Ok(msg)) => {
                            if let Err(e) = self.process_replication_message(msg, stream.as_mut()).await {
                                error!("Error processing replication message: {}", e);
                                // Continue processing despite errors
                            }
                        }
                        Some(Err(e)) => {
                            error!("Replication stream error: {}", e);
                            return Err(e);
                        }
                        None => {
                            debug!("Replication stream ended");
                            break;
                        }
                    }
                }
                // Handle periodic keep-alive timer
                _ = self.keep_alive_timer.tick() => {
                    debug!("Keep-alive timer tick - sending periodic status update");
                    if let Err(e) = self.send_standby_status_update(stream.as_mut(), false).await {
                        error!("Error sending periodic keep-alive: {}", e);
                        // Continue despite keep-alive errors (graceful degradation)
                    }
                }
            }
        }

        Ok(())
    }

    /// Updates the last received LSN from XLogData message.
    async fn update_lsn(&mut self, xlog_data: &XLogDataBody<LogicalReplicationMessage>) {
        let lsn = xlog_data.wal_start();
        self.last_received_lsn = lsn;
    }

    /// Marks the current LSN as fully applied after successful processing.
    fn mark_lsn_applied(&mut self) {
        let received = self.last_received_lsn;
        self.last_applied_lsn = received;
    }

    /// Gets the current PostgreSQL timestamp in microseconds since PostgreSQL epoch (2000-01-01).
    fn get_pg_timestamp() -> i64 {
        let pg_epoch = UNIX_EPOCH + Duration::from_secs(946_684_800); // PostgreSQL epoch: 2000-01-01
        pg_epoch.elapsed().unwrap_or(Duration::ZERO).as_micros() as i64
    }

    /// Sends a standby status update to PostgreSQL with current LSN progress.
    /// Returns Ok(()) on success, logs warnings on failure and continues gracefully.
    async fn send_standby_status_update(
        &mut self,
        stream: std::pin::Pin<&mut LogicalReplicationStream>,
        reply_requested: bool,
    ) -> Result<(), Error> {
        let applied_lsn = self.last_applied_lsn;
        let pg_lsn = PgLsn::from(applied_lsn);
        let timestamp = Self::get_pg_timestamp();
        let reply_flag = if reply_requested { 1 } else { 0 };

        // Send standby status update: write_lsn, flush_lsn, apply_lsn all set to applied_lsn
        match stream
            .standby_status_update(pg_lsn, pg_lsn, pg_lsn, timestamp, reply_flag)
            .await
        {
            Ok(()) => {
                // Update keep-alive tracking on successful send
                self.last_keep_alive_sent = Some(Instant::now());
                let count = {
                    let mut counter = self.keep_alive_sent_count;
                    counter += 1;
                    counter
                };

                // Debug logging for successful keep-alive
                debug!("Sent keep-alive #{count} (LSN: {applied_lsn}, reply: {reply_flag})");

                Ok(())
            }
            Err(e) => {
                // Log warning but don't terminate CDC processing for keep-alive failures
                error!(
                    "Warning: Failed to send standby status update (LSN: {applied_lsn}, reply: {reply_flag}): {e}"
                );
                error!("CDC processing will continue despite keep-alive failure");
                Ok(()) // Return Ok to continue processing
            }
        }
    }

    /// Handles incoming keep-alive messages and sends appropriate responses.
    /// Uses graceful error handling - logs warnings but continues processing.
    async fn handle_keep_alive(
        &mut self,
        keep_alive: &PrimaryKeepAliveBody,
        stream: std::pin::Pin<&mut LogicalReplicationStream>,
    ) -> Result<(), Error> {
        // Check if PostgreSQL requested a reply
        let reply_requested = keep_alive.reply() == 1;

        debug!(
            "Received keep-alive from PostgreSQL (wal_end: {}, reply_requested: {})",
            keep_alive.wal_end(),
            reply_requested
        );

        if reply_requested {
            debug!("PostgreSQL requested immediate keep-alive response");
            // Send immediate standby status update response
            // Note: send_standby_status_update already handles errors gracefully
            self.send_standby_status_update(stream, false).await?;
        }

        Ok(())
    }

    /// Processes a single replication message.
    async fn process_replication_message(
        &mut self,
        msg: ReplicationMessage<LogicalReplicationMessage>,
        stream: std::pin::Pin<&mut LogicalReplicationStream>,
    ) -> Result<(), Error> {
        match msg {
            ReplicationMessage::XLogData(xlog_data) => {
                self.update_lsn(&xlog_data).await;
                let result = match xlog_data.into_data() {
                    LogicalReplicationMessage::Begin(body) => self.process_begin(&body).await,
                    LogicalReplicationMessage::Commit(body) => self.process_commit(&body).await,
                    LogicalReplicationMessage::Origin(body) => self.process_origin(&body).await,
                    LogicalReplicationMessage::Relation(body) => self.process_relation(&body).await,
                    LogicalReplicationMessage::Type(body) => self.process_type(&body).await,
                    LogicalReplicationMessage::Insert(body) => self.process_insert(&body).await,
                    LogicalReplicationMessage::Update(body) => self.process_update(&body).await,
                    LogicalReplicationMessage::Delete(body) => self.process_delete(&body).await,
                    LogicalReplicationMessage::Truncate(body) => self.process_truncate(&body).await,
                    _ => {
                        println!("Unhandled replication message type");
                        Ok(())
                    }
                };
                if result.is_ok() {
                    self.mark_lsn_applied();
                }
                result
            }
            ReplicationMessage::PrimaryKeepAlive(keep_alive) => {
                self.handle_keep_alive(&keep_alive, stream).await
            }
            _ => {
                error!("ReplicationMessage error: {:?}", &msg);
                Ok(())
            }
        }
    }

    /// Gets keep-alive statistics for debugging purposes.
    fn get_keep_alive_stats(&self) -> (Option<Instant>, u64) {
        let last_sent = self.last_keep_alive_sent;
        let count = self.keep_alive_sent_count;
        (last_sent, count)
    }

    /// Processes transaction begin messages.
    async fn process_begin(&self, _body: &BeginBody) -> Result<(), Error> {
        // transaction begin and end messages can be ignored since all logical replication
        // messages are sent after the transaction has been committed
        Ok(())
    }

    /// Processes transaction commit messages.
    async fn process_commit(&self, _body: &CommitBody) -> Result<(), Error> {
        // transaction begin and end messages can be ignored since all logical replication
        // messages are sent after the transaction has been committed
        Ok(())
    }

    /// Processes origin messages.
    async fn process_origin(&self, _body: &OriginBody) -> Result<(), Error> {
        // origin messages are not relevant to the cache
        Ok(())
    }

    /// Processes relation (table schema) messages.
    async fn process_relation(&self, body: &RelationBody) -> Result<(), Error> {
        // Parse RelationBody into TableMetadata
        let table_metadata = self.parse_relation_to_table_metadata(body);

        // Register table metadata in cache
        if let Err(e) = self.cdc_tx.send(CdcMessage::Register(table_metadata)) {
            //todo, halt use of cache and fallback to proxy only mode
            error!("Failed to register table from CDC: {e:?}");
        }

        Ok(())
    }

    /// Processes type definition messages.
    async fn process_type(&self, _body: &TypeBody) -> Result<(), Error> {
        Ok(())
    }

    /// Processes insert messages with query-aware filtering.
    async fn process_insert(&mut self, body: &InsertBody) -> Result<(), Error> {
        debug!("-----------process_insert");
        let relation_oid = body.rel_id();

        // Check if there are any cached queries for this table
        if !self.is_relation_active(relation_oid).await {
            return Ok(()); // No cached queries for this table
        }

        // Parse row data from InsertBody
        let row_data = self.parse_insert_row_data(body)?;

        // Let cache handle the insert with query-aware filtering
        if let Err(e) = self.cdc_tx.send(CdcMessage::Insert(relation_oid, row_data)) {
            //todo, halt use of cache and fallback to proxy only mode
            error!("Failed to handle INSERT for relation {relation_oid}: {e:?}");
        }

        Ok(())
    }

    /// Processes update messages with query-aware filtering.
    async fn process_update(&mut self, body: &UpdateBody) -> Result<(), Error> {
        let relation_oid = body.rel_id();

        // Check if there are any cached queries for this table
        if !self.is_relation_active(relation_oid).await {
            return Ok(()); // No cached queries for this table
        }

        // Parse old and new row data from UpdateBody
        let (key_data, new_row_data) = self.parse_update_row_data(body)?;

        // Let cache handle the update with query-aware filtering
        if let Err(e) = self.cdc_tx.send(CdcMessage::Update(CdcMessageUpdate {
            relation_oid,
            key_data,
            row_data: new_row_data,
        })) {
            //todo, halt use of cache and fallback to proxy only mode
            error!("Failed to handle UPDATE for relation {relation_oid}: {e:?}");
        }

        Ok(())
    }

    /// Processes delete messages with query-aware filtering.
    async fn process_delete(&mut self, body: &DeleteBody) -> Result<(), Error> {
        let relation_oid = body.rel_id();

        // Check if there are any cached queries for this table
        if !self.is_relation_active(relation_oid).await {
            return Ok(()); // No cached queries for this table
        }

        // Parse row data from DeleteBody
        let row_data = self.parse_delete_row_data(body)?;

        // Let cache handle the delete with query-aware filtering
        if let Err(e) = self.cdc_tx.send(CdcMessage::Delete(relation_oid, row_data)) {
            //todo, halt use of cache and fallback to proxy only mode
            error!("Failed to handle DELETE for relation {relation_oid}: {e:?}");
        }

        Ok(())
    }

    /// Processes truncate messages.
    async fn process_truncate(&mut self, body: &TruncateBody) -> Result<(), Error> {
        let mut ids: Vec<u32> = Vec::with_capacity(body.rel_ids().len());
        for &id in body.rel_ids() {
            if self.is_relation_active(id).await {
                ids.push(id);
            }
        }
        if let Err(e) = self.cdc_tx.send(CdcMessage::Truncate(ids)) {
            //todo, halt use of cache and fallback to proxy only mode
            error!("Failed to handle truncate from CDC: {e:?}");
        }
        Ok(())
    }

    /// Parse RelationBody into TableMetadata for cache registration.
    fn parse_relation_to_table_metadata(&self, relation_body: &RelationBody) -> TableMetadata {
        let relation_oid = relation_body.rel_id();
        let table_name = relation_body.name().unwrap_or("unknown_table").to_owned();
        let schema_name = relation_body
            .namespace()
            .unwrap_or("unknown_schema")
            .to_owned();

        // Build column metadata from relation body
        let mut columns = BiHashMap::new();
        let mut primary_key_columns = Vec::new();

        for (idx, column) in relation_body.columns().iter().enumerate() {
            let is_primary_key = column.flags() == 1; // flags field is 1 when column is part of primary key

            let type_oid = column.type_id() as u32; // Convert i32 to u32
            let data_type = tokio_postgres::types::Type::from_oid(type_oid)
                .unwrap_or(tokio_postgres::types::Type::TEXT); // Fallback for unknown types

            let type_name = data_type.name().to_owned(); // Get the type name from tokio_postgres Type

            let column_metadata = ColumnMetadata {
                name: column.name().unwrap_or("unknown_column").to_owned(),
                position: (idx + 1) as i16, // PostgreSQL columns are 1-indexed
                type_oid,
                data_type,
                type_name,
                is_primary_key,
            };

            if is_primary_key {
                primary_key_columns.push(column.name().unwrap_or("unknown_column").to_owned());
            }

            columns.insert_overwrite(column_metadata);
        }

        TableMetadata {
            name: table_name,
            schema: schema_name,
            relation_oid,
            primary_key_columns,
            columns,
            indexes: Vec::new(), // Indexes are queried separately in cache_table_register
        }
    }

    /// Parse row data from InsertBody into a Vec of column values indexed by position.
    fn parse_insert_row_data(&self, body: &InsertBody) -> Result<Vec<Option<String>>, Error> {
        let mut row_data = Vec::new();

        // Parse the tuple data from InsertBody
        let tuple = body.tuple();

        // Extract values from each column in the tuple
        for column_data in tuple.tuple_data().iter() {
            let value = match column_data {
                postgres_replication::protocol::TupleData::Null => None,
                postgres_replication::protocol::TupleData::UnchangedToast => None, // Treat as NULL for now
                postgres_replication::protocol::TupleData::Text(data) => {
                    Some(String::from_utf8_lossy(data).to_string())
                }
            };
            row_data.push(value);
        }

        Ok(row_data)
    }

    /// Parse old and new row data from UpdateBody into Vecs of column values indexed by position.
    #[allow(clippy::type_complexity)]
    fn parse_update_row_data(
        &self,
        body: &UpdateBody,
    ) -> Result<(Vec<Option<String>>, Vec<Option<String>>), Error> {
        // Parse new tuple data (always present)
        let new_tuple = body.new_tuple();
        let mut new_row_data = Vec::new();

        for column_data in new_tuple.tuple_data().iter() {
            let value = match column_data {
                postgres_replication::protocol::TupleData::Null => None,
                postgres_replication::protocol::TupleData::UnchangedToast => None,
                postgres_replication::protocol::TupleData::Text(data) => {
                    Some(String::from_utf8_lossy(data).to_string())
                }
            };
            new_row_data.push(value);
        }

        let mut key_data = Vec::new();
        if let Some(key_tuple) = body.key_tuple() {
            for column_data in key_tuple.tuple_data().iter() {
                let value = match column_data {
                    postgres_replication::protocol::TupleData::Null => None,
                    postgres_replication::protocol::TupleData::UnchangedToast => None,
                    postgres_replication::protocol::TupleData::Text(data) => {
                        Some(String::from_utf8_lossy(data).to_string())
                    }
                };
                key_data.push(value);
            }
        }

        Ok((key_data, new_row_data))
    }

    /// Parse row data from DeleteBody into a Vec of column values indexed by position.
    fn parse_delete_row_data(&self, body: &DeleteBody) -> Result<Vec<Option<String>>, Error> {
        let mut row_data = Vec::new();

        // DeleteBody contains either key_tuple (for tables with REPLICA IDENTITY USING INDEX)
        // or old_tuple (for tables with REPLICA IDENTITY FULL)
        let tuple = if let Some(key_tuple) = body.key_tuple() {
            key_tuple
        } else if let Some(old_tuple) = body.old_tuple() {
            old_tuple
        } else {
            // No tuple data available (REPLICA IDENTITY NOTHING)
            error!("DELETE operation requires REPLICA IDENTITY FULL or USING INDEX, found NOTHING");
            return Ok(Vec::new());
        };

        // Extract values from each column in the tuple
        for column_data in tuple.tuple_data().iter() {
            let value = match column_data {
                postgres_replication::protocol::TupleData::Null => None,
                postgres_replication::protocol::TupleData::UnchangedToast => None,
                postgres_replication::protocol::TupleData::Text(data) => {
                    Some(String::from_utf8_lossy(data).to_string())
                }
            };
            row_data.push(value);
        }

        Ok(row_data)
    }

    /// Check if there are any cached queries for a specific table by relation OID.
    pub async fn is_relation_active(&mut self, relation_oid: u32) -> bool {
        if self.active_relations.contains(&relation_oid) {
            return true;
        }

        let (resp_tx, resp_rx) = oneshot::channel();

        if let Err(e) = self
            .cdc_tx
            .send(CdcMessage::RelationCheck(relation_oid, resp_tx))
        {
            //todo, halt use of cache and fallback to proxy only mode
            error!("Failed to handle DELETE for relation {relation_oid}: {e:?}");
            return true;
        }

        match resp_rx.await {
            Ok(has_queries) => {
                if has_queries {
                    self.active_relations.insert(relation_oid);
                }
                has_queries
            }
            Err(_) => {
                error!("the sender dropped");
                false
            }
        }
    }
}
