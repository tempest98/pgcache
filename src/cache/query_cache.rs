use std::cell::RefCell;
use std::rc::Rc;

use pg_query::ParseResult;
use tokio::sync::mpsc::UnboundedSender;
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage, types::Type};
use tokio_util::bytes::BytesMut;
use tracing::{info, instrument, trace};

use crate::cache::query::{cache_query_row_matches, query_select_replace};
use crate::settings::Settings;

use super::*;

#[derive(Debug)]
pub struct QueryRequest {
    pub data: BytesMut,
    pub ast: ParseResult,
    pub reply_tx: Sender<CacheReply>,
}

#[derive(Debug, Clone)]
pub struct QueryCache {
    db_cache: Rc<Client>,
    db_origin: Rc<Client>,

    worker_tx: UnboundedSender<QueryRequest>,

    cache: Rc<RefCell<Cache>>,
}

impl QueryCache {
    pub async fn new(
        settings: &Settings,
        cache: Cache,
        worker_tx: UnboundedSender<QueryRequest>,
    ) -> Result<Self, CacheError> {
        let (cache_client, cache_connection) = Config::new()
            .host(&settings.cache.host)
            .port(settings.cache.port)
            .user(&settings.cache.user)
            .dbname(&settings.cache.database)
            .connect(NoTls)
            .await?;

        let (origin_client, origin_connection) = Config::new()
            .host(&settings.origin.host)
            .port(settings.origin.port)
            .user(&settings.origin.user)
            .dbname(&settings.origin.database)
            .connect(NoTls)
            .await?;

        //task to process connection to cache pg db
        tokio::spawn(async move {
            if let Err(e) = cache_connection.await {
                error!("connection error: {e}");
            }
        });

        //task to process connection to origin pg db
        tokio::spawn(async move {
            if let Err(e) = origin_connection.await {
                error!("connection error: {e}");
            }
        });

        Ok(Self {
            db_cache: Rc::new(cache_client),
            db_origin: Rc::new(origin_client),
            worker_tx,
            cache: Rc::new(RefCell::new(cache)),
        })
    }

    #[instrument(skip_all)]
    pub async fn query_dispatch(&mut self, msg: QueryRequest) -> Result<(), CacheError> {
        let fingerprint = query_fingerprint(&msg.ast).map_err(|_| ParseError::Other)?;
        let cached_query_state = self
            .cache
            .borrow()
            .queries
            .get(&fingerprint)
            .map(|q| q.state);

        if cached_query_state.is_some_and(|state| state == CachedQueryState::Ready) {
            self.worker_tx.send(msg).map_err(|e| {
                error!("worker send {e}");
                CacheError::WorkerSend
            })
        } else {
            //forward query and load cache
            msg.reply_tx
                .send(CacheReply::Forward(msg.data))
                .await
                .map_err(|_| CacheError::Reply)?;

            if cached_query_state.is_none() {
                let table_oid = self.query_register(fingerprint, &msg.ast).await?;
                let rows = self.query_cache_fetch(&msg.ast).await?;
                self.query_cache_results(table_oid, &rows).await?;
                self.cache
                    .borrow_mut()
                    .queries
                    .entry(fingerprint)
                    .and_modify(|mut query| query.state = CachedQueryState::Ready);
                trace!("cached query ready");
            };

            Ok(())
        }
    }

    #[instrument(skip_all)]
    pub async fn query_cache_fetch(
        &mut self,
        ast: &ParseResult,
    ) -> Result<Vec<SimpleQueryMessage>, CacheError> {
        let new_ast = query_select_replace(ast);
        let query = new_ast.deparse()?;
        self.db_origin
            .simple_query(&query)
            .await
            .map_err(CacheError::PgError)
    }

    /// Registers a query in the cache for future lookups.
    #[instrument(skip_all)]
    pub async fn query_register(
        &mut self,
        fingerprint: u64,
        ast: &ParseResult,
    ) -> Result<u32, CacheError> {
        let table_name = &ast.select_tables()[0];

        if !self
            .cache
            .borrow()
            .tables
            .contains_key2(table_name.as_str())
        {
            let table = self.cache_table_create(table_name).await?;
            self.cache.borrow_mut().tables.insert_overwrite(table);
        }

        let relation_oid = self
            .cache
            .borrow()
            .tables
            .get2(table_name.as_str())
            .ok_or(CacheError::UnknownTable)?
            .relation_oid;

        // Parse WHERE conditions and store full expression AST
        let filter_expr = query_where_clause_parse(ast).unwrap_or_default();

        // Create CachedQuery entry
        let cached_query = CachedQuery {
            state: CachedQueryState::Loading,
            fingerprint,
            table_name: table_name.to_owned(),
            relation_oid,
            filter_expr,
        };

        // Store cached query metadata
        self.cache
            .borrow_mut()
            .queries
            .insert_overwrite(cached_query);
        trace!("cached query loading");

        Ok(relation_oid)
    }

    /// Check if there are any cached queries for a specific table by relation OID.
    pub async fn cached_queries_exist(&self, relation_oid: u32) -> bool {
        self.cache
            .borrow()
            .queries
            .iter()
            .any(|query| query.relation_oid == relation_oid)
    }

    /// Stores query results in the cache for faster retrieval.
    #[instrument(skip_all)]
    pub async fn query_cache_results(
        &self,
        table_oid: u32,
        response: &[SimpleQueryMessage],
    ) -> Result<(), CacheError> {
        if response.len() < 3 {
            //no results to store
            return Ok(());
        }
        let sql_list = {
            let cache = self.cache.borrow();
            let table = cache
                .tables
                .get1(&table_oid)
                .ok_or(CacheError::UnknownTable)?;

            let pkey_columns = &table.primary_key_columns;
            let table_name = table.name.as_str();

            let rows = response
                .iter()
                .filter_map(|msg| {
                    if let SimpleQueryMessage::Row(row) = msg {
                        Some(row)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            let mut sql_list = Vec::new();
            let columns: Vec<&str> = Vec::from_iter(rows[0].columns().iter().map(|c| c.name()));
            for &row in &rows {
                let mut values: Vec<String> = Vec::new();
                for idx in 0..row.columns().len() {
                    values.push(
                        row.get(idx)
                            .map(|v| format!("'{v}'"))
                            .unwrap_or("NULL".to_owned()),
                    );
                }

                let update_columns = columns
                    .iter()
                    .filter(|&&c| !pkey_columns.contains(&c.to_owned()))
                    .map(|&c| format!("{c} = EXCLUDED.{c}"))
                    .collect::<Vec<_>>();

                let mut insert_table =
                    format!("insert into {}({}) values (", table_name, columns.join(","));
                insert_table.push_str(&values.join(","));
                insert_table.push_str(") on conflict (");
                insert_table.push_str(&pkey_columns.join(","));
                insert_table.push_str(") do update set ");
                insert_table.push_str(&update_columns.join(", "));

                sql_list.push(insert_table);
            }
            sql_list
        };

        self.db_cache
            .simple_query(sql_list.join(";").as_str())
            .await?;

        Ok(())
    }

    #[instrument(skip_all)]
    async fn cache_table_create(&self, table_name: &str) -> Result<TableMetadata, CacheError> {
        let table = self.query_table_metadata(table_name).await?;
        self.cache_table_create_from_metadata(&table).await?;
        Ok(table)
    }

    #[instrument(skip_all)]
    async fn query_table_metadata(&self, table_name: &str) -> Result<TableMetadata, CacheError> {
        let create_table_sql = r"
            SELECT
                c.oid AS relation_oid,
                c.relname AS table_name,
                a.attname AS column_name,
                a.attnum AS position,
                a.atttypid AS type_oid,
                pg_catalog.format_type(a.atttypid, a.atttypmod) as type_name,
                a.attnum = any(pgc.conkey) as is_primary_key
            FROM pg_class c
            JOIN pg_namespace n on n.oid = c.relnamespace
            JOIN pg_attribute a on a.attrelid = c.oid
            JOIN pg_type t on t.oid = a.atttypid
            JOIN pg_constraint pgc on pgc.conrelid = c.oid
            WHERE c.relname = $1
            AND n.nspname = 'public'
            AND a.attnum > 0
            AND pgc.contype = 'p'
            AND NOT a.attisdropped
            ORDER BY a.attnum
        ";

        let rows = self
            .db_origin
            .query(create_table_sql, &[&table_name])
            .await?;

        let mut primary_key_columns: Vec<String> = Vec::new();
        let mut columns: BiHashMap<ColumnMetadata> = BiHashMap::with_capacity(rows.len());
        let mut relation_oid: Option<u32> = None;

        for row in &rows {
            // Get relation_oid from first row
            if relation_oid.is_none() {
                relation_oid = Some(row.get("relation_oid"));
            }

            let type_oid = row.get("type_oid");
            let data_type = Type::from_oid(type_oid).expect("valid type");

            let column = ColumnMetadata {
                name: row.get("column_name"),
                position: row.get("position"),
                type_oid,
                data_type,
                type_name: row.get("type_name"),
                is_primary_key: row.get("is_primary_key"),
            };

            if column.is_primary_key {
                primary_key_columns.push(column.name.clone());
            }
            columns.insert_overwrite(column);
        }

        let table = TableMetadata {
            name: table_name.to_owned(),
            schema: "public".to_owned(), //hardcoding for now
            relation_oid: relation_oid.expect("relation_oid should be present"),
            primary_key_columns,
            columns,
        };

        Ok(table)
    }

    #[instrument(skip_all)]
    async fn cache_table_create_from_metadata(
        &self,
        table_metadata: &TableMetadata,
    ) -> Result<(), CacheError> {
        let mut columns = Vec::new();
        for column in &table_metadata.columns {
            let column_sql = format!(
                "    {} {} {}",
                column.name,
                column.type_name,
                if column.is_primary_key {
                    "PRIMARY KEY"
                } else {
                    ""
                }
            );
            columns.push(column_sql);
        }

        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (\n{}\n)",
            table_metadata.name,
            columns.join(",\n")
        );

        self.db_cache.execute(&sql, &[]).await?;

        Ok(())
    }

    /// Register table metadata from CDC processing.
    #[instrument(skip_all)]
    pub async fn cache_table_register(
        &mut self,
        table_metadata: TableMetadata,
    ) -> Result<(), CacheError> {
        let relation_oid = table_metadata.relation_oid;

        // Check if table already exists
        let table_exists = self.cache.borrow().tables.contains_key1(&relation_oid);

        if table_exists {
            // TODO: Handle schema changes when table already exists
            // Compare existing TableMetadata with new metadata from CDC
            // Handle cases like:
            // - Column additions/removals
            // - Type changes
            // - Primary key changes
            // For now, we just skip re-registration
            info!(
                "Table {} (OID: {}) already exists, skipping registration",
                table_metadata.name, relation_oid
            );
            return Ok(());
        }

        // Table doesn't exist, create cache table from CDC metadata
        self.cache_table_create_from_metadata(&table_metadata)
            .await?;

        // Store CDC metadata in both indexes
        self.cache
            .borrow_mut()
            .tables
            .insert_overwrite(table_metadata);

        Ok(())
    }

    fn cache_upsert_sql(
        &self,
        table_metadata: &TableMetadata,
        row_data: &[Option<String>],
    ) -> Result<String, CacheError> {
        // For now, use a simplified approach without parameters to avoid ToSql complexity

        // Build column names and values for INSERT
        let mut column_names = Vec::new();
        let mut values = Vec::new();

        for column_meta in &table_metadata.columns {
            let position = column_meta.position as usize - 1;
            if position < row_data.len() {
                let value = row_data[position]
                    .as_deref()
                    .map_or("NULL".to_string(), |v| format!("'{v}'"));

                column_names.push(column_meta.name.as_str());
                values.push(value);
            }
        }

        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
            table_metadata.name,
            column_names.to_vec().join(", "),
            values.to_vec().join(", "),
            table_metadata.primary_key_columns.join(", "),
            column_names
                .iter()
                .filter(|&col| !table_metadata
                    .primary_key_columns
                    .contains(&col.to_string()))
                .map(|col| format!("{col} = EXCLUDED.{col}"))
                .collect::<Vec<_>>()
                .join(", ")
        );

        Ok(sql)
    }

    /// Handle INSERT operation with query-aware filtering.
    /// Applies the insert to cache entries that match the filter conditions.
    #[instrument(skip_all)]
    pub async fn handle_insert(
        &self,
        relation_oid: u32,
        row_data: Vec<Option<String>>,
    ) -> Result<(), CacheError> {
        // Get cached queries that reference this table
        let sql_list = {
            let cache = self.cache.borrow();
            let cached_queries = cache
                .queries
                .iter()
                .filter(|query| query.relation_oid == relation_oid);

            // Get table metadata for column information
            let table_metadata = match cache.tables.get1(&relation_oid) {
                Some(metadata) => metadata,
                None => {
                    error!("No table metadata found for relation_oid: {}", relation_oid);
                    return Ok(());
                }
            };

            // Check each cached query to see if this INSERT affects it
            let mut sql_list = Vec::new();
            for query in cached_queries {
                if cache_query_row_matches(query, &row_data, table_metadata) {
                    sql_list.push(self.cache_upsert_sql(table_metadata, &row_data)?);
                }
            }

            sql_list
        };

        for sql in sql_list {
            self.db_cache.execute(sql.as_str(), &[]).await?;
        }

        Ok(())
    }

    /// Handle UPDATE operation with query-aware filtering.
    /// Analyzes old and new values to determine cache operations needed.
    #[instrument(skip_all)]

    pub async fn handle_update(
        &self,
        relation_oid: u32,
        key_data: Vec<Option<String>>,
        new_row_data: Vec<Option<String>>,
    ) -> Result<(), CacheError> {
        // Get table metadata for column information
        let (sql, maybe_key_sql) = {
            let cache = self.cache.borrow();
            let table_metadata = match cache.tables.get1(&relation_oid) {
                Some(metadata) => metadata,
                None => {
                    error!("No table metadata found for relation_oid: {relation_oid}");
                    return Ok(());
                }
            };

            // Check each cached query to see if this UPDATE affects it
            let matched = cache
                .queries
                .iter()
                .filter(|query| query.relation_oid == relation_oid)
                .any(|query| cache_query_row_matches(query, &new_row_data, table_metadata));

            let sql = if matched {
                self.cache_upsert_sql(table_metadata, &new_row_data)?
            } else {
                self.cache_delete_sql(table_metadata, &new_row_data)?
            };

            let maybe_key_sql = if !key_data.is_empty() {
                Some(self.cache_delete_sql(table_metadata, &key_data)?)
            } else {
                None
            };
            (sql, maybe_key_sql)
        };

        self.db_cache.execute(sql.as_str(), &[]).await?;
        if let Some(key_sql) = maybe_key_sql {
            self.db_cache.execute(key_sql.as_str(), &[]).await?;
        }

        Ok(())
    }

    /// Handle DELETE operation by removing the row from cache.
    #[instrument(skip_all)]
    pub async fn handle_delete(
        &self,
        relation_oid: u32,
        row_data: Vec<Option<String>>,
    ) -> Result<(), CacheError> {
        // Get table metadata for column information
        let delete_sql = {
            let cache = self.cache.borrow();
            let table_metadata = match cache.tables.get1(&relation_oid) {
                Some(metadata) => metadata,
                None => {
                    error!("No table metadata found for relation_oid: {}", relation_oid);
                    return Ok(());
                }
            };

            self.cache_delete_sql(table_metadata, &row_data)?
        };

        self.db_cache.execute(delete_sql.as_str(), &[]).await?;

        Ok(())
    }

    /// Apply DELETE to cache database by removing matching rows.
    #[instrument(skip_all)]
    fn cache_delete_sql(
        &self,
        table_metadata: &TableMetadata,
        row_data: &[Option<String>],
    ) -> Result<String, CacheError> {
        // Build WHERE clause using primary key columns
        let mut where_conditions = Vec::new();

        for pk_column in &table_metadata.primary_key_columns {
            if let Some(column_meta) = table_metadata.columns.get1(pk_column.as_str()) {
                let position = column_meta.position as usize - 1;
                if position < row_data.len() {
                    let value = row_data[position].as_deref().unwrap_or("NULL");
                    where_conditions.push(format!("{pk_column} = '{value}'"));
                }
            }
        }

        if where_conditions.is_empty() {
            error!("Cannot build DELETE WHERE clause: no primary key values found");
            return Err(CacheError::NoPrimaryKey);
        }

        let sql = format!(
            "DELETE FROM {} WHERE {}",
            table_metadata.name,
            where_conditions.join(" AND ")
        );

        Ok(sql)
    }
}
