use std::cell::RefCell;
use std::rc::Rc;

use tokio::sync::mpsc::UnboundedSender;
use tokio_postgres::Row;
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage, types::Type};
use tokio_util::bytes::BytesMut;
use tracing::{info, instrument, trace};

use crate::cache::query::cache_query_row_matches;
use crate::query::ast::{Deparse, ast_query_fingerprint};
use crate::query::transform::{query_select_replace, query_table_update_queries};
use crate::settings::Settings;

use super::*;

#[derive(Debug)]
pub struct QueryRequest {
    pub data: BytesMut,
    pub select_statement: Box<SelectStatement>,
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
        let stmt = &msg.select_statement;
        let fingerprint = ast_query_fingerprint(stmt);
        let cached_query_state = self
            .cache
            .borrow()
            .cached_queries
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
                let table_oids = self.query_register(fingerprint, stmt).await?;
                for table_oid in table_oids {
                    let rows = self.query_cache_fetch(table_oid, stmt).await?;
                    self.query_cache_results(table_oid, &rows).await?;
                    self.cache
                        .borrow_mut()
                        .cached_queries
                        .entry(fingerprint)
                        .and_modify(|mut query| query.state = CachedQueryState::Ready);
                }
                trace!("cached query ready");
            };

            Ok(())
        }
    }

    #[instrument(skip_all)]
    pub async fn query_cache_fetch(
        &mut self,
        relation_oid: u32,
        select_statement: &SelectStatement,
    ) -> Result<Vec<SimpleQueryMessage>, CacheError> {
        let mut buf = String::with_capacity(1024);
        let query = {
            let cache = self.cache.borrow();
            let table = cache
                .tables
                .get1(&relation_oid)
                .ok_or(CacheError::UnknownTable)?;

            let maybe_alias = select_statement
                .tables()
                .filter(|&tn| tn.name == table.name)
                .flat_map(|t| t.alias.as_ref())
                .next();

            let select_columns = table.select_columns(maybe_alias);

            let new_ast = query_select_replace(select_statement, select_columns);
            new_ast.deparse(&mut buf)
        };

        dbg!(&query);
        self.db_origin
            .simple_query(query)
            .await
            .map_err(CacheError::PgError)
    }

    /// Registers a query in the cache for future lookups.
    #[instrument(skip_all)]
    pub async fn query_register(
        &mut self,
        fingerprint: u64,
        select_statement: &SelectStatement,
    ) -> Result<Vec<u32>, CacheError> {
        let mut relation_oids = Vec::new();
        for table_node in select_statement.tables() {
            let table_name = table_node.name.as_str();
            let schema = table_node.schema.as_deref();

            if !self.cache.borrow().tables.contains_key2(table_name) {
                let table = self.cache_table_create(schema, table_name).await?;
                self.cache.borrow_mut().tables.insert_overwrite(table);
            }
        }

        for (table_node, update_query) in query_table_update_queries(select_statement) {
            let relation_oid = self
                .cache
                .borrow()
                .tables
                .get2(table_node.name.as_str())
                .ok_or(CacheError::UnknownTable)?
                .relation_oid;

            self.cache
                .borrow_mut()
                .update_queries
                .entry(relation_oid)
                .and_modify(|mut queries| queries.select_statements.push(update_query.clone()))
                .or_insert_with(|| UpdateQueries {
                    relation_oid,
                    select_statements: vec![update_query],
                });

            relation_oids.push(relation_oid);
        }

        // Create CachedQuery entry using the already converted AST
        let cached_query = CachedQuery {
            state: CachedQueryState::Loading,
            fingerprint,
            relation_oids: relation_oids.clone(),
            select_statement: select_statement.clone(),
        };

        // Store cached query metadata
        self.cache
            .borrow_mut()
            .cached_queries
            .insert_overwrite(cached_query);
        trace!("cached query loading");

        Ok(relation_oids)
    }

    /// Check if there are any cached queries for a specific table by relation OID.
    pub async fn cached_queries_exist(&self, relation_oid: u32) -> bool {
        self.cache
            .borrow()
            .cached_queries
            .iter()
            .any(|query| query.relation_oids.contains(&relation_oid))
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
    async fn cache_table_create(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<TableMetadata, CacheError> {
        let table = self.query_table_metadata(schema, table).await?;
        self.cache_table_create_from_metadata(&table).await?;
        Ok(table)
    }

    #[instrument(skip_all)]
    async fn query_table_metadata(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<TableMetadata, CacheError> {
        let rows = self.query_table_columns_get(schema, table).await?;

        let mut primary_key_columns: Vec<String> = Vec::new();
        let mut columns: BiHashMap<ColumnMetadata> = BiHashMap::with_capacity(rows.len());
        let mut relation_oid: Option<u32> = None;
        let mut schema: Option<&str> = schema;

        for row in &rows {
            // Get relation_oid from first row
            if relation_oid.is_none() {
                relation_oid = Some(row.get("relation_oid"));
            }

            if schema.is_none() {
                schema = Some(row.get("table_schema"));
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

        let Some(relation_oid) = relation_oid else {
            return Err(CacheError::UnknownTable);
        };

        let Some(schema) = schema else {
            return Err(CacheError::UnknownSchema);
        };

        let table = TableMetadata {
            name: table.to_owned(),
            schema: schema.to_owned(),
            relation_oid,
            primary_key_columns,
            columns,
        };

        Ok(table)
    }

    #[instrument(skip_all)]
    async fn query_table_columns_get(
        &self,
        schema: Option<&str>,
        table: &str,
    ) -> Result<Vec<Row>, CacheError> {
        // need a nicer way to do this, two separate queries for when the schema is known
        // and for when it is not known.
        let rows = if let Some(schema) = schema {
            let sql = r"
                SELECT
                    c.oid AS relation_oid,
                    n.nspname AS table_schema,
                    c.relname AS table_name,
                    a.attname AS column_name,
                    a.attnum AS position,
                    a.atttypid AS type_oid,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) as type_name,
                    a.attnum = any(pgc.conkey) as is_primary_key
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid
                JOIN pg_type t ON t.oid = a.atttypid
                JOIN pg_constraint pgc ON pgc.conrelid = c.oid
                WHERE c.relname = $1
                AND n.nspname = $2
                AND a.attnum > 0
                AND pgc.contype = 'p'
                AND NOT a.attisdropped
                ORDER BY a.attnum;
            ";

            self.db_origin.query(sql, &[&table, &schema]).await?
        } else {
            let sql = r"
                SELECT
                    c.oid AS relation_oid,
                    n.nspname AS table_schema,
                    c.relname AS table_name,
                    a.attname AS column_name,
                    a.attnum AS position,
                    a.atttypid AS type_oid,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) as type_name,
                    a.attnum = any(pgc.conkey) as is_primary_key
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_attribute a ON a.attrelid = c.oid
                JOIN pg_type t ON t.oid = a.atttypid
                JOIN pg_constraint pgc ON pgc.conrelid = c.oid
                WHERE c.relname = $1
                AND c.oid = (
                    SELECT c2.oid
                    FROM pg_class c2
                    JOIN pg_namespace n2 ON n2.oid = c2.relnamespace
                    WHERE c2.relname = $1
                    AND c2.relkind = 'r'
                    AND n2.nspname = any(current_schemas(false))
                    ORDER BY array_position(current_schemas(false), n2.nspname)
                    LIMIT 1
                )
                AND a.attnum > 0
                AND pgc.contype = 'p'
                AND NOT a.attisdropped
                ORDER BY a.attnum;
            ";

            self.db_origin.query(sql, &[&table]).await?
        };

        Ok(rows)
    }

    #[instrument(skip_all)]
    async fn cache_table_create_from_metadata(
        &self,
        table_metadata: &TableMetadata,
    ) -> Result<(), CacheError> {
        let mut columns = Vec::new();
        for column in &table_metadata.columns {
            let column_sql = format!("    {} {}", column.name, column.type_name,);
            columns.push(column_sql);
        }

        let drop_sql = format!("DROP TABLE IF EXISTS {}", table_metadata.name);

        let sql = format!(
            "CREATE TABLE {} (\n{},\n\tPRIMARY KEY({})\n)",
            table_metadata.name,
            columns.join(",\n"),
            table_metadata.primary_key_columns.join(", "),
        );

        self.db_cache.execute(&drop_sql, &[]).await?;
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

        let table_exists = self.cache.borrow().tables.contains_key1(&relation_oid);
        if table_exists {
            if let Some(current_table) = self.cache.borrow().tables.get1(&relation_oid)
                && current_table == &table_metadata
            {
                return Ok(());
            }

            // invalidate all the cached queries that use this table and recreate the table.
            info!(
                "Table {} (OID: {}) recreating table, invalidating queries",
                table_metadata.name, relation_oid
            );

            self.cache_table_invalidate(relation_oid);
        }

        self.cache_table_create_from_metadata(&table_metadata)
            .await?;

        self.cache
            .borrow_mut()
            .tables
            .insert_overwrite(table_metadata);

        Ok(())
    }

    fn cache_table_invalidate(&mut self, relation_oid: u32) {
        let mut cache = self.cache.borrow_mut();
        let fingerprints = cache
            .cached_queries
            .iter()
            .filter(|q| q.relation_oids.contains(&relation_oid))
            .map(|q| q.fingerprint)
            .collect::<Vec<_>>();

        for fp in fingerprints {
            trace!("invalidating query {fp}");
            cache.cached_queries.remove(&fp);
        }
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
                .cached_queries
                .iter()
                .filter(|query| query.relation_oids.contains(&relation_oid));

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
                .cached_queries
                .iter()
                .filter(|query| query.relation_oids.contains(&relation_oid))
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

    /// Handle Truncate operation by truncating table in cache.
    #[instrument(skip_all)]
    pub async fn handle_truncate(&self, relation_oids: &[u32]) -> Result<(), CacheError> {
        // Get table metadata for column information
        let truncate_sql = {
            let mut table_names: Vec<&str> = Vec::new();

            let cache = self.cache.borrow();
            for oid in relation_oids {
                if let Some(table_metadata) = cache.tables.get1(oid) {
                    table_names.push(table_metadata.name.as_str());
                };
            }

            format!("TRUNCATE {}", table_names.join(", "))
        };

        self.db_cache.execute(truncate_sql.as_str(), &[]).await?;

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
