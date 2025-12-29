use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use iddqd::BiHashMap;
use postgres_protocol::escape;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Sender, UnboundedSender};

use tokio_postgres::Row;
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage, types::Type};
use tokio_util::bytes::BytesMut;
use tracing::{error, info, instrument, trace};

use crate::catalog::{ColumnMetadata, IndexMetadata, TableMetadata};
use crate::query::ast::{Deparse, TableNode, ast_query_fingerprint};
use crate::query::constraints::analyze_query_constraints;
use crate::query::resolved::{
    ResolvedColumnNode, ResolvedJoinNode, ResolvedSelectStatement, ResolvedTableNode,
    select_statement_resolve,
};
use crate::query::transform::{
    query_table_update_queries, resolved_select_replace, resolved_table_replace_with_values,
};
use crate::settings::Settings;

use super::{
    CacheError,
    messages::CacheReply,
    query::CacheableQuery,
    types::{Cache, CachedQuery, CachedQueryState, UpdateQueries, UpdateQuery},
};

#[derive(Debug, PartialEq, Eq)]
pub enum QueryType {
    Simple,
    Extended,
}

#[derive(Debug)]
pub struct QueryRequest {
    pub query_type: QueryType,
    pub data: BytesMut,
    pub cacheable_query: Box<CacheableQuery>,
    pub result_formats: Vec<i16>,
    pub client_socket: TcpStream,
    pub reply_tx: Sender<CacheReply>,
    /// Resolved search_path for schema resolution
    pub search_path: Vec<String>,
}

/// Request sent to cache worker for executing cached queries.
/// Contains the resolved AST with schema-qualified table names.
#[derive(Debug)]
pub struct WorkerRequest {
    pub query_type: QueryType,
    pub data: BytesMut,
    pub resolved: ResolvedSelectStatement,
    pub result_formats: Vec<i16>,
    pub client_socket: TcpStream,
    pub reply_tx: Sender<CacheReply>,
}

#[derive(Debug, Clone)]
pub struct QueryCache {
    db_cache: Rc<Client>,
    db_origin: Rc<Client>,

    worker_tx: UnboundedSender<WorkerRequest>,

    cache: Rc<RefCell<Cache>>,
}

impl QueryCache {
    pub async fn new(
        settings: &Settings,
        cache: Cache,
        worker_tx: UnboundedSender<WorkerRequest>,
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
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn query_dispatch(&mut self, msg: QueryRequest) -> Result<(), CacheError> {
        // Generate fingerprint from AST (parameters are already replaced if this was a parameterized query)
        let stmt = msg.cacheable_query.statement();
        let fingerprint = ast_query_fingerprint(stmt);

        // Check cache state: Ready -> send to worker, Loading -> forward, None -> register
        let cache_state = self
            .cache
            .borrow()
            .cached_queries
            .get(&fingerprint)
            .map(|q| (q.state, q.resolved.clone()));

        if let Some((CachedQueryState::Ready, resolved)) = cache_state {
            let worker_request = WorkerRequest {
                query_type: msg.query_type,
                data: msg.data,
                resolved,
                result_formats: msg.result_formats,
                client_socket: msg.client_socket,
                reply_tx: msg.reply_tx,
            };
            self.worker_tx.send(worker_request).map_err(|e| {
                error!("worker send {e}");
                CacheError::WorkerSend
            })
        } else {
            //forward query and load cache
            msg.reply_tx
                .send(CacheReply::Forward(msg.data))
                .await
                .map_err(|_| CacheError::Reply)?;

            // Only register if not already in cache (state was None, not Loading)
            if cache_state.is_none() {
                let search_path_refs: Vec<&str> =
                    msg.search_path.iter().map(String::as_str).collect();
                let (table_oids, resolved) = self
                    .query_register(fingerprint, &msg.cacheable_query, &search_path_refs)
                    .await?;
                let mut total_bytes: usize = 0;
                for table_oid in table_oids {
                    let rows = self.query_cache_fetch(table_oid, &resolved).await?;
                    let bytes = self.query_cache_results(table_oid, &rows).await?;
                    total_bytes += bytes;
                    self.cache
                        .borrow_mut()
                        .cached_queries
                        .entry(fingerprint)
                        .and_modify(|mut query| query.state = CachedQueryState::Ready);
                }
                self.cache
                    .borrow_mut()
                    .cached_queries
                    .entry(fingerprint)
                    .and_modify(|mut query| {
                        query.state = CachedQueryState::Ready;
                        query.cached_bytes = total_bytes;
                    });
                trace!("cached query ready, cached_bytes={total_bytes}");
            };

            Ok(())
        }
    }

    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn query_cache_fetch(
        &mut self,
        relation_oid: u32,
        resolved: &ResolvedSelectStatement,
    ) -> Result<Vec<SimpleQueryMessage>, CacheError> {
        let mut buf = String::with_capacity(1024);
        let query = {
            let cache = self.cache.borrow();
            let table = cache
                .tables
                .get1(&relation_oid)
                .ok_or(CacheError::UnknownTable {
                    oid: Some(relation_oid),
                    name: None,
                })?;

            let maybe_alias = resolved
                .nodes::<ResolvedTableNode>()
                .find(|tn| tn.relation_oid == relation_oid)
                .and_then(|t| t.alias.as_deref());

            let select_columns = table.resolved_select_columns(maybe_alias);

            let new_ast = resolved_select_replace(resolved, select_columns);
            new_ast.deparse(&mut buf)
        };

        self.db_origin
            .simple_query(query)
            .await
            .map_err(CacheError::PgError)
    }

    /// Registers a query in the cache for future lookups.
    ///
    /// Returns the relation OIDs and resolved statement for cache population.
    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn query_register(
        &mut self,
        fingerprint: u64,
        cacheable_query: &CacheableQuery,
        search_path: &[&str],
    ) -> Result<(Vec<u32>, ResolvedSelectStatement), CacheError> {
        let select_statement = cacheable_query.statement();
        let mut relation_oids = Vec::new();

        // Ensure all tables are registered in the cache
        // First, resolve each table using the search_path to find the correct schema
        for table_node in select_statement.nodes::<TableNode>() {
            let table_name = table_node.name.as_str();

            // Determine schema: explicit or search through search_path
            let schema = if let Some(schema) = table_node.schema.as_deref() {
                schema.to_owned()
            } else {
                // Search through search_path to find which schema the table is in
                self.schema_for_table_find(table_name, search_path).await?
            };

            if !self
                .cache
                .borrow()
                .tables
                .contains_key2(&(schema.as_str(), table_name))
            {
                let table = self.cache_table_create(Some(&schema), table_name).await?;
                self.cache.borrow_mut().tables.insert_overwrite(table);
            }
        }

        // Resolve the query using catalog metadata
        let resolved =
            select_statement_resolve(select_statement, &self.cache.borrow().tables, search_path)?;

        // Analyze constraints from the resolved query
        let query_constraints = analyze_query_constraints(&resolved);

        for (table_node, update_select) in query_table_update_queries(cacheable_query) {
            // Resolve schema using explicit schema or search_path
            let schema = if let Some(schema) = table_node.schema.as_deref() {
                schema.to_owned()
            } else {
                self.schema_for_table_find(table_node.name.as_str(), search_path)
                    .await?
            };
            let relation_oid = self
                .cache
                .borrow()
                .tables
                .get2(&(schema.as_str(), table_node.name.as_str()))
                .ok_or(CacheError::UnknownTable {
                    oid: None,
                    name: Some(table_node.name.clone()),
                })?
                .relation_oid;

            // Resolve the update query
            let update_resolved =
                select_statement_resolve(&update_select, &self.cache.borrow().tables, search_path)?;

            let update_query = UpdateQuery {
                fingerprint,
                resolved: update_resolved,
            };

            self.cache
                .borrow_mut()
                .update_queries
                .entry(relation_oid)
                .and_modify(|mut queries| queries.queries.push(update_query.clone()))
                .or_insert_with(|| UpdateQueries {
                    relation_oid,
                    queries: vec![update_query],
                });

            relation_oids.push(relation_oid);
        }

        // Create CachedQuery entry with resolved AST and constraints
        let cached_query = CachedQuery {
            state: CachedQueryState::Loading,
            fingerprint,
            relation_oids: relation_oids.clone(),
            select_statement: select_statement.clone(),
            resolved: resolved.clone(),
            constraints: query_constraints,
            cached_bytes: 0,
        };

        // Store cached query metadata
        self.cache
            .borrow_mut()
            .cached_queries
            .insert_overwrite(cached_query);
        trace!("cached query loading");

        Ok((relation_oids, resolved))
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
    /// Returns the number of bytes cached (sum of raw value sizes).
    #[instrument(skip_all)]
    pub async fn query_cache_results(
        &self,
        table_oid: u32,
        response: &[SimpleQueryMessage],
    ) -> Result<usize, CacheError> {
        let [
            SimpleQueryMessage::RowDescription(row_description),
            data_rows @ ..,
            _command_complete,
        ] = response
        else {
            //no results to store
            return Ok(0);
        };

        let mut cached_bytes: usize = 0;

        let sql_list = {
            let cache = self.cache.borrow();
            let table = cache
                .tables
                .get1(&table_oid)
                .ok_or(CacheError::UnknownTable {
                    oid: Some(table_oid),
                    name: None,
                })?;

            let pkey_columns = &table.primary_key_columns;
            let schema = &table.schema;
            let table_name = &table.name;

            let rows = data_rows
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
            let columns: Vec<&str> = Vec::from_iter(row_description.iter().map(|c| c.name()));
            for &row in &rows {
                let mut values: Vec<String> = Vec::new();
                for idx in 0..row.columns().len() {
                    let value = row.get(idx);
                    cached_bytes += value.map_or(0, |v| v.len());
                    values.push(
                        value
                            .map(escape::escape_literal)
                            .unwrap_or("NULL".to_owned()),
                    );
                }

                let update_columns = columns
                    .iter()
                    .filter(|&&c| !pkey_columns.contains(&c.to_owned()))
                    .map(|&c| format!("{c} = EXCLUDED.{c}"))
                    .collect::<Vec<_>>();

                let mut insert_table = format!(
                    "insert into {schema}.{table_name}({}) values (",
                    columns.join(",")
                );
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

        Ok(cached_bytes)
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
            let type_name = data_type.name().to_owned(); // Get the type name from tokio_postgres Type
            let pg_position: i64 = row.get("position");

            let column = ColumnMetadata {
                name: row.get("column_name"),
                position: pg_position as i16,
                type_oid,
                data_type,
                type_name,
                is_primary_key: row.get("is_primary_key"),
            };

            if column.is_primary_key {
                primary_key_columns.push(column.name.clone());
            }

            columns.insert_overwrite(column);
        }

        let Some(relation_oid) = relation_oid else {
            return Err(CacheError::UnknownTable {
                oid: relation_oid,
                name: None,
            });
        };

        let Some(schema) = schema else {
            return Err(CacheError::UnknownSchema);
        };

        let indexes = self.query_table_indexes_get(relation_oid).await?;

        let table = TableMetadata {
            name: table.to_owned(),
            schema: schema.to_owned(),
            relation_oid,
            primary_key_columns,
            columns,
            indexes,
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
                    RANK() OVER (order by a.attnum) AS position,
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
                    RANK() OVER (order by a.attnum) AS position,
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

    /// Query index metadata for a table from the origin database.
    ///
    /// Returns non-primary-key indexes, excluding expression and partial indexes.
    /// Primary key indexes are excluded since the PRIMARY KEY constraint creates them.
    #[instrument(skip_all)]
    async fn query_table_indexes_get(
        &self,
        relation_oid: u32,
    ) -> Result<Vec<IndexMetadata>, CacheError> {
        let sql = r"
            SELECT
                i.relname AS index_name,
                ix.indisunique AS is_unique,
                am.amname AS method,
                array_agg(a.attname ORDER BY array_position(ix.indkey::int[], a.attnum::int)) AS columns
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_am am ON am.oid = i.relam
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE t.oid = $1
              AND NOT ix.indisprimary
              AND ix.indexprs IS NULL
              AND ix.indpred IS NULL
            GROUP BY i.relname, ix.indisunique, am.amname, ix.indkey
            ORDER BY i.relname;
        ";

        let rows = self.db_origin.query(sql, &[&relation_oid]).await?;

        let indexes = rows
            .iter()
            .map(|row| {
                let columns: Vec<String> = row.get("columns");
                IndexMetadata {
                    name: row.get("index_name"),
                    is_unique: row.get("is_unique"),
                    method: row.get("method"),
                    columns,
                }
            })
            .collect();

        Ok(indexes)
    }

    /// Find which schema a table belongs to by searching through the search_path.
    ///
    /// Queries the database to find the first schema in search_path that contains
    /// the given table name.
    #[instrument(skip_all)]
    async fn schema_for_table_find(
        &self,
        table_name: &str,
        search_path: &[&str],
    ) -> Result<String, CacheError> {
        // First check if the table is already in our cache
        for schema in search_path {
            if self
                .cache
                .borrow()
                .tables
                .get2(&(*schema, table_name))
                .is_some()
            {
                return Ok((*schema).to_owned());
            }
        }

        // Query the database to find which schema contains the table
        let sql = r"
            SELECT n.nspname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = $1
            AND c.relkind = 'r'
            AND n.nspname = any($2)
            ORDER BY array_position($2::text[], n.nspname::text)
            LIMIT 1;
        ";

        let rows = self
            .db_origin
            .query(sql, &[&table_name, &search_path])
            .await?;

        rows.first()
            .map(|row| row.get::<_, String>(0))
            .ok_or_else(|| CacheError::UnknownTable {
                oid: None,
                name: Some(table_name.to_owned()),
            })
    }

    async fn query_row_changes(
        &self,
        relation_oid: u32,
        row_data: &[Option<String>],
    ) -> Result<Vec<Row>, CacheError> {
        let sql = {
            let cache = self.cache.borrow();
            let table_metadata =
                cache
                    .tables
                    .get1(&relation_oid)
                    .ok_or(CacheError::UnknownTable {
                        oid: Some(relation_oid),
                        name: None,
                    })?;

            // Build WHERE clause using primary key columns
            let mut where_conditions = Vec::new();
            for pk_column in &table_metadata.primary_key_columns {
                if let Some(column_meta) = table_metadata.columns.get1(pk_column.as_str()) {
                    let position = column_meta.position as usize - 1;
                    if let Some(row_value) = row_data.get(position) {
                        let value = row_value
                            .as_deref()
                            .map_or_else(|| "NULL".to_owned(), escape::escape_literal);
                        where_conditions.push(format!("{pk_column} = {value}"));
                    }
                }
            }

            if where_conditions.is_empty() {
                return Err(CacheError::NoPrimaryKey);
            }

            // Build comparison columns
            let mut comparison_columns = Vec::new();
            for column_meta in &table_metadata.columns {
                let position = column_meta.position as usize - 1;
                if let Some(row_value) = row_data.get(position) {
                    let value = row_value
                        .as_deref()
                        .map_or_else(|| "NULL".to_owned(), escape::escape_literal);
                    comparison_columns.push(format!(
                        "{} IS DISTINCT FROM {} AS {}",
                        column_meta.name, value, column_meta.name
                    ));
                }
            }

            format!(
                "SELECT {} FROM {}.{} WHERE {}",
                comparison_columns.join(", "),
                table_metadata.schema,
                table_metadata.name,
                where_conditions.join(" AND ")
            )
        };

        self.db_cache
            .query(&sql, &[])
            .await
            .map_err(CacheError::PgError)
    }

    #[instrument(skip_all)]
    async fn cache_table_create_from_metadata(
        &self,
        table_metadata: &TableMetadata,
    ) -> Result<(), CacheError> {
        let schema = &table_metadata.schema;
        let table = &table_metadata.name;

        let column_defs: Vec<String> = table_metadata
            .columns
            .iter()
            .map(|c| format!("    {} {}", c.name, c.type_name))
            .collect();
        let column_defs = column_defs.join(",\n");

        let primary_key = table_metadata.primary_key_columns.join(", ");

        // Create schema if it doesn't exist, then create table with qualified name
        let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS {schema}");
        let drop_sql = format!("DROP TABLE IF EXISTS {schema}.{table}");
        let create_sql = format!(
            "CREATE UNLOGGED TABLE {schema}.{table} (\n{column_defs},\n\tPRIMARY KEY({primary_key})\n)"
        );

        self.db_cache.execute(&create_schema_sql, &[]).await?;
        self.db_cache.execute(&drop_sql, &[]).await?;
        self.db_cache.execute(&create_sql, &[]).await?;

        // Create indexes
        for index in &table_metadata.indexes {
            let unique = if index.is_unique { "UNIQUE " } else { "" };
            let method = &index.method;
            let columns = index.columns.join(", ");
            let index_sql =
                format!("CREATE {unique}INDEX ON {schema}.{table} USING {method} ({columns})");
            self.db_cache.execute(&index_sql, &[]).await?;
        }

        Ok(())
    }

    /// Register table metadata from CDC processing.
    ///
    /// CDC RelationBody doesn't include index information, so indexes are
    /// queried from the origin database when table_metadata.indexes is empty.
    #[instrument(skip_all)]
    pub async fn cache_table_register(
        &mut self,
        mut table_metadata: TableMetadata,
    ) -> Result<(), CacheError> {
        let relation_oid = table_metadata.relation_oid;

        let table_exists = self.cache.borrow().tables.contains_key1(&relation_oid);
        if table_exists {
            if let Some(current_table) = self.cache.borrow().tables.get1(&relation_oid)
                && current_table.schema_eq(&table_metadata)
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

        // CDC RelationBody doesn't include indexes, so query them from origin
        if table_metadata.indexes.is_empty() {
            table_metadata.indexes = self.query_table_indexes_get(relation_oid).await?;
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
        let (fingerprints, oids): (Vec<_>, Vec<_>) = cache
            .cached_queries
            .iter()
            .filter(|&q| q.relation_oids.contains(&relation_oid))
            .map(|q| (q.fingerprint, q.relation_oids.iter()))
            .unzip();

        let oids = oids.into_iter().flatten().copied().collect::<HashSet<_>>();

        for fp in fingerprints {
            trace!("invalidating query {fp}");
            cache.cached_queries.remove(&fp);

            trace!("invalidating update queries {fp}");
            for oid in &oids {
                if let Some(mut queries) = cache.update_queries.get_mut(oid) {
                    queries.queries.retain(|q| q.fingerprint != fp);
                }
            }
        }
    }

    fn cache_query_invalidate(&mut self, fingerprint: u64) {
        let mut cache = self.cache.borrow_mut();
        let Some(query) = cache.cached_queries.remove(&fingerprint) else {
            return;
        };

        trace!("invalidating query {fingerprint}");

        trace!("invalidating update queries {fingerprint}");
        for oid in &query.relation_oids {
            if let Some(mut queries) = cache.update_queries.get_mut(oid) {
                queries.queries.retain(|q| q.fingerprint != fingerprint);
            }
        }
    }

    fn cache_upsert_with_predicate_sql(
        &self,
        resolved: &ResolvedSelectStatement,
        table_metadata: &TableMetadata,
        row_data: &[Option<String>],
    ) -> Result<String, CacheError> {
        // For now, use a simplified approach without parameters to avoid ToSql complexity

        // Build column names and values for INSERT
        let mut column_names = Vec::new();
        let mut values = Vec::new();

        for column_meta in &table_metadata.columns {
            let position = column_meta.position as usize - 1;
            if let Some(row_value) = row_data.get(position) {
                let value = row_value
                    .as_deref()
                    .map_or_else(|| "NULL".to_owned(), escape::escape_literal);

                column_names.push(column_meta.name.as_str());
                values.push(value);
            }
        }

        let value_select = resolved_table_replace_with_values(resolved, table_metadata, row_data)?;
        let mut select = String::with_capacity(1024);
        value_select.deparse(&mut select);

        let schema = &table_metadata.schema;
        let table = &table_metadata.name;
        let column_list = column_names.join(", ");
        let value_list = values.join(", ");
        let pk_column_list = table_metadata.primary_key_columns.join(", ");
        let update_list = column_names
            .iter()
            .filter(|&col| {
                !table_metadata
                    .primary_key_columns
                    .contains(&col.to_string())
            })
            .map(|col| format!("{col} = EXCLUDED.{col}"))
            .collect::<Vec<_>>()
            .join(", ");

        let sql = format!(
            "INSERT INTO {schema}.{table} ({column_list}) \
            SELECT {value_list} WHERE EXISTS ({select}) \
            ON CONFLICT ({pk_column_list}) \
            DO UPDATE SET {update_list}"
        );

        Ok(sql)
    }

    fn update_queries_sql_list(
        &self,
        relation_oid: u32,
        row_data: &[Option<String>],
    ) -> Result<Vec<String>, CacheError> {
        let cache = self.cache.borrow();
        let update_queries =
            cache
                .update_queries
                .get(&relation_oid)
                .ok_or(CacheError::UnknownTable {
                    oid: Some(relation_oid),
                    name: None,
                })?;

        // Get table metadata for column information
        let Some(table_metadata) = cache.tables.get1(&relation_oid) else {
            error!("No table metadata found for relation_oid: {}", relation_oid);
            return Err(CacheError::UnknownTable {
                oid: Some(relation_oid),
                name: None,
            });
        };

        let mut sql_list = Vec::new();
        for update_query in &update_queries.queries {
            sql_list.push(self.cache_upsert_with_predicate_sql(
                &update_query.resolved,
                table_metadata,
                row_data,
            )?);
        }

        Ok(sql_list)
    }

    fn update_queries_check_invalidate(
        &self,
        relation_oid: u32,
        row_changes: &Option<&Row>,
        row_data: &[Option<String>],
    ) -> Result<Vec<u64>, CacheError> {
        let cache = self.cache.borrow();
        let update_queries =
            cache
                .update_queries
                .get(&relation_oid)
                .ok_or(CacheError::UnknownTable {
                    oid: Some(relation_oid),
                    name: None,
                })?;

        // Get table metadata for column information
        let Some(table_metadata) = cache.tables.get1(&relation_oid) else {
            error!("No table metadata found for relation_oid: {}", relation_oid);
            return Err(CacheError::UnknownTable {
                oid: Some(relation_oid),
                name: None,
            });
        };

        // invalidate queries that have a column from the table as part of
        // a join condition
        // coarse invalidation, can be refined later
        let mut fp_list = Vec::new();
        for update_query in &update_queries.queries {
            // Get the cached query to access constraints
            let cached_query = cache
                .cached_queries
                .get(&update_query.fingerprint)
                .ok_or_else(|| {
                    error!(
                        "Cached query not found for fingerprint: {}",
                        update_query.fingerprint
                    );
                    CacheError::Other
                })?;

            // Check if this is an INSERT (row_changes is None)
            if row_changes.is_none() {
                // If it is not a join then no need to invalidate
                if update_query.resolved.is_single_table() {
                    continue;
                }

                // For INSERTs: Check if new row matches all table constraints
                // If it doesn't match, the row won't appear in results, no invalidation needed
                if let Some(constraints) = cached_query
                    .constraints
                    .table_constraints
                    .get(&table_metadata.name)
                {
                    let mut all_match = true;
                    for (column_name, constraint_value) in constraints {
                        if let Some(column_meta) = table_metadata.columns.get1(column_name.as_str())
                        {
                            let position = column_meta.position as usize - 1;
                            if let Some(row_value) = row_data.get(position)
                                && !constraint_value.matches(row_value)
                            {
                                all_match = false;
                                break;
                            }
                        }
                    }

                    if !all_match {
                        // Row doesn't match constraints, won't appear in results
                        continue;
                    }
                }

                // Row matches constraints or no constraints exist
                // Must invalidate because result set will grow
                fp_list.push(update_query.fingerprint);
                continue;
            }

            let resolved = &update_query.resolved;
            let tables = resolved
                .nodes::<ResolvedTableNode>()
                .flat_map(|t| {
                    [
                        (Some(t.name.as_str()), t.name.as_str()),
                        (t.alias.as_deref(), t.name.as_str()),
                    ]
                })
                .collect::<HashMap<_, _>>();
            let joins = resolved.nodes::<ResolvedJoinNode>().collect::<Vec<_>>();

            // Check if we need to invalidate based on constraint analysis
            let mut needs_invalidation = false;

            for join in joins {
                //extract tables and columns used in the join condition
                let columns = join
                    .nodes::<ResolvedColumnNode>()
                    .map(|c| (tables.get(&Some(c.table.as_str())), c.column.as_str()))
                    .collect::<Vec<_>>();

                for (table, column) in columns {
                    if table.is_none_or(|t| t != &table_metadata.name) {
                        continue;
                    }

                    let column_changed =
                        row_changes.is_some_and(|row| row.get::<&str, bool>(column));

                    if !column_changed {
                        continue;
                    }

                    // JOIN column changed - use constraint-based optimization
                    if let Some(constraints) = cached_query
                        .constraints
                        .table_constraints
                        .get(&table_metadata.name)
                    {
                        // Check if new values match all constraints for this table
                        let mut all_constraints_match = true;
                        for (constraint_column, constraint_value) in constraints {
                            if let Some(column_meta) =
                                table_metadata.columns.get1(constraint_column.as_str())
                            {
                                let position = column_meta.position as usize - 1;
                                if let Some(row_value) = row_data.get(position)
                                    && !constraint_value.matches(row_value)
                                {
                                    all_constraints_match = false;
                                    break;
                                }
                            }
                        }

                        if !all_constraints_match {
                            // New values don't match all constraints
                            // Row leaving or staying out of result set, UPDATE handles removal
                            continue;
                        }

                        // New values match all constraints AND column changed
                        // Since column changed, old value was different (didn't match constraint)
                        // Therefore: row is entering result set → need invalidation
                        needs_invalidation = true;
                        break;
                    } else {
                        // No constraints available, use conservative approach
                        needs_invalidation = true;
                        break;
                    }
                }

                if needs_invalidation {
                    break;
                }
            }

            if needs_invalidation {
                fp_list.push(update_query.fingerprint);
            }
        }

        Ok(fp_list)
    }

    /// Handle INSERT operation with query-aware filtering.
    /// Applies the insert to cache entries that match the filter conditions.
    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn handle_insert(
        &mut self,
        relation_oid: u32,
        row_data: Vec<Option<String>>,
    ) -> Result<(), CacheError> {
        // Get cached queries that need to be invalidated
        let fp_list = self.update_queries_check_invalidate(relation_oid, &None, &row_data)?;
        for fp in fp_list {
            self.cache_query_invalidate(fp)
        }

        // Get update queries that reference this table
        let sql_list = self.update_queries_sql_list(relation_oid, &row_data)?;

        for sql in sql_list {
            let modified_cnt = self.db_cache.execute(sql.as_str(), &[]).await?;
            if modified_cnt == 1 {
                break;
            } else if modified_cnt > 1 {
                return Err(CacheError::TooManyModifiedRows);
            }
        }

        Ok(())
    }

    /// Handle UPDATE operation with query-aware filtering.
    /// Analyzes old and new values to determine cache operations needed.
    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn handle_update(
        &mut self,
        relation_oid: u32,
        key_data: Vec<Option<String>>,
        new_row_data: Vec<Option<String>>,
    ) -> Result<(), CacheError> {
        let row_changes = self.query_row_changes(relation_oid, &new_row_data).await?;

        // Get cached queries that need to be invalidated
        let fp_list = self.update_queries_check_invalidate(
            relation_oid,
            &row_changes.first(),
            &new_row_data,
        )?;
        for fp in fp_list {
            self.cache_query_invalidate(fp)
        }

        let sql_list = self.update_queries_sql_list(relation_oid, &new_row_data)?;

        let mut matched = false;
        for sql in sql_list {
            let modified_cnt = self.db_cache.execute(sql.as_str(), &[]).await?;
            if modified_cnt == 1 {
                matched = true;
                break;
            } else if modified_cnt > 1 {
                return Err(CacheError::TooManyModifiedRows);
            }
        }

        if !matched {
            let delete_sql = {
                let cache = self.cache.borrow();
                let Some(table_metadata) = cache.tables.get1(&relation_oid) else {
                    error!("No table metadata found for relation_oid: {}", relation_oid);
                    return Err(CacheError::UnknownTable {
                        oid: Some(relation_oid),
                        name: None,
                    });
                };

                self.cache_delete_sql(table_metadata, &new_row_data)?
            };
            self.db_cache.execute(delete_sql.as_str(), &[]).await?;
        }

        if !key_data.is_empty() {
            let delete_sql = {
                let cache = self.cache.borrow();
                let Some(table_metadata) = cache.tables.get1(&relation_oid) else {
                    error!("No table metadata found for relation_oid: {}", relation_oid);
                    return Err(CacheError::UnknownTable {
                        oid: Some(relation_oid),
                        name: None,
                    });
                };

                self.cache_delete_sql(table_metadata, &key_data)?
            };
            self.db_cache.execute(delete_sql.as_str(), &[]).await?;
        }

        Ok(())
    }

    /// Handle DELETE operation by removing the row from cache.
    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
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
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn handle_truncate(&self, relation_oids: &[u32]) -> Result<(), CacheError> {
        // Get table metadata for column information
        let truncate_sql = {
            let mut table_names: Vec<String> = Vec::new();

            let cache = self.cache.borrow();
            for oid in relation_oids {
                if let Some(table_metadata) = cache.tables.get1(oid) {
                    table_names.push(format!("{}.{}", table_metadata.schema, table_metadata.name));
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
                if let Some(row_value) = row_data.get(position) {
                    let value = row_value
                        .as_deref()
                        .map_or_else(|| "NULL".to_owned(), escape::escape_literal);
                    where_conditions.push(format!("{pk_column} = {value}"));
                }
            }
        }

        if where_conditions.is_empty() {
            error!("Cannot build DELETE WHERE clause: no primary key values found");
            return Err(CacheError::NoPrimaryKey);
        }

        let sql = format!(
            "DELETE FROM {}.{} WHERE {}",
            table_metadata.schema,
            table_metadata.name,
            where_conditions.join(" AND ")
        );

        Ok(sql)
    }
}
