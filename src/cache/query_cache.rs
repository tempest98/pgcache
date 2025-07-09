#![allow(dead_code)]

use std::collections::HashMap;
use std::rc::Rc;

use iddqd::{BiHashItem, BiHashMap, IdHashItem, IdHashMap, bi_upcast, id_upcast};
use pg_query::ParseResult;
use tokio::sync::mpsc::UnboundedSender;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage, SimpleQueryRow, types::Type};
use tokio_util::bytes::{Buf, BytesMut};
use tracing::{debug, instrument};

use crate::settings::Settings;

use super::*;

#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub relation_oid: u32,
    pub name: String,
    pub schema: String,
    pub primary_key_columns: Vec<String>,
    pub columns: HashMap<String, ColumnMetadata>,
    // pub last_updated: std::time::SystemTime,
    // pub estimated_row_count: Option<i64>,
}

impl BiHashItem for TableMetadata {
    type K1<'a> = u32;
    type K2<'a> = &'a str;

    fn key1(&self) -> Self::K1<'_> {
        self.relation_oid
    }

    fn key2(&self) -> Self::K2<'_> {
        self.name.as_str()
    }

    bi_upcast!();
}

#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    pub name: String,
    pub position: i16,
    pub type_oid: u32,
    pub data_type: Type,
    pub type_name: String,
    pub is_primary_key: bool,
    // pub is_nullable: bool,
    // pub max_length: Option<i32>,
    // pub default_value: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CachedQuery {
    pub fingerprint: u64,
    pub table_name: String,
    pub relation_oid: u32,
    pub filter_expr: Option<WhereExpr>,
}

impl IdHashItem for CachedQuery {
    type Key<'a> = u64;

    fn key(&self) -> Self::Key<'_> {
        self.fingerprint
    }

    id_upcast!();
}

pub struct QueryRequest {
    pub data: BytesMut,
    pub ast: ParseResult,
    pub reply_tx: oneshot::Sender<CacheReply>,
}

#[derive(Debug)]
pub struct QueryCache {
    db_cache: Client,
    db_origin: Client,

    worker_tx: UnboundedSender<QueryRequest>,

    tables: BiHashMap<TableMetadata>,
    pub cached_queries: IdHashMap<CachedQuery>,
}

impl QueryCache {
    pub async fn new(
        settings: &Settings,
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
                eprintln!("connection error: {e}");
            }
        });

        //task to process connection to origin pg db
        tokio::spawn(async move {
            if let Err(e) = origin_connection.await {
                eprintln!("connection error: {e}");
            }
        });

        Ok(Self {
            db_cache: cache_client,
            db_origin: origin_client,
            worker_tx,
            tables: BiHashMap::new(),
            cached_queries: IdHashMap::new(),
        })
    }

    pub async fn query_dispatch(&mut self, msg: QueryRequest) -> Result<(), CacheError> {
        let fingerprint = query_fingerprint(&msg.ast).map_err(|_| ParseError::Other)?;
        let cache_hit = self.cached_queries.contains_key(&fingerprint);

        if cache_hit {
            self.worker_tx.send(msg).map_err(|e| {
                error!("worker send {e}");
                CacheError::WorkerSend
            })
        } else {
            match self
                .handle_cache_miss(fingerprint, &msg.data, &msg.ast)
                .await
            {
                Ok(buf) => msg
                    .reply_tx
                    .send(CacheReply::Data(buf))
                    .map_err(|_| CacheError::Reply),
                Err(e) => {
                    error!("handle_cached_query failed {e}");
                    msg.reply_tx
                        .send(CacheReply::Error(msg.data))
                        .map_err(|_| CacheError::Reply)
                }
            }
        }
    }

    #[instrument]
    pub async fn handle_cache_miss(
        &mut self,
        fingerprint: u64,
        data: &BytesMut,
        ast: &ParseResult,
    ) -> Result<BytesMut, CacheError> {
        let msg_len = (&data[1..5]).get_u32() as usize;
        let query = str::from_utf8(&data[5..msg_len]).map_err(|_| ParseError::InvalidUtf8)?;
        // let stmt = query_target.prepare(query).await.unwrap();
        let res = self.db_origin.simple_query(query).await?;
        // let res = query_target.query(query, &[]).await;
        // dbg!(&res);

        let mut buf = BytesMut::new();
        let SimpleQueryMessage::RowDescription(desc) = &res[0] else {
            return Err(CacheError::InvalidMessage);
        };
        row_description_encode(desc, &mut buf);

        let mut rows = Vec::new();
        for msg in &res[1..(res.len() - 1)] {
            match msg {
                SimpleQueryMessage::Row(row) => {
                    simple_query_row_encode(row, &mut buf);
                    rows.push(row);
                }
                _ => return Err(CacheError::InvalidMessage),
            }
        }

        let SimpleQueryMessage::CommandComplete(cnt) = &res[res.len() - 1] else {
            return Err(CacheError::InvalidMessage);
        };
        command_complete_encode(*cnt, &mut buf);
        ready_for_query_encode(&mut buf);

        // Create cache table and store results
        // todo query_register and query_cache_results need to be treated atomically
        let table_oid = self.query_register(fingerprint, ast).await?;
        self.query_cache_results(table_oid, &rows).await?;

        debug!("cache miss");
        Ok(buf)
    }

    /// Registers a query in the cache for future lookups.
    pub async fn query_register(
        &mut self,
        fingerprint: u64,
        ast: &ParseResult,
    ) -> Result<u32, CacheError> {
        let table_name = &ast.select_tables()[0];

        if !self.tables.contains_key2(table_name.as_str()) {
            let table = self.cache_table_create(table_name).await?;
            self.tables.insert_overwrite(table);
        }

        let table = self
            .tables
            .get2(table_name.as_str())
            .ok_or(CacheError::UnknownTable)?;

        // Parse WHERE conditions and store full expression AST
        let filter_expr = query_where_clause_parse(ast).unwrap_or_default();

        // Create CachedQuery entry
        let cached_query = CachedQuery {
            fingerprint,
            table_name: table_name.to_owned(),
            relation_oid: table.relation_oid,
            filter_expr,
        };

        // Store cached query metadata
        self.cached_queries.insert_overwrite(cached_query);

        Ok(table.relation_oid)
    }

    /// Stores query results in the cache for faster retrieval.
    pub async fn query_cache_results(
        &self,
        table_oid: u32,
        rows: &[&SimpleQueryRow],
    ) -> Result<(), CacheError> {
        let table = self
            .tables
            .get1(&table_oid)
            .ok_or(CacheError::UnknownTable)?;

        let columns: Vec<&str> = Vec::from_iter(rows[0].columns().iter().map(|c| c.name()));
        for &row in rows {
            let mut params: Vec<Box<dyn ToSql + Sync>> = Vec::new();
            for idx in 0..row.columns().len() {
                let value = row.get(idx);
                let col = table
                    .columns
                    .get(row.columns()[idx].name())
                    .ok_or(CacheError::UnknownColumn)?;
                match col.data_type {
                    Type::BOOL => {
                        params.push(Box::new(value.and_then(|v| v.parse::<bool>().ok())));
                    }
                    Type::INT4 => {
                        params.push(Box::new(value.and_then(|v| v.parse::<i32>().ok())));
                    }
                    Type::OID => {
                        params.push(Box::new(value.and_then(|v| v.parse::<u32>().ok())));
                    }
                    Type::VARCHAR | Type::TEXT => {
                        params.push(Box::new(value));
                    }
                    _ => {
                        params.push(Box::new(value));
                    }
                };
            }

            let mut values: Vec<String> = Vec::new();
            for i in 0..params.len() {
                values.push(format!("${}", i + 1));
            }

            let pkey_columns = table.primary_key_columns.clone();

            let update_columns = columns
                .iter()
                .filter(|&&c| !pkey_columns.contains(&c.to_owned()))
                .map(|&c| format!("{c} = EXCLUDED.{c}"))
                .collect::<Vec<_>>();

            let mut insert_table = format!(
                "insert into {}({}) values (",
                table.name.as_str(),
                columns.join(",")
            );
            insert_table.push_str(&values.join(","));
            insert_table.push_str(") on conflict (");
            insert_table.push_str(&pkey_columns.join(","));
            insert_table.push_str(") do update set ");
            insert_table.push_str(&update_columns.join(", "));

            self.db_cache.execute_raw(&insert_table, params).await?;
        }

        Ok(())
    }

    async fn cache_table_create(&self, table_name: &str) -> Result<TableMetadata, CacheError> {
        let table = self.query_table_metadata(table_name).await?;
        self.cache_table_create_from_metadata(&table).await?;
        Ok(table)
    }

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
        let mut columns: HashMap<String, ColumnMetadata> = HashMap::with_capacity(rows.len());
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
            columns.insert(column.name.clone(), column);
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

    async fn cache_table_create_from_metadata(
        &self,
        table_metadata: &TableMetadata,
    ) -> Result<(), CacheError> {
        let mut columns = Vec::new();
        for column in table_metadata.columns.values() {
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
}

#[derive(Debug, Clone)]
pub struct CacheWorker {
    db_cache: Rc<Client>,
}

impl CacheWorker {
    pub async fn new(settings: &Settings) -> Result<Self, CacheError> {
        let (cache_client, cache_connection) = Config::new()
            .host(&settings.cache.host)
            .port(settings.cache.port)
            .user(&settings.cache.user)
            .dbname(&settings.cache.database)
            .connect(NoTls)
            .await?;

        //task to process connection to cache pg db
        tokio::spawn(async move {
            if let Err(e) = cache_connection.await {
                eprintln!("connection error: {e}");
            }
        });

        Ok(Self {
            db_cache: Rc::new(cache_client),
        })
    }

    #[instrument]
    pub async fn handle_cached_query(
        &self,
        data: &BytesMut,
        ast: &ParseResult,
    ) -> Result<BytesMut, CacheError> {
        let msg_len = (&data[1..5]).get_u32() as usize;
        let query = str::from_utf8(&data[5..msg_len]).map_err(|_| ParseError::InvalidUtf8)?;
        // let stmt = query_target.prepare(query).await.unwrap();
        let res = self.db_cache.simple_query(query).await?;
        // let res = query_target.query(query, &[]).await;
        // dbg!(&res);

        let mut buf = BytesMut::new();
        let SimpleQueryMessage::RowDescription(desc) = &res[0] else {
            return Err(CacheError::InvalidMessage);
        };

        row_description_encode(desc, &mut buf);

        let mut rows = Vec::new();
        for msg in &res[1..(res.len() - 1)] {
            match msg {
                SimpleQueryMessage::Row(row) => {
                    simple_query_row_encode(row, &mut buf);
                    rows.push(row);
                }
                _ => return Err(CacheError::InvalidMessage),
            }
        }

        let SimpleQueryMessage::CommandComplete(cnt) = &res[res.len() - 1] else {
            return Err(CacheError::InvalidMessage);
        };
        command_complete_encode(*cnt, &mut buf);
        ready_for_query_encode(&mut buf);

        debug!("cache hit");
        Ok(buf)
    }
}
