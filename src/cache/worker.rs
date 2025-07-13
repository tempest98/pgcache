use std::rc::Rc;

use pg_query::ParseResult;
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage};
use tokio_util::bytes::{Buf, BytesMut};
use tracing::{debug, instrument};

use crate::pg::protocol::encode::*;
use crate::settings::Settings;

use super::*;

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
                error!("connection error: {e}");
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
