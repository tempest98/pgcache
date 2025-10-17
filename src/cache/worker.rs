use std::rc::Rc;

use tokio::io::AsyncWriteExt;
use tokio_postgres::{Client, Config, NoTls, SimpleQueryMessage};
use tokio_util::bytes::{Buf, BufMut, BytesMut};
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

    #[instrument(skip_all)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn handle_cached_query(&self, msg: &mut QueryRequest) -> Result<(), CacheError> {
        const BUFFER_SIZE_THRESHOLD: usize = 64 * 1024;

        let data = &msg.data;
        let msg_len = (&data[1..5]).get_u32() as usize;
        let query = str::from_utf8(&data[5..msg_len]).map_err(|_| ParseError::InvalidUtf8)?;
        // let stmt = query_target.prepare(query).await.unwrap();
        let res = self.db_cache.simple_query(query).await?;

        let SimpleQueryMessage::RowDescription(desc) = &res[0] else {
            return Err(CacheError::InvalidMessage);
        };

        let mut buf = BytesMut::new();
        row_description_encode(desc, &mut buf);
        if msg.client_socket.write_all_buf(&mut buf).await.is_err() {
            error!("no client");
            return Err(CacheError::Write);
        }

        buf.clear();
        for query_msg in &res[1..(res.len() - 1)] {
            match query_msg {
                SimpleQueryMessage::Row(row) => {
                    // Use raw buffer directly to avoid decode/encode overhead
                    // The raw buffer contains field data but not the field count
                    let raw_data = row.raw_buffer_bytes();
                    let field_count = row.len() as u16;

                    buf.put_u8(b'D'); // DATA_ROW_TAG
                    buf.put_i32(4 + 2 + raw_data.len() as i32); // 4 (length field) + 2 (field count) + data
                    buf.put_u16(field_count);
                    buf.put_slice(raw_data);
                }
                _ => return Err(CacheError::InvalidMessage),
            }

            //send data if more than 64kB have been accumulated
            if buf.len() > BUFFER_SIZE_THRESHOLD
                && msg.client_socket.write_all_buf(&mut buf).await.is_err()
            {
                error!("no client");
                return Err(CacheError::Write);
            }
        }
        if msg.client_socket.write_all_buf(&mut buf).await.is_err() {
            error!("no client");
            return Err(CacheError::Write);
        }

        let SimpleQueryMessage::CommandComplete(cnt) = &res[res.len() - 1] else {
            return Err(CacheError::InvalidMessage);
        };

        let mut buf = BytesMut::new();
        command_complete_encode(*cnt, &mut buf);

        ready_for_query_encode(&mut buf);

        if msg.client_socket.write_all_buf(&mut buf).await.is_err() {
            error!("no client");
            return Err(CacheError::Write);
        }

        debug!("cache hit");
        Ok(())
    }
}
