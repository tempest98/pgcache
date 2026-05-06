use std::io;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_util::bytes::{BufMut, BytesMut};
use tokio_util::codec::Decoder;
use tracing::debug;

use crate::cache::{CacheError, CacheResult, MapIntoReport};
use crate::settings::PgSettings;

use super::protocol::backend::{AUTHENTICATION_OK, PgBackendMessageCodec, PgBackendMessageType};
use super::protocol::frontend::simple_query_message_build;

/// Raw TCP connection to the cache database with PG protocol framing.
///
/// Avoids per-row overhead of tokio-postgres by providing direct access
/// to the underlying stream and codec for zero-copy frame forwarding.
pub struct CacheConnection {
    pub stream: TcpStream,
    pub read_buf: BytesMut,
    pub codec: PgBackendMessageCodec,
    /// Recycled SQL assembly buffer. The worker clears and rewrites this on every
    /// cache hit (SET generation prefix + precomputed body + optional LIMIT),
    /// avoiding per-request String allocations.
    pub sql_buf: String,
}

impl CacheConnection {
    /// Connect to the cache database and complete the PG startup handshake.
    /// Assumes trust authentication (no password exchange).
    pub async fn connect(settings: &PgSettings) -> CacheResult<Self> {
        let addr = format!("{}:{}", settings.host, settings.port);
        let stream = TcpStream::connect(&addr)
            .await
            .map_into_report::<CacheError>()?;
        let _ = stream.set_nodelay(true);

        let mut conn = Self {
            stream,
            read_buf: BytesMut::with_capacity(64 * 1024),
            codec: PgBackendMessageCodec::default(),
            sql_buf: String::with_capacity(1024),
        };

        // Send startup message
        let startup = startup_message_build(&settings.user, &settings.database);
        conn.stream
            .write_all(&startup)
            .await
            .map_into_report::<CacheError>()?;

        // Read until ReadyForQuery — trust auth sends:
        // AuthenticationOk → ParameterStatus* → BackendKeyData → ReadyForQuery
        conn.startup_handshake().await?;

        debug!(
            "cache connection established to {}:{}",
            settings.host, settings.port
        );
        Ok(conn)
    }

    /// Read startup responses until ReadyForQuery is received.
    async fn startup_handshake(&mut self) -> CacheResult<()> {
        use tokio::io::AsyncReadExt;

        loop {
            // Try decoding from existing buffer
            while let Some(msg) = self
                .codec
                .decode(&mut self.read_buf)
                .map_err(|_| CacheError::InvalidMessage)?
            {
                #[allow(clippy::wildcard_enum_match_arm)]
                match msg.message_type {
                    PgBackendMessageType::Authentication => {
                        // Verify it's AuthenticationOk (auth type at bytes 5..9)
                        let auth_type = msg
                            .data
                            .get(5..9)
                            .and_then(|b| b.try_into().ok())
                            .map(i32::from_be_bytes)
                            .unwrap_or(-1);
                        if auth_type != AUTHENTICATION_OK {
                            return Err(CacheError::InvalidMessage.into());
                        }
                    }
                    PgBackendMessageType::ReadyForQuery => {
                        return Ok(());
                    }
                    PgBackendMessageType::ErrorResponse => {
                        return Err(CacheError::InvalidMessage.into());
                    }
                    // Skip ParameterStatus, BackendKeyData, NegotiateProtocolVersion, etc.
                    _ => {}
                }
            }

            // Need more data
            let n = self
                .stream
                .read_buf(&mut self.read_buf)
                .await
                .map_into_report::<CacheError>()?;
            if n == 0 {
                return Err(CacheError::IoError(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed during startup",
                ))
                .into());
            }
        }
    }

    /// Send `self.sql_buf` as a simple query message. Callers build the SQL
    /// into the recycled buffer; this sidesteps split-borrow issues a
    /// `&str`-taking variant would hit when the buffer lives on `self`.
    pub async fn simple_query_send(&mut self) -> CacheResult<()> {
        let msg = simple_query_message_build(&self.sql_buf);
        self.stream
            .write_all(&msg)
            .await
            .map_into_report::<CacheError>()
    }

    /// Send a pipelined SET (simple query) + SELECT (extended query with binary
    /// results). The SELECT SQL comes from `self.sql_buf`.
    ///
    /// Pipelines all messages in a single write:
    /// - Q: SET query
    /// - P: Parse (unnamed statement, SQL, no params)
    /// - B: Bind (unnamed portal/stmt, binary result format)
    /// - D: Describe (unnamed portal) — only when `include_describe` is true
    /// - E: Execute (unnamed portal, no row limit)
    /// - S: Sync
    pub async fn pipelined_binary_query_send(
        &mut self,
        set_sql: &str,
        include_describe: bool,
    ) -> CacheResult<()> {
        let set_msg = simple_query_message_build(set_sql);
        let ext_msg = extended_query_binary_build(&self.sql_buf, include_describe);

        let mut combined = BytesMut::with_capacity(set_msg.len() + ext_msg.len());
        combined.extend_from_slice(&set_msg);
        combined.extend_from_slice(&ext_msg);

        self.stream
            .write_all(&combined)
            .await
            .map_into_report::<CacheError>()
    }

    /// Extended-only variant of `pipelined_binary_query_send` for MV reads that
    /// don't need the generation SET (MV tables are not `pgcache_pgrx`-tracked).
    pub async fn extended_binary_query_send(
        &mut self,
        select_sql: &str,
        include_describe: bool,
    ) -> CacheResult<()> {
        let ext_msg = extended_query_binary_build(select_sql, include_describe);
        self.stream
            .write_all(&ext_msg)
            .await
            .map_into_report::<CacheError>()
    }
}

/// Build a PG startup message (protocol v3.0).
///
/// Format: int32 len | int32 protocol_version(196608) | key\0value\0 pairs | \0
fn startup_message_build(user: &str, database: &str) -> BytesMut {
    // Calculate total length
    let body_len = 4 // protocol version
        + 5 + user.len() + 1      // "user\0" + user + \0
        + 9 + database.len() + 1   // "database\0" + database + \0
        + 1; // final \0 terminator
    let total_len = 4 + body_len; // 4 for the length field itself
    let total_len_i32 = i32::try_from(total_len).expect("startup message fits in i32");

    let mut buf = BytesMut::with_capacity(total_len);
    buf.put_i32(total_len_i32);
    buf.put_i32(196608); // Protocol 3.0
    buf.put_slice(b"user\0");
    buf.put_slice(user.as_bytes());
    buf.put_u8(0);
    buf.put_slice(b"database\0");
    buf.put_slice(database.as_bytes());
    buf.put_u8(0);
    buf.put_u8(0); // terminator
    buf
}

/// Build Parse + Bind + [Describe('P')] + Execute + Sync messages for binary result format.
///
/// Uses unnamed statement and portal, no parameters, binary result format.
/// When `include_describe` is true, inserts a Describe('P') message between Bind and Execute
/// so the cache DB returns a RowDescription that can be forwarded to the client.
fn extended_query_binary_build(sql: &str, include_describe: bool) -> BytesMut {
    let sql_bytes = sql.as_bytes();

    // Parse: 'P' | int32 len | \0 (unnamed) | sql\0 | int16 0 (no param types)
    let parse_len: i32 = i32::try_from(4 + 1 + sql_bytes.len() + 1 + 2)
        .expect("Parse message fits in i32");

    // Bind: 'B' | int32 len | \0 (portal) | \0 (stmt) | int16 0 (param formats)
    //       | int16 0 (params) | int16 1 (result format count) | int16 1 (binary)
    let bind_len: i32 = 4 + 1 + 1 + 2 + 2 + 2 + 2; // 14

    // Describe: 'D' | int32 len | 'P' | \0 (unnamed portal)
    let describe_len: i32 = 4 + 1 + 1; // 6

    // Execute: 'E' | int32 len | \0 (portal) | int32 0 (no limit)
    let execute_len: i32 = 4 + 1 + 4; // 9

    // Sync: 'S' | int32 4
    let sync_len: i32 = 4;

    let describe_total = if include_describe { 1 + describe_len } else { 0 };
    let total = 1 + parse_len + 1 + bind_len + describe_total + 1 + execute_len + 1 + sync_len;
    let mut buf = BytesMut::with_capacity(usize::try_from(total).expect("non-negative size"));

    // Parse
    buf.put_u8(b'P');
    buf.put_i32(parse_len);
    buf.put_u8(0); // unnamed statement
    buf.put_slice(sql_bytes);
    buf.put_u8(0); // null-terminate SQL
    buf.put_i16(0); // no parameter types

    // Bind
    buf.put_u8(b'B');
    buf.put_i32(bind_len);
    buf.put_u8(0); // unnamed portal
    buf.put_u8(0); // unnamed statement
    buf.put_i16(0); // no parameter format codes
    buf.put_i16(0); // no parameters
    buf.put_i16(1); // one result format code
    buf.put_i16(1); // binary format

    // Describe (optional)
    if include_describe {
        buf.put_u8(b'D');
        buf.put_i32(describe_len);
        buf.put_u8(b'P'); // describe portal
        buf.put_u8(0); // unnamed portal
    }

    // Execute
    buf.put_u8(b'E');
    buf.put_i32(execute_len);
    buf.put_u8(0); // unnamed portal
    buf.put_i32(0); // no row limit

    // Sync
    buf.put_u8(b'S');
    buf.put_i32(sync_len);

    buf
}
