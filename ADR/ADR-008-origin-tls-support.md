# ADR-008: Origin TLS Support

## Status
Accepted (Implemented)

## Context

pgcache connects to origin PostgreSQL databases to forward queries and receive results. Many production PostgreSQL deployments require TLS encryption. pgcache needs to:

1. Support configurable SSL modes (disable, prefer, require)
2. Integrate with tokio-postgres for async database connections
3. Handle PostgreSQL's SSL negotiation protocol
4. Verify server certificates using standard root CAs

## Decision

### SSL Mode Configuration

Support PostgreSQL-standard SSL modes via settings:

```rust
pub enum SslMode {
    Disable,  // Never use SSL
    Prefer,   // Try SSL, fall back to plaintext
    Require,  // SSL required, fail if unavailable
}
```

### TLS Library Choice: rustls

Use rustls instead of OpenSSL because:
- Pure Rust implementation (no C dependencies)
- Memory-safe by design
- Good async integration via tokio-rustls
- Simpler build/deployment (no system OpenSSL version issues)

### Certificate Verification

Use webpki-roots for standard CA certificates:

```rust
pub fn tls_config_build() -> Arc<ClientConfig> {
    let root_store = RootCertStore::from_iter(
        webpki_roots::TLS_SERVER_ROOTS.iter().cloned()
    );

    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Arc::new(config)
}
```

This trusts the standard set of root CAs (same as browsers/curl). No client certificate authentication is used.

### PostgreSQL SSL Negotiation Protocol

PostgreSQL uses a special handshake before TLS:

```
pgcache                     Origin PostgreSQL
   |                              |
   |--- SSLRequest (8 bytes) ---->|
   |                              |
   |<-------- 'S' or 'N' ---------|
   |                              |
   | (if 'S': TLS handshake)      |
   |<======= TLS Handshake ======>|
   |                              |
   | (now encrypted)              |
   |--- StartupMessage ---------->|
   ...
```

#### SSLRequest Message Format
```rust
/// PostgreSQL SSLRequest message (8 bytes)
/// Format: 4-byte length (8) + 4-byte code (80877103)
const SSL_REQUEST: &[u8] = &[0x00, 0x00, 0x00, 0x08, 0x04, 0xd2, 0x16, 0x2f];
```

#### Server Response
- `'S'` (0x53): Server accepts SSL, proceed with TLS handshake
- `'N'` (0x4E): Server does not support SSL

### Implementation: pg_tls_connect()

```rust
pub async fn pg_tls_connect(
    mut stream: TcpStream,
    server_name: &str,
) -> Result<RustlsTlsStream<TcpStream>, TlsError> {
    // 1. Send SSLRequest
    stream.write_all(SSL_REQUEST).await?;

    // 2. Read response (single byte)
    let mut response = [0u8; 1];
    stream.read_exact(&mut response).await?;

    match response[0] {
        b'S' => {
            // 3. Server accepts SSL - perform TLS handshake
            let config = tls_config_build();
            let connector = TlsConnector::from(config);
            let server_name: ServerName<'_> = server_name.to_owned().try_into()?;
            let tls_stream = connector.connect(server_name, stream).await?;
            Ok(tls_stream)
        }
        b'N' => Err(TlsError::SslNotSupported),
        byte => Err(TlsError::UnexpectedResponse { byte }),
    }
}
```

### tokio-postgres Integration

tokio-postgres uses the `MakeTlsConnect` trait for TLS. We implement:

#### MakeRustlsConnect
Factory that creates TLS connectors for each connection:

```rust
#[derive(Clone)]
pub struct MakeRustlsConnect {
    config: Arc<ClientConfig>,
}

impl<S> MakeTlsConnect<S> for MakeRustlsConnect
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = RustlsStream<S>;
    type TlsConnect = RustlsConnect;
    type Error = InvalidDnsNameError;

    fn make_tls_connect(&mut self, hostname: &str) -> Result<Self::TlsConnect, Self::Error> {
        let server_name = ServerName::try_from(hostname.to_owned())?;
        Ok(RustlsConnect {
            config: Arc::clone(&self.config),
            server_name,
        })
    }
}
```

#### RustlsConnect
Performs the actual TLS connection:

```rust
pub struct RustlsConnect {
    config: Arc<ClientConfig>,
    server_name: ServerName<'static>,
}

impl<S> TlsConnect<S> for RustlsConnect
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = RustlsStream<S>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send>>;

    fn connect(self, stream: S) -> Self::Future {
        Box::pin(async move {
            let connector = TlsConnector::from(self.config);
            let tls_stream = connector
                .connect(self.server_name, stream)
                .await
                .map_err(io::Error::other)?;
            Ok(RustlsStream(tls_stream))
        })
    }
}
```

#### RustlsStream
Wrapper implementing tokio-postgres's TlsStream trait:

```rust
pub struct RustlsStream<S>(RustlsTlsStream<S>);

impl<S> TlsStream for RustlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn channel_binding(&self) -> ChannelBinding {
        // Return none for simplicity
        // Full impl would compute SHA-256 of server cert for SCRAM-SHA-256-PLUS
        ChannelBinding::none()
    }
}
```

### Connection Flow in Proxy

```rust
async fn origin_connect(
    addrs: &[SocketAddr],
    ssl_mode: SslMode,
    server_name: &str,
) -> Result<OriginStream, ConnectionError> {
    for addr in addrs {
        let stream = TcpStream::connect(addr).await?;

        match ssl_mode {
            SslMode::Disable => {
                return Ok(OriginStream::Plain(stream));
            }
            SslMode::Require => {
                let tls_stream = pg_tls_connect(stream, server_name).await?;
                return Ok(OriginStream::Tls(tls_stream));
            }
            SslMode::Prefer => {
                match pg_tls_connect(stream, server_name).await {
                    Ok(tls_stream) => return Ok(OriginStream::Tls(tls_stream)),
                    Err(TlsError::SslNotSupported) => {
                        // Reconnect without TLS
                        let stream = TcpStream::connect(addr).await?;
                        return Ok(OriginStream::Plain(stream));
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
    }

    Err(ConnectionError::NoAddressesAvailable)
}
```

### OriginStream Abstraction

```rust
pub enum OriginStream {
    Plain(TcpStream),
    Tls(RustlsTlsStream<TcpStream>),
}

impl AsyncRead for OriginStream { /* delegate to inner */ }
impl AsyncWrite for OriginStream { /* delegate to inner */ }
```

## Consequences

### Positive
- Standard TLS support for connecting to production PostgreSQL
- Compatible with cloud-hosted databases (RDS, Cloud SQL, Neon, etc.)
- No OpenSSL dependency simplifies builds
- Async-native implementation

### Negative
- No client certificate authentication (could be added later)
- Channel binding not implemented (minor security feature)
- webpki-roots may not include all enterprise CAs

### Future Considerations
- Add support for custom CA certificates
- Implement client certificate authentication
- Add channel binding for SCRAM-SHA-256-PLUS
- Support certificate pinning for high-security deployments

## Error Handling

```rust
error_set! {
    TlsError := {
        IoError(io::Error),
        RustlsError(rustls::Error),
        InvalidDnsName(InvalidDnsNameError),

        #[display("Server does not support SSL/TLS")]
        SslNotSupported,

        #[display("Unexpected response during SSL negotiation: {byte}")]
        UnexpectedResponse { byte: u8 },
    }
}
```

## Dependencies

```toml
[dependencies]
rustls = "0.23"
tokio-rustls = "0.26"
webpki-roots = "0.26"
rustls-pemfile = "2"  # For loading certs/keys from PEM files
```

## Testing

- Test against PostgreSQL with `ssl = on`
- Test against PostgreSQL with `ssl = off` (verify SslNotSupported error)
- Test SslMode::Prefer fallback behavior
- Test invalid server names (certificate verification failure)
- Test against cloud databases (Neon, RDS) to verify real-world compatibility
