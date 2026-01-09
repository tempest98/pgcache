# ADR-009: Client TLS Support

## Status
Proposed

## Context

pgcache acts as a transparent proxy between PostgreSQL clients and an origin database. Clients connect to pgcache, which then forwards queries to the origin (or serves cached results). To support secure connections, pgcache needs to handle TLS encryption for client connections.

The challenge is that pgcache has a multi-threaded architecture where:
1. **Connection handler threads** (`cnxt N`) handle client ↔ origin message routing using async I/O
2. **Cache worker thread** writes cached query results directly to clients using synchronous blocking I/O

When a client uses TLS, both threads need to write encrypted data to the same TLS session. This requires careful coordination of the TLS encryption state.

## Decision

### Approach: "Encrypt in cache worker, raw-write the ciphertext"

We separate TLS **encryption state** from TLS **I/O operations**:

1. The connection handler performs TLS negotiation and handshake
2. After handshake, we decompose the TLS stream into:
   - The underlying TCP stream (for I/O)
   - The rustls `ServerConnection` (encryption state)
3. The encryption state is shared via `Arc<Mutex<ServerConnection>>`
4. Both threads can encrypt data using the shared state, then write raw ciphertext to the socket

### Key Components

#### 1. SharedTlsState
```rust
pub type SharedTlsState = Arc<std::sync::Mutex<rustls::ServerConnection>>;
```

Uses `std::sync::Mutex` (not tokio) because the cache worker thread has its own blocking runtime. The mutex is held briefly only during encryption operations.

#### 2. CacheClientWriterSource

Captures the information needed to create writers on demand:
```rust
pub enum CacheClientWriterSource {
    Plain { fd: OwnedFd },
    Tls { fd: OwnedFd, tls_state: SharedTlsState },
}
```

- Created BEFORE splitting the client stream (needs access to raw fd)
- Uses `libc::dup()` to get an independent file descriptor
- Can be cloned and sent to the cache worker
- Creates `CacheClientWriter` instances on demand

#### 3. CacheClientWriter

Performs synchronous blocking writes from the cache worker:
```rust
pub struct CacheClientWriter {
    fd: OwnedFd,
    tls_state: Option<SharedTlsState>,
}

impl CacheClientWriter {
    /// Write buffer with write_all_buf semantics (tracks position for cancel-safety)
    pub fn write_all_buf(&mut self, buf: &mut impl Buf) -> io::Result<()>;
}
```

For plaintext: writes directly to the fd.
For TLS: encrypts using the shared state, then writes ciphertext to the fd.

#### 4. ClientStream

Abstracts over connection types for the connection handler:
```rust
pub enum ClientStream {
    Plain(TcpStream),
    Tls(Box<SharedTlsClientStream>),
}
```

#### 5. SharedTlsClientStream

Wraps TCP stream + shared TLS state for async operations:
```rust
pub struct SharedTlsClientStream {
    tcp: TcpStream,
    tls_state: SharedTlsState,
    read_plaintext_buf: Vec<u8>,  // Buffer for decrypted data
}
```

Implements `AsyncRead` and `AsyncWrite`:
- **Read**: Reads ciphertext from TCP, decrypts via TLS state, returns plaintext
- **Write**: Encrypts plaintext via TLS state, writes ciphertext to TCP

### TLS Negotiation Flow

```
Client                    pgcache                     Origin
  |                          |                          |
  |--- SSLRequest (8 bytes) -->|                        |
  |                          |                          |
  |<-------- 'S' -----------|  (if TLS configured)     |
  |                          |                          |
  |<==== TLS Handshake ====>|                          |
  |                          |                          |
  | (now TLS encrypted)      |                          |
  |--- StartupMessage ------>|                          |
  |                          |--- StartupMessage ------>|
  ...
```

**Critical**: TLS negotiation MUST happen BEFORE framing the stream because:
1. We need to peek at raw bytes to detect SSLRequest
2. After handshake, we decompose the TlsStream to extract the ServerConnection
3. Only then do we wrap the stream for framed message parsing

### Implementation in client_tls_negotiate()

```rust
pub async fn client_tls_negotiate(
    mut stream: TcpStream,
    acceptor: Option<&TlsAcceptor>,
) -> Result<ClientTlsResult, TlsError> {
    // 1. Peek at first 8 bytes to check for SSLRequest
    let mut peek_buf = [0u8; 8];
    let n = stream.peek(&mut peek_buf).await?;

    if n >= 8 && peek_buf == SSL_REQUEST {
        // 2. Consume the SSLRequest
        stream.read_exact(&mut [0u8; 8]).await?;

        if let Some(acceptor) = acceptor {
            // 3. Accept SSL - respond 'S'
            stream.write_all(b"S").await?;

            // 4. Perform TLS handshake
            let tls_stream = acceptor.accept(stream).await?;

            // 5. Decompose into TCP stream + encryption state
            let (tcp_stream, server_connection) = tls_stream.into_inner();
            let tls_state = Arc::new(Mutex::new(server_connection));

            return Ok(ClientTlsResult::Tls { tcp_stream, tls_state });
        } else {
            // TLS not configured - respond 'N'
            stream.write_all(b"N").await?;
        }
    }

    // No SSLRequest or TLS declined
    Ok(ClientTlsResult::Plain(stream))
}
```

### Connection Handler Flow

```rust
// In connection worker thread
let client_stream = match client_tls_negotiate(socket, tls_acceptor).await {
    Ok(ClientTlsResult::Tls { tcp_stream, tls_state }) => {
        ClientStream::Tls(Box::new(SharedTlsClientStream::new(tcp_stream, tls_state)))
    }
    Ok(ClientTlsResult::Plain(stream)) => {
        ClientStream::Plain(stream)
    }
    Err(e) => { /* handle error */ }
};

// Create writer source BEFORE splitting stream
let client_writer_source = client_stream.writer_source_create();

// Connect to origin, setup streams, enter message loop
// ...

// When sending to cache worker:
let msg = CacheMessage::Query(data, client_writer_source.writer_create());
cache_tx.send(msg);
```

### Cache Worker Write Flow

```rust
// In cache worker thread
fn write_to_client(writer: &mut CacheClientWriter, data: &[u8]) -> io::Result<()> {
    let mut buf = Bytes::from(data.to_vec());
    writer.write_all_buf(&mut buf)
}
```

For TLS connections, `write_all_buf`:
1. Locks the shared TLS state (briefly)
2. Writes plaintext to `tls.writer()`
3. Extracts ciphertext via `tls.write_tls()`
4. Releases the lock
5. Writes ciphertext to the raw fd using blocking I/O

## Consequences

### Positive
- Clean separation between encryption and I/O
- Cache worker can write encrypted data without async runtime
- Minimal lock contention (mutex held only during encryption, not I/O)
- Supports both TLS and plaintext connections uniformly

### Negative
- Additional complexity in stream management
- Must coordinate TLS state access between threads
- File descriptor duplication adds resource overhead

### Risks
- Mutex poisoning if a thread panics while holding the lock
- Must ensure TLS state and TCP writes remain synchronized (no interleaved writes from different threads without coordination)

## File Descriptor Handling

The cache worker needs its own file descriptor to write to the socket:

```rust
impl CacheClientWriterSource {
    pub fn new_plain(fd: RawFd) -> Self {
        // Duplicate fd so cache worker has independent ownership
        let dup_fd = unsafe { libc::dup(fd) };
        Self::Plain { fd: unsafe { OwnedFd::from_raw_fd(dup_fd) } }
    }
}
```

This allows:
- Connection handler to continue async reads/writes on original fd
- Cache worker to perform blocking writes on duplicated fd
- Both to operate independently (kernel handles synchronization)

## Constants

```rust
/// PostgreSQL SSLRequest message (8 bytes)
/// Format: 4-byte length (8) + 4-byte code (80877103)
const SSL_REQUEST: &[u8] = &[0x00, 0x00, 0x00, 0x08, 0x04, 0xd2, 0x16, 0x2f];
```

## Testing Notes

- Test with `NoTls` client: Should see startup message bytes (not SSLRequest) on peek
- Test with TLS client: Should see SSLRequest, respond 'S', complete handshake
- Verify cache worker can write encrypted responses
- Verify connection handler async reads work with shared TLS state
