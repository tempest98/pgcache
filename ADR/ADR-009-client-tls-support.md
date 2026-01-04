# ADR-009: Client TLS Support

## Status
Accepted

## Context

pgcache acts as a transparent proxy that accepts connections from PostgreSQL clients. To support secure connections, pgcache needs to handle TLS encryption for incoming client connections.

The challenge is that pgcache has a multi-threaded architecture where both the connection handler thread and the cache worker thread need to write to the same client socket. When using TLS, both threads must share access to the TLS encryption state.

## Decision

- Separate TLS encryption state from I/O operations, sharing the encryption state via `Arc<Mutex<ServerConnection>>`
- After TLS handshake, decompose the TLS stream into the raw TCP stream and the rustls connection object
- Use file descriptor duplication (`libc::dup`) to allow both threads to write to the same socket independently
- Create a generic `TlsStream<T>` type that works for both client connections (ServerConnection) and origin connections (ClientConnection) via a `TlsConnectionOps` trait
- Use `std::sync::Mutex` (not tokio) since the lock is held briefly during encryption only

## Rationale

- **Shared encryption state**: Both threads need to encrypt data using the same TLS session; sharing the rustls connection via Arc<Mutex> is the cleanest approach
- **fd duplication**: Allows the cache worker to have its own TcpStream handle without interfering with the connection handler's stream
- **Generic TlsStream<T>**: Avoids code duplication between client-side and origin-side TLS handling
- **std::sync::Mutex over tokio::Mutex**: The mutex is only held during CPU-bound encryption operations (microseconds), not during I/O, so blocking is acceptable

## Consequences

### Positive
- Clean separation between encryption logic and I/O operations
- Generic implementation reusable for both client and origin TLS
- Minimal lock contention since mutex is held only during encryption, not I/O
- Both threads can write encrypted data independently

### Negative
- Additional complexity in stream management
- File descriptor duplication adds resource overhead
- Risk of mutex poisoning if a thread panics while holding the lock

## Implementation Notes

`ClientSocketSource` captures the raw fd and TLS state before stream splitting, then creates `ClientSocket` instances on demand for the cache worker. The `TlsConnectionOps` trait abstracts over rustls ServerConnection and ClientConnection.
