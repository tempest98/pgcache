# ADR-008: Origin TLS Support

## Status
Accepted

## Context

pgcache connects to origin PostgreSQL databases to forward queries and receive results. Many production PostgreSQL deployments require TLS encryption, including cloud-hosted databases like Neon, RDS, and Cloud SQL.

PostgreSQL uses a non-standard TLS negotiation protocol where the client sends an SSLRequest message before the TLS handshake begins. Standard TLS proxies cannot handle this flow.

## Decision

- Support two SSL modes: `Disable` (default, no TLS) and `Require` (TLS required, fail if unavailable)
- Use rustls as the TLS library instead of OpenSSL
- Use webpki-roots for standard CA certificate verification (same trust store as browsers)
- Implement PostgreSQL's SSLRequest negotiation protocol before the TLS handshake
- Implement tokio-postgres's `MakeTlsConnect` trait for integration with the database client library

## Rationale

- **rustls over OpenSSL**: Pure Rust implementation eliminates C dependencies, simplifies builds and deployment, and provides memory safety guarantees
- **webpki-roots**: Standard root CA bundle provides compatibility with cloud-hosted databases without custom certificate configuration
- **No client certificate auth**: Simplifies initial implementation; most PostgreSQL deployments use password authentication
- **No channel binding**: SCRAM-SHA-256-PLUS channel binding is a minor security feature that can be added later if needed

## Consequences

### Positive
- Compatible with cloud-hosted databases (Neon, RDS, Cloud SQL, etc.)
- No OpenSSL dependency simplifies builds and cross-compilation
- Async-native implementation integrates cleanly with tokio
- Standard CA verification works out of the box

### Negative
- No client certificate authentication support
- webpki-roots may not include enterprise/internal CAs
- Channel binding not implemented (minor security feature for SCRAM-SHA-256-PLUS)

## Implementation Notes

The `pg_tls_connect()` function handles PostgreSQL's SSLRequest protocol, and `MakeRustlsConnect` implements the tokio-postgres TLS trait. Certificate and key loading utilities support server-side TLS configuration.
