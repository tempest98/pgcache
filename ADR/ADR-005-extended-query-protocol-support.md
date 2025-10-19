# ADR-005: Extended Query Protocol Support

## Status
Accepted

## Context

PostgreSQL supports two wire protocol modes for executing queries:

**Simple Query Protocol**: Single message contains complete SQL text with literal values embedded. Client sends Query message, server responds with results. No support for parameterized queries.

**Extended Query Protocol**: Multi-phase protocol (Parse → Bind → Execute) that separates SQL template from parameter values. Supports prepared statements, parameterized queries, and statement reuse across executions.

pgcache initially only supported simple query protocol. To cache parameterized queries from modern PostgreSQL clients (psycopg3, tokio-postgres, JDBC, etc.), extended protocol support was required.

## Decision

Implement full extended query protocol support with the following approach:

1. **Connection-scoped state tracking**: Add `prepared_statements` and `portals` HashMaps to `ConnectionState` to track Parse/Bind lifecycle per connection
2. **Cacheability analysis at Parse time**: Analyze SQL template during Parse message handling, store result in `PreparedStatement`
3. **AST-based parameter replacement**: When executing cacheable queries, replace `LiteralValue::Parameter` nodes in the AST with actual bound parameter values
4. **Early parameter replacement**: Perform parameter replacement when creating `QueryRequest` in cache pipeline, ensuring downstream code receives fully-resolved queries
5. **Lifecycle management**: Unnamed statements/portals use empty string as key and are cleaned up on transaction boundaries; named statements persist until explicitly closed

## Rationale

**Parse-Time Cacheability Analysis**: SQL template structure determines cacheability, not parameter values. Analyzing once during Parse and storing the result in PreparedStatement avoids re-parsing on each Execute.

**AST-Based Parameter Replacement**: Using AST transformation instead of string substitution provides type safety and preserves query structure. Works naturally with existing AST-based query processing.

**Early Replacement in Pipeline**: Performing replacement when creating QueryRequest (not during dispatch) follows single responsibility principle - QueryRequest always contains resolved queries, simplifying downstream logic and allowing query fingerprinting to work uniformly for both simple and extended protocol queries.

**Connection-Scoped State**: Extended protocol is inherently stateful per connection. Tracking in `ConnectionState` provides clear ownership and lifecycle management.

## Consequences

### Positive
- Modern PostgreSQL clients can connect and execute parameterized queries through the cache
- Cacheability determined once per prepared statement, not per execution
- Prepared statements properly cached with parameter values incorporated into cache keys
- Clean separation between protocol state management (ConnectionState) and cache logic (QueryCache)

### Negative
- Increased memory per connection (HashMaps for statements/portals)
- Additional complexity in message handling and state lifecycle management
- Text format parameters only initially (binary format deferred to future work)

### Implementation Notes

**State Cleanup**: On ReadyForQuery with status 'I' (idle, not in transaction), unnamed portals are cleaned up via `self.portals.retain(|name, _| !name.is_empty())`. Named statements/portals persist until explicit Close message.

**Parameter Replacement**: `ast_parameters_replace()` in `query/transform.rs` walks the AST and mutates `LiteralValue::Parameter` nodes to concrete literal types (String, Integer, etc.). Occurs in `cache_run()` when handling `CacheMessage::QueryParameterized`.

**Cache Integration**: Execute message checks if portal's associated statement is cacheable. If yes, sends `CacheMessage::QueryParameterized` to cache subsystem. Parameters are replaced early in the cache pipeline before query dispatch.
