# Extended Query Protocol Implementation Plan

## Overview

This document outlines the plan to add extended query protocol support to pgcache. The extended query protocol allows for prepared statements, parameterized queries, and more efficient query execution.

## Current State

pgcache currently only supports the **simple query protocol**:
- Handles `Query` message type (message tag `'Q'`)
- Extracts SQL text directly from the message
- Parses SQL using `pg_query::parse()`
- Analyzes for cacheability and routes accordingly

The protocol handling code in `pgcache/src/pg/protocol/frontend.rs` already recognizes extended protocol messages (`Parse`, `Bind`, `Execute`, `Describe`, `Close`, `Flush`, `Sync`) but doesn't process them - they're just passed through to the origin.

## Protocol Comparison

### Simple Query Protocol
- Single message contains complete SQL text
- Returns all results in text format
- Single round-trip: Query → RowDescription → DataRow(s) → CommandComplete → ReadyForQuery
- No parameter separation from SQL

### Extended Query Protocol
- Multi-phase: Parse → Bind → Execute
- Parameters sent separately from SQL text
- Binary and text format support for parameters and results
- Supports statement reuse across executions
- Enables pipelining multiple operations

## Message Flow

### Extended Protocol Message Types

**Frontend Messages (Client → Server):**
- **Parse ('P')**: Prepares a statement (SQL + optional parameter types)
- **Bind ('B')**: Binds parameters to a prepared statement, creates portal
- **Execute ('E')**: Executes a bound portal
- **Describe ('D')**: Requests metadata about a statement or portal
- **Close ('C')**: Closes a prepared statement or portal
- **Sync ('S')**: Synchronization point, marks transaction boundary

**Backend Response Messages (Server → Client):**
- **ParseComplete ('1')**: Confirms successful parsing
- **BindComplete ('2')**: Confirms successful binding
- **RowDescription ('T')**: Provides column metadata for query results
- **DataRow ('D')**: Contains actual result data
- **CommandComplete ('C')**: Indicates command completion with row count
- **ParameterDescription ('t')**: Describes prepared statement parameters
- **NoData ('n')**: Indicates no data will be returned
- **ReadyForQuery ('Z')**: Signals readiness for new commands

### Example Flow

1. Client sends Parse: `SELECT * FROM users WHERE id = $1`
2. Server responds: ParseComplete
3. Client sends Bind: with parameter value `[42]`
4. Server responds: BindComplete
5. Client sends Execute
6. Server responds: RowDescription → DataRow(s) → CommandComplete
7. Client sends Sync
8. Server responds: ReadyForQuery

## Implementation Phases

### Phase 0: Refactoring ✅ COMPLETE

**Goal:** Prepare codebase for extended protocol support

**Completed:**
- Created `ConnectionState` struct to encapsulate connection state
- Extracted message handlers: `handle_client_message()`, `handle_origin_message()`, `handle_cache_reply()`
- Extracted helper functions: `origin_connect()`, `streams_setup()`
- Converted `ProxyMessage` from tuple to struct with named fields
- Improved readability with match statements and better error handling
- Reduced `handle_connection` complexity by ~50%

### Phase 1: Basic Extended Protocol Support (No Caching)

**Goal:** Handle extended protocol messages without caching prepared statements. This phase is broken into three sub-phases to enable incremental progress and testing.

---

#### Phase 1A: Message Parsing (Read-only)

**Goal:** Parse extended protocol messages and extract their data without storing state or changing behavior.

**Tasks:**

1. **Create new file `src/pg/protocol/extended.rs`:**
   - Add module declaration in `src/pg/protocol/mod.rs`

2. **Define data structures in `extended.rs`:**
   ```rust
   pub struct PreparedStatement {
       pub name: String,
       pub sql: String,
       pub parameter_count: usize,
   }

   pub struct Portal {
       pub name: String,
       pub statement_name: String,
       pub parameter_values: Vec<Option<Vec<u8>>>,
       pub parameter_formats: Vec<i16>,  // 0=text, 1=binary
       pub result_formats: Vec<i16>,
   }

   pub struct ParsedParseMessage {
       pub statement_name: String,
       pub sql: String,
       pub parameter_oids: Vec<u32>,
   }

   pub struct ParsedBindMessage {
       pub portal_name: String,
       pub statement_name: String,
       pub parameter_formats: Vec<i16>,
       pub parameter_values: Vec<Option<Vec<u8>>>,
       pub result_formats: Vec<i16>,
   }

   pub struct ParsedExecuteMessage {
       pub portal_name: String,
       pub max_rows: i32,
   }

   pub struct ParsedDescribeMessage {
       pub describe_type: u8,  // b'S' for statement, b'P' for portal
       pub name: String,
   }

   pub struct ParsedCloseMessage {
       pub close_type: u8,  // b'S' for statement, b'P' for portal
       pub name: String,
   }
   ```

3. **Implement message parsers in `extended.rs`:**
   - `parse_parse_message(data: &BytesMut) -> Result<ParsedParseMessage, ProtocolError>`
   - `parse_bind_message(data: &BytesMut) -> Result<ParsedBindMessage, ProtocolError>`
   - `parse_execute_message(data: &BytesMut) -> Result<ParsedExecuteMessage, ProtocolError>`
   - `parse_describe_message(data: &BytesMut) -> Result<ParsedDescribeMessage, ProtocolError>`
   - `parse_close_message(data: &BytesMut) -> Result<ParsedCloseMessage, ProtocolError>`

   **Note:** Forward original raw bytes to origin, not re-encoded data. Parsing is for state tracking only.

4. **Add unit tests for all parsers:**
   - Test parsing valid messages
   - Test parsing edge cases (empty names, zero parameters, NULL values)
   - Test parsing error cases (truncated messages, invalid format)

**Success Criteria:**
- ✅ All parser functions compile and pass unit tests
- ✅ Can extract statement names, SQL, parameter data from messages
- ✅ No changes to connection handling behavior yet
- ✅ All existing tests still pass

---

#### Phase 1B: State Management (No behavior changes)

**Goal:** Track prepared statements and portals in memory without changing message forwarding behavior.

**Tasks:**

1. **Add fields to `ConnectionState` in `proxy.rs`:**
   ```rust
   use crate::pg::protocol::extended::{PreparedStatement, Portal};
   use std::collections::HashMap;

   struct ConnectionState {
       // Existing fields...
       fingerprint_cache: HashMap<u64, Option<Box<CacheableQuery>>>,
       in_transaction: bool,
       proxy_mode: ProxyMode,
       proxy_status: ProxyStatus,
       client_fd_dup: OwnedFd,

       // Extended protocol state
       prepared_statements: HashMap<String, PreparedStatement>,
       portals: HashMap<String, Portal>,
   }
   ```

2. **Update `ConnectionState::new()` to initialize new fields:**
   ```rust
   fn new(client_fd_dup: OwnedFd) -> Self {
       Self {
           // ... existing initialization ...
           prepared_statements: HashMap::new(),
           portals: HashMap::new(),
       }
   }
   ```

3. **Implement state tracking methods (no forwarding changes yet):**
   ```rust
   impl ConnectionState {
       fn statement_store(&mut self, parsed: ParsedParseMessage) {
           let stmt = PreparedStatement {
               name: parsed.statement_name.clone(),
               sql: parsed.sql,
               parameter_count: parsed.parameter_oids.len(),
           };
           self.prepared_statements.insert(parsed.statement_name, stmt);
       }

       fn portal_store(&mut self, parsed: ParsedBindMessage) {
           let portal = Portal {
               name: parsed.portal_name.clone(),
               statement_name: parsed.statement_name,
               parameter_values: parsed.parameter_values,
               parameter_formats: parsed.parameter_formats,
               result_formats: parsed.result_formats,
           };
           self.portals.insert(parsed.portal_name, portal);
       }

       fn statement_close(&mut self, name: &str) {
           self.prepared_statements.remove(name);
       }

       fn portal_close(&mut self, name: &str) {
           self.portals.remove(name);
       }

       fn statements_clear(&mut self) {
           self.prepared_statements.clear();
       }

       fn portals_clear(&mut self) {
           self.portals.clear();
       }
   }
   ```

4. **Add state cleanup on transaction boundaries:**
   - Update `handle_origin_message()` to clear unnamed portals on ReadyForQuery with status 'I' (idle)
   - Unnamed statements/portals have empty string names (`""`)

**Success Criteria:**
- ✅ State tracking methods compile and work correctly
- ✅ HashMap operations succeed without panics
- ✅ All existing tests still pass
- ✅ No changes to message forwarding behavior yet

---

#### Phase 1C: Message Dispatch and Integration

**Goal:** Wire up extended protocol message handling end-to-end with proper forwarding and cleanup.

**Tasks:**

1. **Implement message handlers in `ConnectionState`:**
   ```rust
   impl ConnectionState {
       fn handle_parse_message(&mut self, msg: PgFrontendMessage) {
           // Parse the message
           if let Ok(parsed) = parse_parse_message(&msg.data) {
               // Store in state
               self.statement_store(parsed);
           }
           // Forward original bytes to origin unchanged
           self.proxy_mode = ProxyMode::OriginWrite(msg);
       }

       fn handle_bind_message(&mut self, msg: PgFrontendMessage) {
           if let Ok(parsed) = parse_bind_message(&msg.data) {
               self.portal_store(parsed);
           }
           self.proxy_mode = ProxyMode::OriginWrite(msg);
       }

       fn handle_execute_message(&mut self, msg: PgFrontendMessage) {
           // No state changes, just forward
           self.proxy_mode = ProxyMode::OriginWrite(msg);
       }

       fn handle_describe_message(&mut self, msg: PgFrontendMessage) {
           // No state changes, just forward
           self.proxy_mode = ProxyMode::OriginWrite(msg);
       }

       fn handle_close_message(&mut self, msg: PgFrontendMessage) {
           if let Ok(parsed) = parse_close_message(&msg.data) {
               match parsed.close_type {
                   b'S' => self.statement_close(&parsed.name),
                   b'P' => self.portal_close(&parsed.name),
                   _ => {}
               }
           }
           self.proxy_mode = ProxyMode::OriginWrite(msg);
       }

       fn handle_sync_message(&mut self, msg: PgFrontendMessage) {
           // Sync marks end of extended protocol message group
           self.proxy_mode = ProxyMode::OriginWrite(msg);
       }
   }
   ```

2. **Update `handle_client_message()` to dispatch extended protocol messages:**
   ```rust
   async fn handle_client_message(&mut self, msg: PgFrontendMessage) -> Result<(), ParseError> {
       match msg.message_type {
           PgFrontendMessageType::Query => {
               // ... existing Query handling ...
           }
           PgFrontendMessageType::Parse => {
               self.handle_parse_message(msg);
               Ok(())
           }
           PgFrontendMessageType::Bind => {
               self.handle_bind_message(msg);
               Ok(())
           }
           PgFrontendMessageType::Execute => {
               self.handle_execute_message(msg);
               Ok(())
           }
           PgFrontendMessageType::Describe => {
               self.handle_describe_message(msg);
               Ok(())
           }
           PgFrontendMessageType::Close => {
               self.handle_close_message(msg);
               Ok(())
           }
           PgFrontendMessageType::Sync => {
               self.handle_sync_message(msg);
               Ok(())
           }
           PgFrontendMessageType::Flush => {
               // Flush just requests the server to send any pending data
               self.proxy_mode = ProxyMode::OriginWrite(msg);
               Ok(())
           }
           _ => {
               self.proxy_mode = ProxyMode::OriginWrite(msg);
               Ok(())
           }
       }
   }
   ```

3. **Update `handle_origin_message()` for transaction cleanup:**
   ```rust
   fn handle_origin_message(&mut self, msg: PgBackendMessage) {
       if msg.message_type == PgBackendMessageType::ReadyForQuery {
           if msg.data.len() >= 6 {
               let status = msg.data[5];
               self.in_transaction = status == b'T' || status == b'E';

               // Clean up unnamed portals when transaction ends (status 'I' = idle)
               if status == b'I' {
                   self.portals.retain(|name, _| !name.is_empty());
               }
           }
       }
       self.proxy_mode = ProxyMode::ClientWrite(msg);
   }
   ```

4. **Integration testing:**
   - Write Rust integration test using `tokio-postgres` client (uses extended protocol by default)
   - Test basic prepared statement: Parse → Bind → Execute → Sync
   - Test statement reuse: Parse once, Bind+Execute multiple times
   - Test Close statement and Close portal
   - Test unnamed statements (empty name)
   - Verify all messages forward correctly to origin
   - Verify responses come back to client unchanged

5. **Manual testing:**
   - Test with `psql` (uses simple protocol by default)
   - Test with Rust `tokio-postgres` client examples
   - Verify no memory leaks (statements/portals cleaned up properly)

**Success Criteria:**
- ✅ Extended protocol clients (tokio-postgres) can connect and execute queries
- ✅ All messages forward to origin with original bytes unchanged
- ✅ State is tracked correctly (statements and portals stored/removed)
- ✅ Transaction boundaries properly clear unnamed portals
- ✅ Close messages properly clean up state
- ✅ All existing tests still pass
- ✅ New integration tests pass

---

**Phase 1 Overall Success Criteria:**
- Extended protocol clients can connect and execute queries
- All queries forward to origin (no caching yet)
- Proper state management (no memory leaks)
- All existing tests still pass
- Ready for Phase 2 (cache integration)

### Phase 2: Cache Integration for Prepared Statements

**Goal:** Cache queries executed via extended protocol

**Tasks:**
1. **Analyze cacheability during Parse:**
   - Parse SQL in `handle_parse_message()`
   - Determine if query is cacheable
   - Store cacheability result in `PreparedStatement`

2. **Generate cache keys in Bind:**
   - Create fingerprint from SQL template + parameter values
   - Support parameterized cache keys

3. **Cache decision in Execute:**
   - Check if associated statement is cacheable
   - Generate cache key from portal parameters
   - Check cache or forward to origin
   - Serve from cache when possible

4. **Parameter handling:**
   - Parse parameter values from Bind message
   - Support text format (format code 0)
   - Initially reject binary format, add later
   - Convert parameters for fingerprinting

5. **Update cache/worker to handle parameterized queries:**
   - Extend `CacheMessage` to include parameters
   - Update query execution to use parameters

6. **Testing:**
   - Test prepared statement caching
   - Test parameter substitution
   - Test cache hits with same SQL, different parameters
   - Test cache invalidation with CDC

**Success Criteria:**
- Prepared statements are cached correctly
- Parameter values are handled properly
- Cache hits/misses work as expected
- CDC invalidation works with parameterized queries

### Phase 3: Optimization and Advanced Features

**Goal:** Optimize and add advanced extended protocol features

**Tasks:**
1. **Statement reuse optimization:**
   - Reuse parsed AST across multiple Bind operations
   - Cache cacheability analysis per statement

2. **Binary format support:**
   - Add parameter parsing for common binary types (int4, int8, text, bytea)
   - Add result encoding for binary format
   - Gradually expand type support

3. **Portal suspension:**
   - Support row limits in Execute message
   - Handle partial result sets
   - Support multiple Execute calls per portal

4. **Pipelining optimization:**
   - Process multiple messages before responding
   - Batch cache lookups
   - Optimize network round-trips

5. **Server-side cursors:**
   - Support DECLARE CURSOR via extended protocol
   - Cache cursor results

**Success Criteria:**
- Binary format works for common types
- Pipelining improves performance
- Portal suspension works correctly

## Implementation Details

### Data Structures

**PreparedStatement:**
- Stores parsed SQL and metadata
- Caches cacheability decision
- Tracks parameter count and types

**Portal:**
- Links to a PreparedStatement
- Stores bound parameter values
- Stores format preferences

### Message Parsing

**Parse Message Format:**
```
Byte1('P')
Int32 - message length
String - statement name (empty string for unnamed)
String - SQL query
Int16 - number of parameter data types
For each parameter:
    Int32 - OID of parameter data type (0 = unspecified)
```

**Bind Message Format:**
```
Byte1('B')
Int32 - message length
String - portal name (empty string for unnamed)
String - statement name
Int16 - number of parameter format codes
For each format code:
    Int16 - format code (0=text, 1=binary)
Int16 - number of parameter values
For each parameter:
    Int32 - parameter length (-1 = NULL)
    Byte[n] - parameter value
Int16 - number of result column format codes
For each format code:
    Int16 - format code (0=text, 1=binary)
```

**Execute Message Format:**
```
Byte1('E')
Int32 - message length
String - portal name (empty string for unnamed)
Int32 - maximum number of rows to return (0 = unlimited)
```

### Cache Key Generation

For extended protocol queries, the cache key must include:
1. SQL template (from Parse)
2. Parameter values (from Bind)

**Approach:**
- Hash(SQL template) + Hash(sorted parameters) → cache key
- Store mapping: portal → cache key
- Use same cache lookup mechanism as simple protocol

### Transaction Handling

- Track transaction state through `Sync` messages
- Don't cache within explicit transactions (existing behavior)
- Clean up portals on transaction end

### Error Handling

**New error cases:**
- Unknown statement name in Bind
- Unknown portal name in Execute/Close
- Parameter count mismatch
- Unsupported format codes

**Error responses:**
- ErrorResponse with appropriate SQLSTATE
- Follow PostgreSQL error message format

## Testing Strategy

### Unit Tests
- Parse message parsing
- Bind message parsing
- Parameter extraction
- Cache key generation
- State management (add/remove statements/portals)

### Integration Tests
- End-to-end extended protocol flow
- Cache hits/misses with parameters
- Error cases (unknown statement, etc.)
- Transaction boundaries
- CDC invalidation

### Client Compatibility Tests
- tokio-postgres (Rust) - primary testing client
- pgx (Rust) - alternative Rust client
- psql (command-line tool)
- Any other PostgreSQL clients as needed

### Performance Tests
- Compare simple vs extended protocol performance
- Measure prepared statement reuse benefits
- Benchmark parameter substitution overhead

## Risks and Challenges

### Challenge 1: Parameter Type Handling
**Risk:** Binary format requires type-specific encoding/decoding
**Mitigation:** Start with text format only, add binary types incrementally

### Challenge 2: Stateful Protocol Complexity
**Risk:** Extended protocol is stateful, complex state management
**Mitigation:** Clear state structures, comprehensive testing, proper cleanup

### Challenge 3: Fingerprint Cache Invalidation
**Risk:** Current fingerprint cache uses SQL text hash, won't work with parameters
**Mitigation:** Update cache key generation to include parameters

### Challenge 4: Transaction Boundaries
**Risk:** Portal/statement lifecycle across transactions
**Mitigation:** Track transaction state, clean up on Sync

### Challenge 5: Pipelining
**Risk:** Client may send multiple messages before waiting for responses
**Mitigation:** Queue messages, process in order, buffer responses

## Success Metrics

- [ ] Extended protocol clients can connect and execute queries
- [ ] Prepared statements are cached correctly
- [ ] Parameter substitution works for text format
- [ ] All existing tests pass
- [ ] New extended protocol tests pass
- [ ] Performance is comparable or better than simple protocol
- [ ] No memory leaks (statements/portals cleaned up properly)
- [ ] CDC invalidation works with parameterized queries

## References

- [PostgreSQL Protocol Flow](https://www.postgresql.org/docs/current/protocol-flow.html)
- [PostgreSQL Message Formats](https://www.postgresql.org/docs/current/protocol-message-formats.html)
- [libpq Extended Query](https://www.postgresql.org/docs/current/libpq-async.html)

## Next Steps

1. Complete Phase 0 refactoring (✅ DONE)
2. Begin Phase 1: Message parsing and basic state management
3. Test with psycopg3 to verify extended protocol handling
4. Implement Phase 2: Cache integration
5. Optimize in Phase 3
