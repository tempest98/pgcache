# Phase 0: Refactoring for Extended Query Protocol Support

## Goal

Refactor the existing `handle_connection` function in `proxy.rs` to prepare for extended query protocol support. The refactoring will create clearer separation of concerns and make future protocol additions easier.

## Current State Analysis

The `handle_connection` function (lines 267-409) is approximately 143 lines and handles:

1. **Connection setup** (lines 272-310)
   - Connect to origin database
   - Configure TCP settings (nodelay)
   - Split client and origin sockets into read/write halves
   - Create framed readers with protocol codecs
   - Set up StreamMap for multiplexing reads

2. **State initialization** (lines 312-314)
   - `fingerprint_cache`: Maps query text hash → cacheability decision
   - `in_transaction`: Tracks transaction state
   - `proxy_mode` and `proxy_status`: Control flow state

3. **Main message loop** (lines 316-403)
   - Handle three message sources: client, origin, cache replies
   - Route messages based on type and transaction state
   - Perform writes to client, origin, or cache
   - Manage cache degradation

4. **Cleanup** (lines 405-408)
   - Return error if cache died, otherwise success

## Problems with Current Structure

1. **Large function**: Hard to understand at a glance
2. **Mixed concerns**: Connection setup, state management, and message routing are interleaved
3. **Hard to extend**: Adding extended protocol will make the function even larger
4. **Hard to test**: Cannot test individual message handlers in isolation
5. **Hidden state**: Local variables make it unclear what state is being tracked

## Refactoring Plan

### Step 1: Create ConnectionState Structure

Extract all connection-level state into a struct:

```rust
struct ConnectionState {
    // Existing state for simple query protocol
    fingerprint_cache: HashMap<u64, Option<Box<CacheableQuery>>>,
    in_transaction: bool,
    proxy_mode: ProxyMode,
    proxy_status: ProxyStatus,
    client_fd_dup: OwnedFd,

    // Future: Extended protocol state (Phase 1+)
    // prepared_statements: HashMap<String, PreparedStatement>,
    // portals: HashMap<String, Portal>,
}
```

**Benefits:**
- Clear documentation of what state exists
- Easy to add extended protocol state later
- Can be tested independently

### Step 2: Extract Message Handlers

Create methods on `ConnectionState` for handling different message types:

```rust
impl ConnectionState {
    // Handle messages from client
    async fn handle_client_message(&mut self, msg: PgFrontendMessage)
        -> Result<(), ParseError>

    // Handle messages from origin database
    fn handle_origin_message(&mut self, msg: PgBackendMessage)

    // Handle cache replies
    fn handle_cache_reply(&mut self, reply: CacheReply)
}
```

**Benefits:**
- Each handler focuses on one message source
- Clear function signatures show inputs/outputs
- Easy to add new message types (Parse, Bind, Execute, etc.)
- Can unit test message handling logic

### Step 3: Extract Write Operations

Create a method to process the current proxy mode (writing):

```rust
impl ConnectionState {
    async fn process_mode(
        &mut self,
        cache_tx: &SenderCacheType,
        streams_read: &mut StreamMap<...>,
        origin_write: &mut WriteHalf<'_>,
        client_write: &mut WriteHalf<'_>,
    ) -> Result<bool, ConnectionError>
}
```

**Benefits:**
- Separates reading logic from writing logic
- Makes the main loop simpler
- Returns bool indicating if mode completed

### Step 4: Simplify handle_connection

Rewrite the main function to use the new structure:

```rust
async fn handle_connection(...) -> Result<(), ConnectionError> {
    // 1. Connection setup (stays mostly the same)
    let origin_stream = connect_to_origin(addrs).await?;
    configure_sockets(...);

    // 2. Initialize state
    let mut state = ConnectionState::new(client_fd_dup);

    // 3. Set up streams
    let mut streams_read = setup_streams(...);

    // 4. Simplified main loop
    loop {
        match state.proxy_mode {
            ProxyMode::Read => {
                let Some((_, res)) = streams_read.next().await else { break };

                match res? {
                    StreamSource::ClientRead(msg) =>
                        state.handle_client_message(msg).await?,
                    StreamSource::OriginRead(msg) =>
                        state.handle_origin_message(msg),
                    StreamSource::CacheRead(reply) =>
                        state.handle_cache_reply(reply),
                }
            }
            _ => {
                state.process_mode(&cache_tx, &mut streams_read,
                                   &mut origin_write, &mut client_write).await?;
            }
        }
    }

    // 5. Return status
    state.final_status()
}
```

**Benefits:**
- Main function is ~50% smaller
- Clear separation: setup → loop → cleanup
- Loop body is easier to understand
- Easy to see control flow

## Implementation Steps

1. **Step 1**: Create `ConnectionState` struct with constructor
   - Move fields from local variables into struct
   - Create `ConnectionState::new()` method
   - No behavior changes yet

2. **Step 2**: Extract `handle_client_message` method
   - Extract lines 322-338 into method
   - Update to use `self` instead of local variables
   - Test that behavior is unchanged

3. **Step 3**: Extract `handle_origin_message` method
   - Extract lines 340-344 into method
   - Update to use `self` instead of local variables

4. **Step 4**: Extract `handle_cache_reply` method
   - Extract lines 346-354 into method
   - Update to use `self` instead of local variables

5. **Step 5**: Extract `process_mode` method
   - Extract lines 365-401 into method
   - Handle ownership issues (ProxyMode variants)
   - Return bool indicating completion

6. **Step 6**: Refactor main loop
   - Update loop to use new methods
   - Simplify control flow
   - Verify behavior unchanged

7. **Step 7**: Add final cleanup method
   - Extract status check (lines 405-408)
   - Add `final_status()` method

## Testing Strategy

After each step:
1. Run `cargo check` to ensure it compiles
2. Run `cargo test` to ensure tests pass
3. Run `cargo clippy` to check for issues
4. Manually test with a PostgreSQL client if needed

## Future Benefits

Once Phase 0 is complete, adding extended query protocol support becomes:

1. Add new fields to `ConnectionState`:
   - `prepared_statements: HashMap<String, PreparedStatement>`
   - `portals: HashMap<String, Portal>`

2. Add new message handlers:
   - `handle_parse_message()`
   - `handle_bind_message()`
   - `handle_execute_message()`
   - etc.

3. Update `handle_client_message()` to dispatch to new handlers

4. Main `handle_connection` loop remains largely unchanged

## Success Criteria

- [ ] `ConnectionState` struct created with all connection state
- [ ] All message handling extracted into methods
- [ ] `handle_connection` function reduced by ~50% in size
- [ ] All tests pass
- [ ] No clippy warnings
- [ ] Behavior is unchanged (can connect and run queries)
- [ ] Code is easier to understand and modify
