# Plan: Atomic Pipeline for Extended Query Cache Hits

## Context

The previous approach (speculative synthesis of ParseComplete/BindComplete at Parse/Bind time) has a race condition: during CacheRead mode, two independent writers compete on the same TCP socket:

1. The proxy's `client_write_buf` flushes origin responses (ParseComplete, BindComplete, RowDescription from cold-path Parse/Bind/Describe)
2. The cache worker writes DataRow/CommandComplete directly to a `dup()`'d file descriptor

These writes interleave non-deterministically. The client can receive DataRow before RowDescription, or BindComplete after DataRow — causing protocol errors like `message type 0x32 arrived from server while idle`.

**Fix**: Buffer the entire Parse/Bind/Describe/Execute pipeline locally in the proxy. On Execute, dispatch everything to the cache as a single atomic message. The cache worker writes the **full** protocol response (ParseComplete + BindComplete + [RowDescription] + DataRow* + CommandComplete). On cache miss, the cache returns Forward and the proxy dumps all buffered messages to origin.

This eliminates the dual-writer problem: at any moment, exactly one entity writes to the client.

---

## Files to Modify

| File | Changes |
|---|---|
| `src/pg/protocol/extended.rs` | Simplify PreparedStatement/Portal (remove `parse_was_synthesized`, `bind_was_synthesized`, `parse_data`, `bind_data`) |
| `src/cache/messages.rs` | Add `PipelineContext` to `ProxyMessage`, remove `CacheReply::Data` |
| `src/cache/query_cache.rs` | Thread pipeline context through `QueryRequest` → `WorkerRequest` |
| `src/cache/runtime.rs` | Thread pipeline context through `handle_proxy_message` → `handle_worker_request` |
| `src/cache/worker.rs` | Worker prepends ParseComplete+BindComplete (and optionally RowDescription) to write buffer |
| `src/pg/cache_connection.rs` | Add `include_describe` parameter to `pipelined_binary_query_send()` |
| `src/proxy/connection.rs` | Replace hot-path synthesis with pipeline buffering; remove suppress flags, `cache_fallback_flush`, and origin Flush on CacheWrite |

---

## Step 1: Simplify PreparedStatement and Portal

**`src/pg/protocol/extended.rs`**

Remove fields that were only needed for the speculative synthesis approach:

```rust
// Remove from PreparedStatement:
pub parse_data: BytesMut,
pub parse_was_synthesized: bool,

// Remove from Portal:
pub bind_data: BytesMut,
pub bind_was_synthesized: bool,
```

Add to PreparedStatement:
- `origin_prepared: bool` — set to `true` when origin acknowledges the Parse (ParseComplete received). Gates pipeline activation. Initially `false`.

Keep on PreparedStatement:
- `parameter_description: Option<BytesMut>` — captured from origin's ParameterDescription response. Only used to provide ParameterDescription bytes when Describe('S') is in the pipeline. Not used as a pipeline gate.

Remove from PreparedStatement:
- `row_description: Option<BytesMut>` — no longer needed (Describe responses are handled in the pipeline or by origin)

Update `statement_store()` and `portal_store()` signatures accordingly. Update all test constructors of `Portal` (4 tests in extended.rs).

---

## Step 2: Update cache messages

**`src/cache/messages.rs`**

### Remove `CacheReply::Data`

This variant is dead code — never constructed, only pattern-matched. Remove it.

### Add PipelineContext and PipelineDescribe

```rust
/// Whether the pipeline includes a Describe and which type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineDescribe {
    /// No Describe in the pipeline
    None,
    /// Describe('S') — worker should include ParameterDescription + RowDescription
    Statement,
    /// Describe('P') — worker should include RowDescription only
    Portal,
}

/// Pipeline context for atomic extended query dispatch.
/// Contains the raw Parse/Bind/Describe bytes buffered by the proxy,
/// used for origin fallback on cache miss.
pub struct PipelineContext {
    /// All buffered messages (Parse + Bind + optional Describe) concatenated.
    /// Forwarded to origin on cache miss (Forward reply).
    pub buffered_bytes: BytesMut,
    /// Whether the pipeline includes a Describe message.
    pub describe: PipelineDescribe,
}
```

### Add pipeline to ProxyMessage

```rust
pub struct ProxyMessage {
    pub message: CacheMessage,
    pub client_socket: ClientSocket,
    pub reply_tx: oneshot::Sender<CacheReply>,
    pub search_path: Vec<String>,
    pub timing: QueryTiming,
    pub pipeline: Option<PipelineContext>,  // None for simple queries and cold-path extended
}
```

### Update CacheReply::Forward

Forward now carries pipeline bytes prepended to execute data:

```rust
CacheReply::Forward(BytesMut),  // pipeline_bytes + execute_data (or just execute_data if no pipeline)
```

`CacheReply::Complete` simplifies to: `Complete(Option<QueryTiming>)`. The `Option<BytesMut>` that carried RowDescription for hot-path Describe replay is no longer needed — Describe responses are handled naturally in the pipeline or by origin.

---

## Step 3: Pipeline buffering in proxy ConnectionState

**`src/proxy/connection.rs`**

### Remove fields
```rust
// Remove:
suppress_origin_parse_complete: bool,
suppress_origin_bind_complete: bool,
pending_execute_statement_name: Option<String>,
pending_execute_portal_name: Option<String>,
```

### Keep
```rust
cache_hit_pending_sync: bool,      // set on Complete, consumed by Sync
pending_describe_statement: Option<String>,  // for ParameterDescription capture from origin
```

### Add tracking for origin_prepared
```rust
/// Statement name whose ParseComplete we're waiting for from origin.
/// Set when Parse is forwarded on cold path; consumed in handle_origin_message
/// to set origin_prepared = true on the PreparedStatement.
pending_parse_statement: Option<String>,
```

### Add pipeline buffer
```rust
/// Buffered Parse/Bind/Describe messages for the current extended query pipeline.
/// Accumulated as messages arrive; dispatched atomically on Execute.
/// None when no cacheable pipeline is in progress.
pipeline_buffer: Option<PipelineBuffer>,
```

```rust
struct PipelineBuffer {
    /// Concatenated raw bytes of Parse + Bind + optional Describe messages.
    bytes: BytesMut,
    /// Whether a Describe was buffered and which type.
    describe: PipelineDescribe,
}
```

---

## Step 4: Rewrite handle_parse_message

**`src/proxy/connection.rs`**

Remove all hot-path synthesis logic. Instead:

```
fn handle_parse_message(msg):
    parse the message
    store PreparedStatement (no parse_data or parse_was_synthesized)

    // Flush any existing pipeline before potentially starting a new one
    pipeline_flush_to_origin()

    if cacheable && origin_prepared && proxy_status == Normal:
        // Start a new pipeline buffer
        pipeline_buffer = Some(PipelineBuffer {
            bytes: msg.data.clone(),
            describe: PipelineDescribe::None,
        })
        return  // do NOT forward to origin
    else:
        // Cold path — track statement name for origin_prepared on ParseComplete
        pending_parse_statement = Some(parsed.statement_name)
        forward msg.data to origin
```

---

## Step 5: Rewrite handle_bind_message

```
fn handle_bind_message(msg):
    parse the message
    store Portal (no bind_data or bind_was_synthesized)

    if pipeline_buffer.is_some():
        // Append Bind bytes to pipeline buffer
        pipeline_buffer.bytes.extend_from_slice(&msg.data)
        return  // do NOT forward to origin
    else:
        // Cold path
        forward msg.data to origin
```

---

## Step 6: Rewrite handle_describe_message

```
fn handle_describe_message(msg):
    if Describe('S'):
        // If pipeline active AND we have stored ParameterDescription bytes:
        // buffer the Describe for the cache worker to handle
        if pipeline_buffer.is_some() && parameter_description.is_some():
            pipeline_buffer.bytes.extend_from_slice(&msg.data)
            pipeline_buffer.describe = PipelineDescribe::Statement
            return

        // Otherwise fall back to origin — either no pipeline, or pipeline active
        // but we don't have ParameterDescription bytes to include in the response.
        // Flushing the pipeline lets origin handle Parse+Bind+Describe+Execute naturally.
        pipeline_flush_to_origin()
        pending_describe_statement = Some(name)
        forward to origin

    if Describe('P'):
        if pipeline_buffer.is_some():
            pipeline_buffer.bytes.extend_from_slice(&msg.data)
            pipeline_buffer.describe = PipelineDescribe::Portal
            return
        forward to origin
```

---

## Step 7: Rewrite handle_execute_message

```
fn handle_execute_message(msg):
    record metrics, set query_start

    match try_cache_execute(msg):
        Some(cache_msg):
            // Build pipeline context (None for cold-path execute or simple query)
            self.pipeline_context = self.pipeline_buffer.take().map(|pb| PipelineContext {
                buffered_bytes: pb.bytes,
                describe: pb.describe,
            });
            self.proxy_mode = ProxyMode::CacheWrite(cache_msg);

        None:
            // Not cacheable — flush pipeline to origin if active, then forward Execute
            pipeline_flush_to_origin()
            forward msg.data to origin
            proxy_mode = Read
```

`pipeline_context: Option<PipelineContext>` is a temporary field on `ConnectionState`. The CacheWrite arm of the main loop moves it into `ProxyMessage` (Step 8).

Also remove the origin Flush (`H`) that the current code sends before CacheWrite dispatch. In the pipeline model, the hot path never sends anything to origin, so there is nothing to drain. On the cold path (no pipeline), Execute is forwarded directly.

---

## Step 8: Update CacheWrite arm in main loop

**`src/proxy/connection.rs`** — the `ProxyMode::CacheWrite(msg)` match arm

```rust
let proxy_msg = ProxyMessage {
    message: msg,
    client_socket,
    reply_tx,
    search_path: resolved_search_path,
    timing,
    pipeline: state.pipeline_context.take(),
};
```

On `cache_sender.send()` error (cache unavailable fallback):

```rust
Err(e) => {
    let proxy_msg = e.into_message();
    let execute_data = proxy_msg.message.into_data();
    // Flush pipeline bytes + execute to origin
    if let Some(pipeline) = proxy_msg.pipeline {
        state.origin_write_buf.push_back(pipeline.buffered_bytes);
    }
    state.origin_write_buf.push_back(execute_data);
    state.proxy_mode = ProxyMode::Read;
}
```

Remove the call to `cache_fallback_flush`. Remove the `cache_fallback_flush` method entirely.

---

## Step 9: Update handle_cache_reply

**`src/proxy/connection.rs`**

```
CacheReply::Complete(timing):
    cache_hit_pending_sync = true
    proxy_mode = Read

CacheReply::Forward(buf) | CacheReply::Error(buf):
    // buf already contains pipeline_bytes + execute_data (assembled by cache coordinator)
    origin_write_buf.push_back(buf)
    proxy_mode = Read
```

---

## Step 10: Update cache coordinator (Forward path)

**`src/cache/query_cache.rs`**

When the coordinator decides to forward (cache miss or unregistered query), prepend pipeline bytes to the execute data:

```rust
// In query_dispatch(), when forwarding:
let forward_buf = if let Some(pipeline) = request.pipeline {
    let mut buf = pipeline.buffered_bytes;
    buf.extend_from_slice(&request.data);
    buf
} else {
    request.data
};
let _ = request.reply_tx.send(CacheReply::Forward(forward_buf));
```

This requires threading `pipeline: Option<PipelineContext>` through `QueryRequest`.

For worker error fallback: the worker needs pipeline bytes to assemble the error reply. Add `pipeline_bytes: Option<BytesMut>` to `WorkerRequest`:

```rust
pub pipeline_bytes: Option<BytesMut>,  // for error fallback to origin
```

In `handle_worker_request`, on error:
```rust
Err(e) => {
    let mut error_buf = msg.pipeline_bytes.take().unwrap_or_default();
    error_buf.extend_from_slice(&msg.data);
    CacheReply::Error(error_buf)
}
```

---

## Step 11: Worker writes full protocol response

**`src/cache/worker.rs`**

Add `has_pipeline: bool` to `WorkerRequest`, set from `pipeline.is_some()` in the coordinator.

### Text path (`handle_cached_query_text`)

Currently writes: [RowDescription if Simple] + DataRow* + CommandComplete + [ReadyForQuery if Simple]

Change: when `has_pipeline` is true, prepend ParseComplete + BindComplete. When `pipeline_describe` indicates a Describe was buffered, include the Describe response.

```rust
// At the start, before streaming:
if msg.has_pipeline {
    parse_complete_encode(&mut write_buf);
    bind_complete_encode(&mut write_buf);
}
```

RowDescription handling in the text response state machine:

```rust
(TextResponseState::RowDescription, PgBackendMessageType::RowDescription) => {
    if query_type == QueryType::Simple {
        write_buf.extend_from_slice(&frame.data);
    } else if pipeline_describe != PipelineDescribe::None {
        // Describe was in pipeline — include Describe response
        if pipeline_describe == PipelineDescribe::Statement {
            if let Some(param_desc) = &parameter_description {
                write_buf.extend_from_slice(param_desc);
            }
        }
        write_buf.extend_from_slice(&frame.data);
    }
    state = TextResponseState::DataRows;
}
```

Remove `local_row_description` and `captured_row_description` from the text path — RowDescription is no longer captured for later replay.

Add fields to `WorkerRequest`:
```rust
pub has_pipeline: bool,
pub pipeline_describe: PipelineDescribe,
pub parameter_description: Option<BytesMut>,  // for Describe('S') response
```

### Binary path (`handle_cached_query_binary`)

Currently writes: DataRow* + CommandComplete.

Change: when `has_pipeline` is true, prepend ParseComplete + BindComplete. When `pipeline_describe` is not None, include RowDescription from the cache DB.

```rust
// At the start, before streaming:
if msg.has_pipeline {
    parse_complete_encode(&mut write_buf);
    bind_complete_encode(&mut write_buf);
}
```

For Describe in the binary path, the cache DB must return RowDescription. This requires a Describe('P') message in the binary pipeline.

**`src/pg/cache_connection.rs`** — add `include_describe: bool` parameter:

```rust
pub async fn pipelined_binary_query_send(
    &mut self,
    set_sql: &str,
    select_sql: &str,
    include_describe: bool,
) -> CacheResult<()>
```

When `include_describe` is true, insert Describe('P') between Bind and Execute.

### Binary state machine update

```rust
enum BinaryResponseState {
    SetComplete,
    SetReady,
    ParseComplete,
    BindComplete,
    DescribeRow,    // NEW — only entered when include_describe is true
    DataRows,
    Done,
}
```

Transitions:
- `include_describe == false`: BindComplete → DataRows
- `include_describe == true`: BindComplete → DescribeRow → DataRows

On RowDescription in DescribeRow state:
- `PipelineDescribe::Statement`: write stored ParameterDescription + captured RowDescription to write_buf
- `PipelineDescribe::Portal`: write captured RowDescription to write_buf

---

## Step 12: Update handle_origin_message

**`src/proxy/connection.rs`**

Remove the ParseComplete/BindComplete suppression blocks:
```rust
// REMOVE these blocks:
if self.suppress_origin_parse_complete { ... }
if self.suppress_origin_bind_complete { ... }
```

Origin never sends ParseComplete/BindComplete for a pipelined query because Parse/Bind were never forwarded.

Add `origin_prepared` tracking on ParseComplete:
```
ParseComplete received from origin:
    if let Some(name) = self.pending_parse_statement.take():
        if let Some(stmt) = self.prepared_statements.get_mut(&name):
            stmt.origin_prepared = true
    forward to client
```

This enables the pipeline for subsequent Parse calls for this statement. Clients that never send Describe('S') now get pipeline activation after the first cold-path Parse is acknowledged — the pipeline is no longer gated on having ParameterDescription bytes.

---

## Step 13: pipeline_flush_to_origin helper

**`src/proxy/connection.rs`**

```rust
/// Flush any active pipeline buffer to origin (e.g., when a non-cacheable
/// message interrupts a pipeline, or a Close/Flush arrives mid-pipeline).
fn pipeline_flush_to_origin(&mut self) {
    if let Some(pipeline) = self.pipeline_buffer.take() {
        self.origin_write_buf.push_back(pipeline.bytes);
    }
}
```

Call sites:
- `handle_parse_message` — always, before potentially starting a new pipeline
- `handle_execute_message` when `try_cache_execute` returns None
- `handle_close_message` (Close arrives mid-pipeline)
- `handle_flush_message` (Flush 'H' arrives mid-pipeline)
- Any other client message handler that can interrupt a pipeline

---

## Step 14: handle_sync_message — unchanged

`cache_hit_pending_sync` is still the mechanism:
- On cache hit, nothing was sent to origin (Parse/Bind/Execute all handled by cache)
- On cache miss (Forward), pipeline bytes + Execute are flushed to origin, and Sync follows naturally

No changes needed to `handle_sync_message`.

---

## Flow Summary

### Hot path (cache hit, pipeline active)

```
Parse   → proxy buffers in pipeline_buffer, stores PreparedStatement
Bind    → proxy appends to pipeline_buffer, stores Portal
Execute → proxy takes pipeline_buffer, creates PipelineContext,
          sends ProxyMessage with pipeline to cache
          Cache worker writes: ParseComplete + BindComplete + DataRow* + CommandComplete
          Cache replies: Complete(timing)
Sync    → proxy sees cache_hit_pending_sync, synthesizes ReadyForQuery('I')
```

Single writer to client: only the cache worker during Execute.

### Cold path (first execution, no pipeline)

```
Parse   → forward to origin (origin_prepared is false)
          origin responds with ParseComplete → origin_prepared set to true
Bind    → forward to origin (no active pipeline)
Execute → try_cache_execute returns Some (cacheable), sends to cache with pipeline=None
          Cache replies: Forward (miss) — execute_data only (no pipeline bytes)
          Proxy pushes execute_data to origin_write_buf
Sync    → forwarded to origin
```

Origin handles the full Parse/Bind/Execute/Sync sequence. Worker never prepends ParseComplete/BindComplete because `has_pipeline` is false. After this execution, `origin_prepared` is true and subsequent Parse calls for this statement activate the pipeline.

### Describe('P') in pipeline

```
Parse      → buffered in pipeline (origin_prepared is true)
Bind       → appended to pipeline
Describe('P') → appended to pipeline, describe = Portal
Execute    → dispatched to cache with PipelineContext { describe }
             Worker includes RowDescription (from cache DB) in response
```

### Describe('S') in pipeline

```
Parse      → buffered in pipeline (origin_prepared is true)
Bind       → appended to pipeline
Describe('S') → if parameter_description available:
                  appended to pipeline, describe = Statement
                  Worker includes ParameterDescription (from stored bytes)
                  + RowDescription (from cache DB) in response
               else:
                  pipeline flushed to origin, Describe forwarded to origin
                  origin handles everything; ParameterDescription captured for next time
```

---

## Edge Cases

1. **Close mid-pipeline**: `handle_close_message` calls `pipeline_flush_to_origin()`, then forwards Close to origin. Origin receives Parse+Bind+Close which is valid.

2. **Flush ('H') mid-pipeline**: Same — flush pipeline to origin, then forward Flush.

3. **Second Parse before Execute**: `handle_parse_message` always calls `pipeline_flush_to_origin()` before starting a new pipeline. If client sends Parse₁ + Parse₂ + Bind₂ + Execute₂, Parse₁ bytes are flushed to origin when Parse₂ arrives.

4. **Transaction state**: Pipeline only starts when `origin_prepared` is true (set after first cold-path ParseComplete). `try_cache_execute` rejects in-transaction queries. Add a defensive `pipeline_buffer` clear in the transaction-begin detection path.

5. **Unnamed statements**: The unnamed statement slot is overwritten on each Parse. Pipeline buffering works correctly because we take the pipeline on Execute, before the next Parse can start a new one.

---

## Verification

1. `cargo check` — type-check all changes
2. `cargo test --lib` — unit tests (query parsing, cacheability)
3. `cargo test --test <integration>` — integration tests covering extended query cache hits
4. Manual pgbench test: `pgbench -c 4 -T 10 -f select.sql` against pgcache with `RUST_LOG=trace` — verify no protocol errors, verify `net:` trace shows cache worker writing full ParseComplete+BindComplete+DataRow+CommandComplete sequence
5. Verify cold path: first execution of a prepared statement goes to origin, second execution hits cache
6. Verify Forward fallback: query with uncached data returns correct results via origin
7. Verify Describe in pipeline: Parse + Bind + Describe('S') + Execute returns ParameterDescription + RowDescription + DataRows correctly from cache
