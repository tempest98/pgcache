# ADR-010: Population Worker Pool

## Status
Accepted

## Context

Cache population tasks fetch query results from the origin database and insert them into the cache database. Previously, each population was handled by dynamically spawning a task with `spawn_local()` when a query needed to be cached. A connection pool (channel-based) provided database connections to these tasks.

Under load, we observed significant latency variance in population tasks - some completing in ~500ms while others took ~6000ms. The hypothesis was that dynamically spawned tasks were being starved by tokio's cooperative scheduler, as the main writer loop processing commands could monopolize CPU time during busy periods.

Alternatives considered:
1. **Add explicit yield points** - Insert `yield_now()` calls in the main loop. Addresses symptoms but doesn't fix the fundamental scheduling unpredictability.
2. **Separate thread for population** - Move population to its own thread/runtime. Clean separation but adds complexity and cross-thread coordination overhead.
3. **Persistent worker pool** - Fixed number of long-lived workers reading from queues. Predictable scheduling with orderly processing.

## Decision

Replace dynamic task spawning with a pool of 3 persistent population workers:

- Each worker is a long-lived task spawned at startup with its own cache database connection
- Workers receive work via per-worker unbounded channels
- Work is dispatched round-robin across workers
- Workers process items sequentially, ensuring orderly completion

## Rationale

- **Predictable scheduling**: Fixed workers with dedicated channels provide consistent, orderly processing rather than competing with ad-hoc spawned tasks
- **Simpler resource management**: Each worker owns its connection permanently rather than borrowing from a pool
- **Maintains parallelism**: Three workers can still process populations concurrently, matching the previous pool size
- **Consistent with codebase patterns**: Uses channels for work distribution, matching other inter-component communication in pgcache

## Consequences

### Positive
- Eliminates task starvation from cooperative scheduling contention
- More predictable population latency under load
- Simpler mental model - workers are always running, just waiting for work
- Easier to reason about resource usage (fixed 3 connections, 3 tasks)

### Negative
- Round-robin may not be optimal if work items have varying costs (could queue behind slow items)
- Less dynamic - always 3 workers regardless of actual load
- Workers consume resources even when idle (though minimal for waiting tasks)

## Implementation Notes

Workers are spawned inside `LocalSet::run_until()` to satisfy `spawn_local` requirements. The `PopulationWork` struct carries all data needed for population, avoiding shared state between the dispatch site and workers.
