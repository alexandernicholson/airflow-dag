# ironpipe

A DAG-based task orchestration engine for Rust, built on the [Rebar](https://github.com/alexandernicholson/rebar) actor runtime. Production-grade scheduling semantics — trigger rules, state machines, XCom, retries — as native Rust types with actor-based execution.

> `174 tests, 0 clippy warnings (pedantic)`

## Overview

`ironpipe` provides a production-grade DAG execution engine in Rust:

- A type-safe DAG builder with dependency tracking and cycle detection
- A task state machine with 10 states mirroring production DAG engine semantics
- All 12 trigger rules with correct evaluation semantics
- Automatic upstream failure and skip propagation
- XCom (cross-communication) for passing data between tasks
- Actor-based execution on the Rebar runtime with per-task processes
- Retry logic with configurable attempt counts

Tasks are defined as pure data. Execution logic is provided by implementing the `TaskExecutor` trait. The scheduler runs as a Rebar `GenServer` that evaluates trigger rules, dispatches task processes via `async_task_ctx`, and advances the DAG run to completion. XCom state is managed by a Rebar `Agent`.

## Quick Example

```rust
use ironpipe::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

struct MyExecutor;

#[async_trait::async_trait]
impl TaskExecutor for MyExecutor {
    async fn execute(
        &self,
        ctx: &mut TaskContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("Running task: {}", ctx.task_id);
        ctx.xcom_push("status", serde_json::json!("done"));
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    // 1. Build the DAG
    let mut dag = Dag::new("etl_pipeline");
    dag.add_task(Task::builder("extract").retries(2).build()).unwrap();
    dag.add_task(Task::builder("transform").build()).unwrap();
    dag.add_task(Task::builder("load").build()).unwrap();
    dag.chain(&[
        TaskId::new("extract"),
        TaskId::new("transform"),
        TaskId::new("load"),
    ]).unwrap();

    // 2. Register executors
    let executor: Arc<dyn TaskExecutor> = Arc::new(MyExecutor);
    let executors: HashMap<TaskId, Arc<dyn TaskExecutor>> = [
        "extract", "transform", "load",
    ].iter().map(|id| (TaskId::new(*id), Arc::clone(&executor))).collect();

    // 3. Run on Rebar
    let runtime = Arc::new(rebar::runtime::Runtime::new(4));
    let handle = spawn_scheduler(
        runtime, dag, executors, "run_001", chrono::Utc::now(),
    ).await;

    // 4. Wait for completion
    let final_state = handle
        .wait_for_completion(Duration::from_millis(50), Duration::from_secs(30))
        .await
        .unwrap();

    println!("DAG finished: {:?}", final_state);
}
```

## Diagrams

Any `Dag` can render itself as an ASCII diagram:

```rust
let dag = Dag::new("my_pipeline");
// ... add tasks and edges ...

// Horizontal (left-to-right, default)
print!("{}", dag.diagram());

// Vertical (top-to-bottom)
print!("{}", dag.diagram_vertical());
```

Output:

```
╔══════════════════╗
║ DAG: my_pipeline ║
╚══════════════════╝

┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│     extract      │     │    transform     │     │       load       │
│                  │ ───▶│    retries=2     │ ───▶│                  │
└──────────────────┘     └──────────────────┘     └──────────────────┘
```

Task boxes show trigger rules and retries when non-default. Fan-out/fan-in edges render as connectors between layers.

## Features

- **Directed acyclic graph** -- topological sort, cycle detection, roots/leaves queries
- **ASCII diagrams** -- `dag.diagram()` and `dag.diagram_vertical()` render any DAG
- **10 task states** -- None, Scheduled, Queued, Running, Success, Failed, Skipped, UpstreamFailed, UpForRetry, Removed
- **12 trigger rules** -- AllSuccess, AllFailed, AllDone, AllDoneMinOneSuccess, AllSkipped, OneSuccess, OneFailed, OneDone, NoneFailed, NoneFailedMinOneSuccess, NoneSkipped, Always
- **Task groups** -- hierarchical namespacing with `TaskGroup` and `GroupId`
- **XCom** -- key/value cross-communication via Rebar Agent
- **Retry support** -- configurable retries with attempt counting
- **Actor-based execution** -- each task runs via Rebar `async_task_ctx` with its own process
- **Scheduler actor** -- GenServer that drives DAG execution via tick/message loop
- **XCom Agent** -- Rebar Agent for shared XCom state (push/pull/clear via closures)
- **DagHandle** -- async API for querying run state and waiting for completion
- **Distributed execution** -- coordinator/worker architecture for scaling across machines via Rebar clustering
- **Mathematical verification** -- exhaustive state machine proofs (transition matrix, trigger rule completeness)

## Documentation

- [Architecture](docs/architecture.md) -- crate structure and layered design
- [State Machine](docs/state-machine.md) -- task and DAG run state transitions
- [Trigger Rules](docs/trigger-rules.md) -- all 12 rules with truth tables
- [Rebar Integration](docs/rebar-integration.md) -- actor model, message flow, XCom Agent
- [Distributed Execution](docs/distributed.md) -- coordinator/worker scaling across machines
- [Diagrams](docs/diagrams.md) -- ASCII diagram rendering API
- [Benchmark](docs/benchmark.md) -- image processing pipeline, 3.5x speedup with CPU limits

## Dependencies

| Crate | Purpose |
|-------|---------|
| `rebar` | Actor runtime (GenServer, Agent, Task, processes) |
| `tokio` | Async runtime |
| `chrono` | Timestamps and logical dates |
| `serde` / `serde_json` | Serialization for XCom values |
| `thiserror` | Error type derivation |
| `async-trait` | Async trait support for `TaskExecutor` |
| `rmpv` | MessagePack values for Rebar message payloads |
| `ratatui` / `crossterm` / `clap` | CLI/TUI tool |

## License

MIT
