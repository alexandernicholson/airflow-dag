# airflow-dag

Apache Airflow-compatible DAG semantics implemented in Rust on the [Rebar](https://github.com/alexandernicholson/rebar) actor runtime.

> `165 tests, 0 clippy warnings (pedantic)`

## Overview

`airflow-dag` brings the scheduling model of Apache Airflow into Rust. It provides:

- A type-safe DAG builder with dependency tracking and cycle detection
- A task state machine that faithfully mirrors Airflow's `TaskInstance` lifecycle
- All 12 Airflow trigger rules with correct evaluation semantics
- Automatic upstream failure and skip propagation
- XCom (cross-communication) for passing data between tasks
- Actor-based execution on the Rebar runtime with per-task processes
- Retry logic with configurable attempt counts

Tasks are defined as pure data. Execution logic is provided by implementing the `TaskExecutor` trait. The scheduler runs as a Rebar `GenServer` that evaluates trigger rules, dispatches task processes via `async_task_ctx`, and advances the DAG run to completion. XCom state is managed by a Rebar `Agent`.

## Quick Example

```rust
use airflow_dag::*;
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

## CLI Tool

```bash
# Render a DAG diagram (horizontal, left-to-right)
cargo run --bin dag-cli -- diagram diamond

# Render vertically (top-to-bottom)
cargo run --bin dag-cli -- diagram ml --vertical

# Run a demo DAG
cargo run --bin dag-cli -- demo

# Launch interactive TUI
cargo run --bin dag-cli -- tui
```

## Features

- **Directed acyclic graph** -- topological sort, cycle detection, roots/leaves queries
- **10 task states** -- None, Scheduled, Queued, Running, Success, Failed, Skipped, UpstreamFailed, UpForRetry, Removed
- **12 trigger rules** -- AllSuccess, AllFailed, AllDone, AllDoneMinOneSuccess, AllSkipped, OneSuccess, OneFailed, OneDone, NoneFailed, NoneFailedMinOneSuccess, NoneSkipped, Always
- **Task groups** -- hierarchical namespacing with `TaskGroup` and `GroupId`
- **XCom** -- key/value cross-communication via Rebar Agent
- **Retry support** -- configurable retries with attempt counting
- **Actor-based execution** -- each task runs via Rebar `async_task_ctx` with its own process
- **Scheduler actor** -- GenServer that drives DAG execution via tick/message loop
- **XCom Agent** -- Rebar Agent for shared XCom state (push/pull/clear via closures)
- **DagHandle** -- async API for querying run state and waiting for completion
- **Mathematical verification** -- exhaustive state machine proofs (transition matrix, trigger rule completeness)

## Documentation

- [Architecture](docs/architecture.md) -- crate structure and layered design
- [State Machine](docs/state-machine.md) -- task and DAG run state transitions
- [Trigger Rules](docs/trigger-rules.md) -- all 12 rules with truth tables
- [Rebar Integration](docs/rebar-integration.md) -- actor model, message flow, XCom Agent

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
