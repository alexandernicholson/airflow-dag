# Architecture

## Crate Module Structure

```mermaid
graph TD
    LIB["lib.rs<br>(public re-exports)"]

    LIB --> TID["task_id.rs<br>TaskId, GroupId"]
    LIB --> TS["task_state.rs<br>TaskState enum"]
    LIB --> TR["trigger_rule.rs<br>TriggerRule, UpstreamSummary,<br>TriggerEvaluation"]
    LIB --> ERR["error.rs<br>DagError"]

    LIB --> TASK["task.rs<br>Task, TaskBuilder"]
    TASK --> TID
    TASK --> TR

    LIB --> TG["task_group.rs<br>TaskGroup, TaskDefaults"]
    TG --> TASK
    TG --> TID

    LIB --> DAG["dag.rs<br>Dag"]
    DAG --> TASK
    DAG --> TID
    DAG --> ERR

    LIB --> XCOM["xcom.rs<br>XComStore"]
    XCOM --> TID

    LIB --> DR["dag_run.rs<br>DagRun, DagRunState"]
    DR --> DAG
    DR --> TS
    DR --> TR
    DR --> XCOM
    DR --> ERR

    LIB --> EXEC["executor.rs<br>TaskExecutor, TaskContext"]
    EXEC --> TID
    EXEC --> XCOM

    LIB --> XA["xcom_actor.rs<br>XComAgent (Agent)"]
    XA --> XCOM

    LIB --> SA["scheduler_actor.rs<br>SchedulerServer, DagHandle,<br>spawn_scheduler"]
    SA --> DR
    SA --> EXEC
    SA --> XA

    style LIB fill:#4a9eff,color:#fff
    style SA fill:#ff6b6b,color:#fff
    style XA fill:#ff6b6b,color:#fff
    style DR fill:#ffa94d,color:#fff
    style DAG fill:#ffa94d,color:#fff
    style TASK fill:#51cf66,color:#fff
    style TG fill:#51cf66,color:#fff
    style TS fill:#845ef7,color:#fff
    style TR fill:#845ef7,color:#fff
    style TID fill:#845ef7,color:#fff
    style ERR fill:#845ef7,color:#fff
    style XCOM fill:#20c997,color:#fff
    style EXEC fill:#20c997,color:#fff
```

## Layered Design

The crate is organized in four layers. Each layer depends only on the layers below it.

### Layer 1: Pure Types (purple)

`task_id`, `task_state`, `trigger_rule`, `error`

These modules define value types with no runtime dependencies. `TaskState` is a `Copy` enum, `TriggerRule` is a pure function from `UpstreamSummary` to `TriggerEvaluation`, and `DagError` uses `thiserror` for display. Everything here is testable without async or I/O.

### Layer 2: DAG Structure (green)

`task`, `task_group`, `dag`

The `Task` struct holds the definition of a single unit of work -- its ID, trigger rule, retry count, pool, and group. `TaskBuilder` provides a fluent construction API. `TaskGroup` adds hierarchical namespacing. `Dag` is the graph itself: a `HashMap` of tasks with adjacency lists for upstream/downstream edges, plus topological sort via Kahn's algorithm.

### Layer 3: State Machine (orange)

`dag_run`, `xcom`, `executor`

`DagRun` is the runtime instantiation of a `Dag`. It owns the `HashMap<TaskId, TaskState>` that tracks every task's current state, enforces valid transitions (e.g., only `Running` can move to `Success`), and evaluates trigger rules on each `tick()` to propagate skips and upstream failures. `XComStore` is a nested `HashMap` for cross-task data. `TaskExecutor` is the trait users implement.

### Layer 4: Actors (red)

`scheduler_actor`, `xcom_actor`

This layer maps the state machine onto Rebar's actor model. `SchedulerServer` is a `GenServer` whose state is a `DagRun`. It receives `Tick` casts and `task_completed` info messages, dispatches ready tasks via `async_task_ctx`, and updates state accordingly. `XComAgent` wraps a Rebar `Agent` for shared XCom state -- no custom message types needed, just closures via `get`/`cast`. `DagHandle` is the user-facing async API.

## Data Flow

```mermaid
graph LR
    USER["User code"] -->|"spawn_scheduler()"| SA["SchedulerServer<br>(GenServer)"]
    SA -->|"tick()"| DR["DagRun<br>state machine"]
    DR -->|"ready tasks"| SA
    SA -->|"spawn process"| TP1["Task Process 1"]
    SA -->|"spawn process"| TP2["Task Process 2"]
    TP1 -->|"task_completed msg"| SA
    TP2 -->|"task_completed msg"| SA
    TP1 -->|"xcom push"| XA["XComAgent<br>(Agent)"]
    SA -->|"xcom push"| XA
    SA -->|"call: GetRunState"| USER

    style SA fill:#ff6b6b,color:#fff
    style XA fill:#ff6b6b,color:#fff
    style DR fill:#ffa94d,color:#fff
    style TP1 fill:#51cf66,color:#fff
    style TP2 fill:#51cf66,color:#fff
    style USER fill:#4a9eff,color:#fff
```

### Execution Lifecycle

1. **Spawn** -- `spawn_scheduler()` creates a `DagRun`, an `XComAgent`, and a `SchedulerServer`. It sends an initial `Tick` cast.

2. **Tick** -- The scheduler calls `dag_run.tick()`, which propagates upstream failures/skips and returns the list of ready `TaskId`s.

3. **Dispatch** -- For each ready task, the scheduler calls `mark_running()` and spawns a Rebar process via `async_task_ctx` that runs the corresponding `TaskExecutor::execute()`.

4. **Completion** -- When a task process finishes, it sends a `task_completed` JSON message back to the scheduler's `handle_info`. The scheduler calls `mark_success()` or `mark_failed()`, then ticks again.

5. **Retry** -- If a task fails and has remaining retries, `mark_failed()` transitions it to `UpForRetry` instead of `Failed`. The next tick picks it up as a ready task.

6. **Terminal** -- When all tasks reach terminal states, `update_run_state()` sets the `DagRunState` to `Success` (all ok) or `Failed` (any failure). The `DagHandle::wait_for_completion()` future resolves.
