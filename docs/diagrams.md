# Diagrams

Every `Dag` can render itself as an ASCII diagram. This is a library function — no CLI or external tools needed.

## API

```rust
use ironpipe::{Dag, Task, TaskId, TriggerRule};

let mut dag = Dag::new("etl");
dag.add_task(Task::builder("extract").retries(2).build()).unwrap();
dag.add_task(Task::builder("transform").build()).unwrap();
dag.add_task(Task::builder("load").trigger_rule(TriggerRule::Always).build()).unwrap();
dag.chain(&[TaskId::new("extract"), TaskId::new("transform"), TaskId::new("load")]).unwrap();

// Horizontal (left-to-right)
print!("{}", dag.diagram());

// Vertical (top-to-bottom)
print!("{}", dag.diagram_vertical());
```

## Horizontal Layout

The default. Tasks flow left to right, parallel branches are stacked vertically.

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│     extract      │     │    transform     │     │       load       │
│    retries=2     │ ───▶│                  │ ───▶│      Always      │
└──────────────────┘     └──────────────────┘     └──────────────────┘
```

Fan-out and fan-in edges use L-shaped connectors:

```
                         ┌──────────────────┐
                         │     branch_a     │
                      ┌─▶│    retries=1     │ ─┐
┌──────────────────┐  │  └──────────────────┘  │  ┌──────────────────┐
│      start       │  │                        │  │       join       │
│                  │ ─┐  ┌──────────────────┐  └─▶│                  │
└──────────────────┘  │  │     branch_b     │  │  └──────────────────┘
                      └─▶│                  │ ─┘
                         └──────────────────┘
```

## Vertical Layout

Tasks flow top to bottom.

```
             ┌──────────────────┐
             │      start       │
             │                  │
             └──────────────────┘
           ┬──────────│──────────┬
           ▼                     ▼
  ┌──────────────────┐  ┌──────────────────┐
  │     branch_a     │  │     branch_b     │
  │    retries=1     │  │                  │
  └──────────────────┘  └──────────────────┘
           ┴──────────│──────────┴
                      ▼
             ┌──────────────────┐
             │       join       │
             │                  │
             └──────────────────┘
```

## What's Shown

Each task box displays:
- **Task ID** (centered, always shown)
- **Trigger rule** (if not `AllSuccess`, the default)
- **Retries** (if > 0)

The summary below the diagram shows:
- Total task count
- Root tasks (no upstream dependencies)
- Leaf tasks (no downstream dependencies)
- Topological order

## Cycle Handling

If the DAG contains a cycle, both functions return `"Error: DAG contains a cycle"` instead of a diagram.

## Module

The rendering functions are in `ironpipe::diagram`:

```rust
// Standalone functions
ironpipe::diagram::horizontal(&dag) -> String
ironpipe::diagram::vertical(&dag)   -> String

// Convenience methods on Dag
dag.diagram()          -> String
dag.diagram_vertical() -> String
```
