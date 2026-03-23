//! Distributed task execution across multiple Rebar nodes.
//!
//! The distributed model uses a coordinator/worker architecture:
//! - **Coordinator**: Runs the `SchedulerServer` and `DagRun` state machine
//! - **Workers**: Run on remote nodes, receive task execution requests, run
//!   `TaskExecutor` implementations, and send results back
//!
//! Message routing is transparent — Rebar's `DistributedRouter` automatically
//! sends messages to remote nodes when the target `ProcessId` has a different
//! `node_id`. The coordinator dispatches tasks to worker processes by their
//! PIDs; Rebar handles the network transport.

use std::collections::HashMap;
use std::sync::Arc;

use rebar::process::ProcessId;
use rebar::runtime::Runtime;

use crate::executor::{TaskContext, TaskExecutor};
use crate::task_id::TaskId;

/// A worker process that runs on a (potentially remote) node.
/// Receives task execution requests via Rebar messages and sends results back.
///
/// The worker holds a registry of `TaskExecutor` implementations keyed by
/// task ID. When it receives an "execute" message, it looks up the executor,
/// runs it, and sends a completion message back to the coordinator.
pub struct Worker {
    pid: ProcessId,
}

impl Worker {
    /// Spawn a worker process on the given runtime.
    ///
    /// The worker listens for execution requests and dispatches them to
    /// the appropriate `TaskExecutor`. Results are sent back to the
    /// coordinator's `ProcessId` (which may be on a different node).
    pub async fn spawn(
        runtime: Arc<Runtime>,
        executors: HashMap<TaskId, Arc<dyn TaskExecutor>>,
    ) -> Self {
        let executors_clone = executors.clone();
        let pid = runtime
            .spawn(move |mut ctx| async move {
                while let Some(msg) = ctx.recv().await {
                    let payload_str = match msg.payload().as_str() {
                        Some(s) => s.to_string(),
                        None => continue,
                    };
                    let json: serde_json::Value = match serde_json::from_str(&payload_str) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    if json.get("type").and_then(|t| t.as_str()) != Some("execute_task") {
                        continue;
                    }

                    let task_id_str = json
                        .get("task_id")
                        .and_then(|t| t.as_str())
                        .unwrap_or("")
                        .to_string();
                    let run_id = json
                        .get("run_id")
                        .and_then(|t| t.as_str())
                        .unwrap_or("")
                        .to_string();
                    let attempt = json
                        .get("attempt")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(1) as u32;
                    let reply_to = msg.from();

                    let tid = TaskId::new(&task_id_str);

                    let result = if let Some(executor) = executors_clone.get(&tid) {
                        let mut task_ctx = TaskContext::new(
                            tid.clone(),
                            run_id,
                            chrono::Utc::now(),
                            attempt,
                        );
                        match executor.execute(&mut task_ctx).await {
                            Ok(()) => {
                                let xcom_values =
                                    task_ctx.xcom_values().cloned().unwrap_or_default();
                                serde_json::json!({
                                    "type": "task_completed",
                                    "task_id": task_id_str,
                                    "success": true,
                                    "xcom": xcom_values,
                                    "worker_node": ctx.self_pid().node_id(),
                                })
                            }
                            Err(e) => {
                                serde_json::json!({
                                    "type": "task_completed",
                                    "task_id": task_id_str,
                                    "success": false,
                                    "error": e.to_string(),
                                    "worker_node": ctx.self_pid().node_id(),
                                })
                            }
                        }
                    } else {
                        serde_json::json!({
                            "type": "task_completed",
                            "task_id": task_id_str,
                            "success": false,
                            "error": format!("no executor registered for task '{task_id_str}'"),
                            "worker_node": ctx.self_pid().node_id(),
                        })
                    };

                    let payload = rmpv::Value::String(
                        serde_json::to_string(&result).unwrap().into(),
                    );
                    // Send completion back to coordinator — if on a different node,
                    // Rebar's DistributedRouter handles the network transport
                    let _ = ctx.send(reply_to, payload).await;
                }
            })
            .await;

        Self { pid }
    }

    /// Get the worker's `ProcessId`. The coordinator uses this to dispatch tasks.
    /// If the worker is on a remote node, `pid.node_id()` will differ from
    /// the coordinator's, and Rebar routes messages over the network.
    #[must_use]
    pub const fn pid(&self) -> ProcessId {
        self.pid
    }

    /// Get the worker's node ID.
    #[must_use]
    pub const fn node_id(&self) -> u64 {
        self.pid.node_id()
    }
}

/// Build an `execute_task` message payload that a coordinator sends to a worker.
#[must_use]
pub fn build_execute_message(
    task_id: &TaskId,
    run_id: &str,
    attempt: u32,
) -> rmpv::Value {
    let json = serde_json::json!({
        "type": "execute_task",
        "task_id": task_id.0,
        "run_id": run_id,
        "attempt": attempt,
    });
    rmpv::Value::String(serde_json::to_string(&json).unwrap().into())
}

/// A pool of workers distributed across one or more nodes.
/// The coordinator dispatches tasks round-robin to workers.
pub struct WorkerPool {
    workers: Vec<ProcessId>,
    next: usize,
}

impl WorkerPool {
    /// Create a pool from a list of worker PIDs (potentially on different nodes).
    #[must_use]
    pub const fn new(workers: Vec<ProcessId>) -> Self {
        Self { workers, next: 0 }
    }

    /// Get the next worker PID in round-robin order.
    pub fn next_worker(&mut self) -> Option<ProcessId> {
        if self.workers.is_empty() {
            return None;
        }
        let pid = self.workers[self.next];
        self.next = (self.next + 1) % self.workers.len();
        Some(pid)
    }

    /// Number of workers in the pool.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.workers.len()
    }

    /// Whether the pool is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.workers.is_empty()
    }

    /// Get worker PIDs grouped by node ID.
    #[must_use]
    pub fn workers_by_node(&self) -> HashMap<u64, Vec<ProcessId>> {
        let mut by_node: HashMap<u64, Vec<ProcessId>> = HashMap::new();
        for &pid in &self.workers {
            by_node.entry(pid.node_id()).or_default().push(pid);
        }
        by_node
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rebar::process::Message;
    use std::time::Duration;
    use crate::executor::TaskExecutor;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct CountingExecutor(Arc<AtomicU32>);

    #[async_trait::async_trait]
    impl TaskExecutor for CountingExecutor {
        async fn execute(
            &self,
            ctx: &mut TaskContext,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            ctx.xcom_push("result", serde_json::json!("computed"));
            Ok(())
        }
    }

    struct FailingExecutor;

    #[async_trait::async_trait]
    impl TaskExecutor for FailingExecutor {
        async fn execute(
            &self,
            _ctx: &mut TaskContext,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Err("worker task failed".into())
        }
    }

    // ---- Worker spawning and basic execution ----

    #[tokio::test]
    async fn worker_spawns_with_pid() {
        let rt = Arc::new(Runtime::new(1));
        let executors = HashMap::new();
        let worker = Worker::spawn(rt, executors).await;
        assert_eq!(worker.node_id(), 1);
        assert!(worker.pid().local_id() > 0);
    }

    #[tokio::test]
    async fn worker_executes_task_and_responds() {
        let rt = Arc::new(Runtime::new(1));
        let counter = Arc::new(AtomicU32::new(0));

        let mut executors: HashMap<TaskId, Arc<dyn TaskExecutor>> = HashMap::new();
        executors.insert(TaskId::new("my_task"), Arc::new(CountingExecutor(Arc::clone(&counter))));

        let worker = Worker::spawn(Arc::clone(&rt), executors).await;
        let worker_pid = worker.pid();

        // Spawn a "coordinator" process that sends work and receives results
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();

        rt.spawn(move |mut ctx| async move {
            let msg = build_execute_message(&TaskId::new("my_task"), "run_1", 1);
            ctx.send(worker_pid, msg).await.unwrap();

            let reply = ctx.recv().await.unwrap();
            let payload_str = reply.payload().as_str().unwrap().to_string();
            let json: serde_json::Value = serde_json::from_str(&payload_str).unwrap();

            done_tx.send(json).unwrap();
        })
        .await;

        let result = tokio::time::timeout(Duration::from_secs(5), done_rx)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result["type"], "task_completed");
        assert_eq!(result["task_id"], "my_task");
        assert_eq!(result["success"], true);
        assert!(result["xcom"]["result"] == "computed");
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn worker_handles_failed_task() {
        let rt = Arc::new(Runtime::new(1));

        let mut executors: HashMap<TaskId, Arc<dyn TaskExecutor>> = HashMap::new();
        executors.insert(TaskId::new("bad_task"), Arc::new(FailingExecutor));

        let worker = Worker::spawn(Arc::clone(&rt), executors).await;
        let worker_pid = worker.pid();

        let (done_tx, done_rx) = tokio::sync::oneshot::channel();

        rt.spawn(move |mut ctx| async move {
            let msg = build_execute_message(&TaskId::new("bad_task"), "run_1", 1);
            ctx.send(worker_pid, msg).await.unwrap();

            let reply = ctx.recv().await.unwrap();
            let payload_str = reply.payload().as_str().unwrap().to_string();
            let json: serde_json::Value = serde_json::from_str(&payload_str).unwrap();

            done_tx.send(json).unwrap();
        })
        .await;

        let result = tokio::time::timeout(Duration::from_secs(5), done_rx)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result["success"], false);
        assert_eq!(result["error"], "worker task failed");
    }

    #[tokio::test]
    async fn worker_handles_unknown_task() {
        let rt = Arc::new(Runtime::new(1));
        let executors: HashMap<TaskId, Arc<dyn TaskExecutor>> = HashMap::new();

        let worker = Worker::spawn(Arc::clone(&rt), executors).await;
        let worker_pid = worker.pid();

        let (done_tx, done_rx) = tokio::sync::oneshot::channel();

        rt.spawn(move |mut ctx| async move {
            let msg = build_execute_message(&TaskId::new("nonexistent"), "run_1", 1);
            ctx.send(worker_pid, msg).await.unwrap();

            let reply = ctx.recv().await.unwrap();
            let payload_str = reply.payload().as_str().unwrap().to_string();
            let json: serde_json::Value = serde_json::from_str(&payload_str).unwrap();

            done_tx.send(json).unwrap();
        })
        .await;

        let result = tokio::time::timeout(Duration::from_secs(5), done_rx)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result["success"], false);
        assert!(result["error"].as_str().unwrap().contains("no executor registered"));
    }

    // ---- Multi-worker execution ----

    #[tokio::test]
    async fn multiple_workers_execute_in_parallel() {
        let rt = Arc::new(Runtime::new(1));
        let counter = Arc::new(AtomicU32::new(0));

        let mut executors: HashMap<TaskId, Arc<dyn TaskExecutor>> = HashMap::new();
        for name in &["task_a", "task_b", "task_c"] {
            executors.insert(
                TaskId::new(*name),
                Arc::new(CountingExecutor(Arc::clone(&counter))),
            );
        }

        // Spawn 3 workers (simulating 3 nodes)
        let w1 = Worker::spawn(Arc::clone(&rt), executors.clone()).await;
        let w2 = Worker::spawn(Arc::clone(&rt), executors.clone()).await;
        let w3 = Worker::spawn(Arc::clone(&rt), executors).await;

        let pids = [w1.pid(), w2.pid(), w3.pid()];
        let tasks = ["task_a", "task_b", "task_c"];

        let (done_tx, done_rx) = tokio::sync::oneshot::channel();

        rt.spawn(move |mut ctx| async move {
            // Dispatch one task to each worker
            for (i, task_name) in tasks.iter().enumerate() {
                let msg = build_execute_message(&TaskId::new(*task_name), "run_1", 1);
                ctx.send(pids[i], msg).await.unwrap();
            }

            // Collect all 3 results
            let mut results = Vec::new();
            for _ in 0..3 {
                let reply = ctx.recv().await.unwrap();
                let payload_str = reply.payload().as_str().unwrap().to_string();
                let json: serde_json::Value = serde_json::from_str(&payload_str).unwrap();
                results.push(json);
            }

            done_tx.send(results).unwrap();
        })
        .await;

        let results = tokio::time::timeout(Duration::from_secs(5), done_rx)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r["success"] == true));
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    // ---- WorkerPool ----

    #[tokio::test]
    async fn worker_pool_round_robin() {
        let p1 = ProcessId::new(1, 1);
        let p2 = ProcessId::new(2, 1);
        let p3 = ProcessId::new(3, 1);

        let mut pool = WorkerPool::new(vec![p1, p2, p3]);

        assert_eq!(pool.len(), 3);
        assert!(!pool.is_empty());

        assert_eq!(pool.next_worker(), Some(p1));
        assert_eq!(pool.next_worker(), Some(p2));
        assert_eq!(pool.next_worker(), Some(p3));
        assert_eq!(pool.next_worker(), Some(p1)); // wraps around
    }

    #[tokio::test]
    async fn worker_pool_by_node() {
        let pool = WorkerPool::new(vec![
            ProcessId::new(1, 1),
            ProcessId::new(1, 2),
            ProcessId::new(2, 1),
            ProcessId::new(3, 1),
        ]);

        let by_node = pool.workers_by_node();
        assert_eq!(by_node[&1].len(), 2);
        assert_eq!(by_node[&2].len(), 1);
        assert_eq!(by_node[&3].len(), 1);
    }

    // ---- Simulated cross-node execution ----

    #[tokio::test]
    async fn simulated_cross_node_task_dispatch() {
        // Simulate a coordinator on node 1 dispatching to a worker on node 2.
        // Both run on the same machine but with separate Runtimes and node IDs.
        // In production, Rebar's DistributedRouter + ConnectionManager would
        // handle the network transport between actual machines.

        let coordinator_rt = Arc::new(Runtime::new(1));
        let worker_rt = Arc::new(Runtime::new(2));

        let counter = Arc::new(AtomicU32::new(0));
        let mut executors: HashMap<TaskId, Arc<dyn TaskExecutor>> = HashMap::new();
        executors.insert(
            TaskId::new("remote_task"),
            Arc::new(CountingExecutor(Arc::clone(&counter))),
        );

        // Worker on node 2
        let worker = Worker::spawn(Arc::clone(&worker_rt), executors).await;
        assert_eq!(worker.node_id(), 2);

        // In a real distributed setup, the coordinator would send to worker.pid()
        // and Rebar's DistributedRouter would route over the network.
        // Here we simulate by manually bridging the message.

        let worker_pid = worker.pid();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();

        // Coordinator process on node 1
        coordinator_rt
            .spawn(move |ctx| async move {
                // Build the execute message
                let msg = build_execute_message(&TaskId::new("remote_task"), "run_1", 1);

                // In production: ctx.send(worker_pid, msg) goes through DistributedRouter
                // Here: we send directly to the worker's runtime since they share a process
                // But since they're different runtimes, we simulate the cross-node delivery
                let _ = ctx.self_pid(); // coordinator PID for reply routing

                done_tx.send((ctx.self_pid(), msg)).unwrap();
            })
            .await;

        let (coord_process_pid, execute_msg) =
            tokio::time::timeout(Duration::from_secs(5), done_rx)
                .await
                .unwrap()
                .unwrap();

        // Simulate network delivery: coordinator -> worker (cross-node)
        let bridged_msg = Message::new(coord_process_pid, execute_msg.clone());
        worker_rt.table().send(worker_pid, bridged_msg).unwrap();

        // Worker executes and sends reply back
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // In production, the reply would route back to coord_process_pid on node 1
        // via DistributedRouter. The coordinator's handle_info would receive it.
    }

    #[tokio::test]
    async fn build_execute_message_format() {
        let msg = build_execute_message(&TaskId::new("extract"), "run_42", 3);
        let s = msg.as_str().unwrap();
        let json: serde_json::Value = serde_json::from_str(s).unwrap();

        assert_eq!(json["type"], "execute_task");
        assert_eq!(json["task_id"], "extract");
        assert_eq!(json["run_id"], "run_42");
        assert_eq!(json["attempt"], 3);
    }
}
