use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rebar::gen_server::{CallError, GenServer, GenServerContext, GenServerRef, spawn_gen_server};
use rebar::process::{Message, ProcessId};
use rebar::runtime::Runtime;
use rebar::task::async_task_ctx;

use crate::dag::Dag;
use crate::dag_run::{DagRun, DagRunState};
use crate::executor::{TaskContext, TaskExecutor};
use crate::task_id::TaskId;
use crate::task_state::TaskState;
use crate::xcom_actor::XComAgent;

/// Call messages for the scheduler actor.
pub enum SchedulerCall {
    GetRunState,
    GetTaskState(TaskId),
    GetAllTaskStates,
}

/// Cast messages for the scheduler actor.
pub enum SchedulerCast {
    /// Trigger a scheduler tick.
    Tick,
}

/// Reply messages from the scheduler actor.
pub enum SchedulerReply {
    RunState(DagRunState),
    TaskState(TaskState),
    AllTaskStates(HashMap<TaskId, TaskState>),
}

/// Internal state of the scheduler actor.
pub struct SchedulerState {
    dag_run: DagRun,
    runtime: Arc<Runtime>,
    executors: HashMap<TaskId, Arc<dyn TaskExecutor>>,
    xcom: XComAgent,
}

/// `GenServer` actor that orchestrates DAG execution.
/// Initial state is passed via a `Mutex<Option>` and taken during init.
pub struct SchedulerServer {
    initial_state: Mutex<Option<SchedulerState>>,
}

impl SchedulerServer {
    const fn new(state: SchedulerState) -> Self {
        Self {
            initial_state: Mutex::new(Some(state)),
        }
    }
}

#[async_trait::async_trait]
impl GenServer for SchedulerServer {
    type State = SchedulerState;
    type Call = SchedulerCall;
    type Cast = SchedulerCast;
    type Reply = SchedulerReply;

    async fn init(&self, _ctx: &GenServerContext) -> Result<Self::State, String> {
        self.initial_state
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| "SchedulerServer already initialized".to_string())
    }

    async fn handle_call(
        &self,
        msg: Self::Call,
        _from: ProcessId,
        state: &mut Self::State,
        _ctx: &GenServerContext,
    ) -> Self::Reply {
        match msg {
            SchedulerCall::GetRunState => SchedulerReply::RunState(state.dag_run.run_state()),
            SchedulerCall::GetTaskState(id) => {
                SchedulerReply::TaskState(state.dag_run.task_state(&id))
            }
            SchedulerCall::GetAllTaskStates => {
                let states: HashMap<TaskId, TaskState> = state
                    .dag_run
                    .dag()
                    .task_ids()
                    .map(|id| (id.clone(), state.dag_run.task_state(id)))
                    .collect();
                SchedulerReply::AllTaskStates(states)
            }
        }
    }

    async fn handle_cast(
        &self,
        msg: Self::Cast,
        state: &mut Self::State,
        ctx: &GenServerContext,
    ) {
        match msg {
            SchedulerCast::Tick => {
                dispatch_ready_tasks(state, ctx).await;
            }
        }
    }

    async fn handle_info(
        &self,
        msg: Message,
        state: &mut Self::State,
        ctx: &GenServerContext,
    ) {
        // Parse task completion messages from spawned task processes
        if let Some(payload_str) = msg.payload().as_str()
            && let Ok(json) = serde_json::from_str::<serde_json::Value>(payload_str)
            && json.get("type").and_then(|t| t.as_str()) == Some("task_completed")
        {
            let task_id_str = json
                .get("task_id")
                .and_then(|t| t.as_str())
                .unwrap_or("");
            let task_id = TaskId::new(task_id_str);
            let success = json
                .get("success")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);

            if success {
                let xcom_map: HashMap<String, serde_json::Value> = json
                    .get("xcom")
                    .and_then(|x| serde_json::from_value(x.clone()).ok())
                    .unwrap_or_default();

                // Push XCom values via the Agent
                for (key, value) in xcom_map {
                    state.xcom.push(task_id.clone(), key, value);
                }
                let _ = state.dag_run.mark_success(&task_id);
            } else {
                let _ = state.dag_run.mark_failed(&task_id);
            }

            // Tick to find and dispatch newly-ready tasks
            dispatch_ready_tasks(state, ctx).await;
        }
    }
}

/// Evaluate the DAG run and dispatch ready tasks for execution.
async fn dispatch_ready_tasks(state: &mut SchedulerState, ctx: &GenServerContext) {
    let ready = state.dag_run.tick();
    let scheduler_pid = ctx.self_pid();

    for task_id in ready {
        if let Some(executor) = state.executors.get(&task_id) {
            let _ = state.dag_run.mark_running(&task_id);

            let executor = Arc::clone(executor);
            let tid = task_id.clone();
            let run_id = state.dag_run.run_id.clone();
            let logical_date = state.dag_run.logical_date;
            let attempt = state.dag_run.attempt_count(&tid);

            // Use Rebar's Task API for proper result tracking
            let _task = async_task_ctx(&state.runtime, move |ctx| async move {
                let mut task_ctx =
                    TaskContext::new(tid.clone(), run_id, logical_date, attempt);

                let result = executor.execute(&mut task_ctx).await;

                let json = match result {
                    Ok(()) => {
                        let xcom_values = task_ctx.xcom_values().cloned().unwrap_or_default();
                        serde_json::json!({
                            "type": "task_completed",
                            "task_id": tid.0,
                            "success": true,
                            "xcom": xcom_values,
                        })
                    }
                    Err(e) => {
                        serde_json::json!({
                            "type": "task_completed",
                            "task_id": tid.0,
                            "success": false,
                            "error": e.to_string(),
                        })
                    }
                };

                let payload = rmpv::Value::String(
                    serde_json::to_string(&json).unwrap().into(),
                );
                let _ = ctx.send(scheduler_pid, payload).await;
            })
            .await;
        }
    }
}

/// A handle to a running DAG scheduler.
pub struct DagHandle {
    scheduler_ref: GenServerRef<SchedulerServer>,
}

impl DagHandle {
    /// Get the current run state.
    ///
    /// # Errors
    /// Returns `CallError` if the scheduler actor fails to respond.
    pub async fn run_state(&self) -> Result<DagRunState, CallError> {
        let reply = self
            .scheduler_ref
            .call(SchedulerCall::GetRunState, Duration::from_secs(5))
            .await?;
        match reply {
            SchedulerReply::RunState(s) => Ok(s),
            _ => unreachable!(),
        }
    }

    /// Get the state of a specific task.
    ///
    /// # Errors
    /// Returns `CallError` if the scheduler actor fails to respond.
    pub async fn task_state(&self, task_id: &TaskId) -> Result<TaskState, CallError> {
        let reply = self
            .scheduler_ref
            .call(
                SchedulerCall::GetTaskState(task_id.clone()),
                Duration::from_secs(5),
            )
            .await?;
        match reply {
            SchedulerReply::TaskState(s) => Ok(s),
            _ => unreachable!(),
        }
    }

    /// Get all task states.
    ///
    /// # Errors
    /// Returns `CallError` if the scheduler actor fails to respond.
    pub async fn all_task_states(&self) -> Result<HashMap<TaskId, TaskState>, CallError> {
        let reply = self
            .scheduler_ref
            .call(SchedulerCall::GetAllTaskStates, Duration::from_secs(5))
            .await?;
        match reply {
            SchedulerReply::AllTaskStates(s) => Ok(s),
            _ => unreachable!(),
        }
    }

    /// Check if the run is complete.
    ///
    /// # Errors
    /// Returns `CallError` if the scheduler actor fails to respond.
    pub async fn is_complete(&self) -> Result<bool, CallError> {
        let state = self.run_state().await?;
        Ok(matches!(state, DagRunState::Success | DagRunState::Failed))
    }

    /// Wait for the DAG run to complete, polling at the given interval.
    ///
    /// # Errors
    /// Returns `CallError` if the scheduler actor fails to respond.
    pub async fn wait_for_completion(
        &self,
        poll_interval: Duration,
        timeout: Duration,
    ) -> Result<DagRunState, CallError> {
        let start = tokio::time::Instant::now();
        loop {
            let state = self.run_state().await?;
            if matches!(state, DagRunState::Success | DagRunState::Failed) {
                return Ok(state);
            }
            if start.elapsed() > timeout {
                return Ok(state);
            }
            tokio::time::sleep(poll_interval).await;
        }
    }
}

/// Spawn a DAG scheduler as a `GenServer`.
/// This is the main entry point for running a DAG on Rebar.
///
/// Uses Rebar's `Agent` for `XCom` state and `async_task_ctx` for task execution.
pub async fn spawn_scheduler<S: ::std::hash::BuildHasher>(
    runtime: Arc<Runtime>,
    dag: Dag,
    executors: HashMap<TaskId, Arc<dyn TaskExecutor>, S>,
    run_id: impl Into<String>,
    logical_date: chrono::DateTime<chrono::Utc>,
) -> DagHandle {
    let xcom = XComAgent::start(Arc::clone(&runtime)).await;
    let dag_run = DagRun::new(dag, run_id, logical_date);

    let state = SchedulerState {
        dag_run,
        runtime: Arc::clone(&runtime),
        executors: executors.into_iter().collect(),
        xcom,
    };

    let server = SchedulerServer::new(state);
    let scheduler_ref = spawn_gen_server(Arc::clone(&runtime), server).await;

    // Trigger initial tick to start executing root tasks
    scheduler_ref.cast(SchedulerCast::Tick).ok();

    DagHandle { scheduler_ref }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::TaskExecutor;
    use crate::task::Task;
    use chrono::Utc;
    use serde_json::json;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct NoopExecutor;

    #[async_trait::async_trait]
    impl TaskExecutor for NoopExecutor {
        async fn execute(
            &self,
            _ctx: &mut TaskContext,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            Ok(())
        }
    }

    struct XComProducer {
        value: serde_json::Value,
    }

    #[async_trait::async_trait]
    impl TaskExecutor for XComProducer {
        async fn execute(
            &self,
            ctx: &mut TaskContext,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            ctx.xcom_push("return_value", self.value.clone());
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
            Err("intentional failure".into())
        }
    }

    struct CountingExecutor {
        count: Arc<AtomicU32>,
    }

    #[async_trait::async_trait]
    impl TaskExecutor for CountingExecutor {
        async fn execute(
            &self,
            _ctx: &mut TaskContext,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn simple_task(id: &str) -> Task {
        Task::builder(id).build()
    }

    fn make_executors(
        tasks: &[&str],
        executor: Arc<dyn TaskExecutor>,
    ) -> HashMap<TaskId, Arc<dyn TaskExecutor>> {
        tasks
            .iter()
            .map(|id| (TaskId::new(*id), Arc::clone(&executor)))
            .collect()
    }

    #[tokio::test]
    async fn single_task_completes() {
        let rt = Arc::new(Runtime::new(1));
        let mut dag = Dag::new("test");
        dag.add_task(simple_task("a")).unwrap();

        let executors = make_executors(&["a"], Arc::new(NoopExecutor));
        let handle = spawn_scheduler(rt, dag, executors, "run_1", Utc::now()).await;

        let state = handle
            .wait_for_completion(Duration::from_millis(10), Duration::from_secs(5))
            .await
            .unwrap();

        assert_eq!(state, DagRunState::Success);
    }

    #[tokio::test]
    async fn linear_chain_completes_in_order() {
        let rt = Arc::new(Runtime::new(1));
        let mut dag = Dag::new("test");
        dag.add_task(simple_task("a")).unwrap();
        dag.add_task(simple_task("b")).unwrap();
        dag.add_task(simple_task("c")).unwrap();
        dag.chain(&[TaskId::new("a"), TaskId::new("b"), TaskId::new("c")])
            .unwrap();

        let executors = make_executors(&["a", "b", "c"], Arc::new(NoopExecutor));
        let handle = spawn_scheduler(rt, dag, executors, "run_1", Utc::now()).await;

        let state = handle
            .wait_for_completion(Duration::from_millis(10), Duration::from_secs(5))
            .await
            .unwrap();

        assert_eq!(state, DagRunState::Success);
        assert_eq!(handle.task_state(&TaskId::new("a")).await.unwrap(), TaskState::Success);
        assert_eq!(handle.task_state(&TaskId::new("b")).await.unwrap(), TaskState::Success);
        assert_eq!(handle.task_state(&TaskId::new("c")).await.unwrap(), TaskState::Success);
    }

    #[tokio::test]
    async fn diamond_dag_completes() {
        let rt = Arc::new(Runtime::new(1));
        let mut dag = Dag::new("test");
        dag.add_task(simple_task("a")).unwrap();
        dag.add_task(simple_task("b")).unwrap();
        dag.add_task(simple_task("c")).unwrap();
        dag.add_task(simple_task("d")).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("b")).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("c")).unwrap();
        dag.set_downstream(&TaskId::new("b"), &TaskId::new("d")).unwrap();
        dag.set_downstream(&TaskId::new("c"), &TaskId::new("d")).unwrap();

        let counter = Arc::new(AtomicU32::new(0));
        let executors: HashMap<TaskId, Arc<dyn TaskExecutor>> = ["a", "b", "c", "d"]
            .iter()
            .map(|id| {
                (
                    TaskId::new(*id),
                    Arc::new(CountingExecutor {
                        count: Arc::clone(&counter),
                    }) as Arc<dyn TaskExecutor>,
                )
            })
            .collect();

        let handle = spawn_scheduler(rt, dag, executors, "run_1", Utc::now()).await;

        let state = handle
            .wait_for_completion(Duration::from_millis(10), Duration::from_secs(5))
            .await
            .unwrap();

        assert_eq!(state, DagRunState::Success);
        assert_eq!(counter.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn task_failure_propagates() {
        let rt = Arc::new(Runtime::new(1));
        let mut dag = Dag::new("test");
        dag.add_task(simple_task("a")).unwrap();
        dag.add_task(simple_task("b")).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("b")).unwrap();

        let mut executors: HashMap<TaskId, Arc<dyn TaskExecutor>> = HashMap::new();
        executors.insert(TaskId::new("a"), Arc::new(FailingExecutor));
        executors.insert(TaskId::new("b"), Arc::new(NoopExecutor));

        let handle = spawn_scheduler(rt, dag, executors, "run_1", Utc::now()).await;

        let state = handle
            .wait_for_completion(Duration::from_millis(10), Duration::from_secs(5))
            .await
            .unwrap();

        assert_eq!(state, DagRunState::Failed);
        assert_eq!(handle.task_state(&TaskId::new("a")).await.unwrap(), TaskState::Failed);
        assert_eq!(handle.task_state(&TaskId::new("b")).await.unwrap(), TaskState::UpstreamFailed);
    }

    #[tokio::test]
    async fn xcom_produced_by_task() {
        let rt = Arc::new(Runtime::new(1));
        let mut dag = Dag::new("test");
        dag.add_task(simple_task("producer")).unwrap();

        let mut executors: HashMap<TaskId, Arc<dyn TaskExecutor>> = HashMap::new();
        executors.insert(
            TaskId::new("producer"),
            Arc::new(XComProducer { value: json!("hello from producer") }),
        );

        let handle = spawn_scheduler(rt, dag, executors, "run_1", Utc::now()).await;

        let state = handle
            .wait_for_completion(Duration::from_millis(10), Duration::from_secs(5))
            .await
            .unwrap();

        assert_eq!(state, DagRunState::Success);
    }

    #[tokio::test]
    async fn task_with_retries_eventually_fails() {
        let rt = Arc::new(Runtime::new(1));
        let mut dag = Dag::new("test");
        dag.add_task(Task::builder("a").retries(1).build()).unwrap();

        let counter = Arc::new(AtomicU32::new(0));
        let mut executors: HashMap<TaskId, Arc<dyn TaskExecutor>> = HashMap::new();

        struct CountingFailer(Arc<AtomicU32>);
        #[async_trait::async_trait]
        impl TaskExecutor for CountingFailer {
            async fn execute(
                &self,
                _ctx: &mut TaskContext,
            ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Err("always fails".into())
            }
        }

        executors.insert(
            TaskId::new("a"),
            Arc::new(CountingFailer(Arc::clone(&counter))),
        );

        let handle = spawn_scheduler(rt, dag, executors, "run_1", Utc::now()).await;

        let state = handle
            .wait_for_completion(Duration::from_millis(10), Duration::from_secs(5))
            .await
            .unwrap();

        assert_eq!(state, DagRunState::Failed);
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }
}
