use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::dag::Dag;
use crate::error::DagError;
use crate::task_id::TaskId;
use crate::task_state::TaskState;
use crate::trigger_rule::{TriggerEvaluation, UpstreamSummary};
use crate::xcom::XComStore;

/// State of a DAG run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DagRunState {
    Queued,
    Running,
    Success,
    Failed,
}

/// An instantiation of a DAG in time. Owns task states and drives scheduling.
#[derive(Debug)]
pub struct DagRun {
    dag: Dag,
    pub run_id: String,
    state: DagRunState,
    task_states: HashMap<TaskId, TaskState>,
    attempt_counts: HashMap<TaskId, u32>,
    pub logical_date: DateTime<Utc>,
    pub data_interval_start: Option<DateTime<Utc>>,
    pub data_interval_end: Option<DateTime<Utc>>,
    xcom: XComStore,
}

impl DagRun {
    /// Create a new DAG run. All tasks start in TaskState::None.
    pub fn new(dag: Dag, run_id: impl Into<String>, logical_date: DateTime<Utc>) -> Self {
        let task_states: HashMap<TaskId, TaskState> = dag
            .task_ids()
            .map(|id| (id.clone(), TaskState::None))
            .collect();
        let attempt_counts: HashMap<TaskId, u32> = dag
            .task_ids()
            .map(|id| (id.clone(), 0))
            .collect();
        Self {
            dag,
            run_id: run_id.into(),
            state: DagRunState::Queued,
            task_states,
            attempt_counts,
            logical_date,
            data_interval_start: None,
            data_interval_end: None,
            xcom: XComStore::new(),
        }
    }

    /// Get the current state of a task.
    pub fn task_state(&self, id: &TaskId) -> TaskState {
        self.task_states
            .get(id)
            .copied()
            .unwrap_or(TaskState::None)
    }

    /// Get the overall run state.
    pub fn run_state(&self) -> DagRunState {
        self.state
    }

    /// Get a reference to the underlying DAG.
    pub fn dag(&self) -> &Dag {
        &self.dag
    }

    /// Get the attempt count for a task.
    pub fn attempt_count(&self, id: &TaskId) -> u32 {
        self.attempt_counts.get(id).copied().unwrap_or(0)
    }

    /// Mark a task as running. Valid from None, Scheduled, or UpForRetry.
    pub fn mark_running(&mut self, id: &TaskId) -> Result<(), DagError> {
        let current = self.task_state(id);
        match current {
            TaskState::None | TaskState::Scheduled | TaskState::UpForRetry => {
                self.task_states.insert(id.clone(), TaskState::Running);
                *self.attempt_counts.entry(id.clone()).or_insert(0) += 1;
                self.state = DagRunState::Running;
                Ok(())
            }
            _ => Err(DagError::InvalidStateTransition {
                task_id: id.clone(),
                from: current,
                to: TaskState::Running,
            }),
        }
    }

    /// Mark a task as successful. Valid from Running.
    pub fn mark_success(&mut self, id: &TaskId) -> Result<(), DagError> {
        let current = self.task_state(id);
        if current != TaskState::Running {
            return Err(DagError::InvalidStateTransition {
                task_id: id.clone(),
                from: current,
                to: TaskState::Success,
            });
        }
        self.task_states.insert(id.clone(), TaskState::Success);
        self.update_run_state();
        Ok(())
    }

    /// Mark a task as failed. Valid from Running.
    pub fn mark_failed(&mut self, id: &TaskId) -> Result<(), DagError> {
        let current = self.task_state(id);
        if current != TaskState::Running {
            return Err(DagError::InvalidStateTransition {
                task_id: id.clone(),
                from: current,
                to: TaskState::Failed,
            });
        }

        // Check if we should retry
        if self.should_retry(id) {
            self.task_states.insert(id.clone(), TaskState::UpForRetry);
        } else {
            self.task_states.insert(id.clone(), TaskState::Failed);
        }
        self.update_run_state();
        Ok(())
    }

    /// Mark a task as skipped.
    pub fn mark_skipped(&mut self, id: &TaskId) -> Result<(), DagError> {
        self.task_states.insert(id.clone(), TaskState::Skipped);
        self.update_run_state();
        Ok(())
    }

    /// Check if the task should be retried.
    fn should_retry(&self, id: &TaskId) -> bool {
        let task = match self.dag.get_task(id) {
            Some(t) => t,
            None => return false,
        };
        let attempts = self.attempt_counts.get(id).copied().unwrap_or(0);
        attempts <= task.retries
    }

    /// Compute the upstream summary for a task.
    fn upstream_summary(&self, id: &TaskId) -> UpstreamSummary {
        let upstream_ids = self.dag.upstream_of(id);
        let states: Vec<TaskState> = upstream_ids
            .iter()
            .map(|uid| self.task_state(uid))
            .collect();
        UpstreamSummary::from_states(&states)
    }

    /// Get tasks that are ready to run based on trigger rule evaluation.
    pub fn ready_tasks(&self) -> Vec<TaskId> {
        let mut ready = Vec::new();
        for id in self.dag.task_ids() {
            let state = self.task_state(id);
            // Only consider tasks that haven't started or are up for retry
            if state != TaskState::None && state != TaskState::UpForRetry {
                continue;
            }
            let summary = self.upstream_summary(id);
            let task = self.dag.get_task(id).unwrap();
            let eval = task.trigger_rule.evaluate(&summary);
            if eval == TriggerEvaluation::Ready {
                ready.push(id.clone());
            }
        }
        // Sort for deterministic ordering
        ready.sort();
        ready
    }

    /// Run one scheduler tick: propagate UpstreamFailed/Skip states and return newly-ready tasks.
    pub fn tick(&mut self) -> Vec<TaskId> {
        // First pass: propagate UpstreamFailed and Skip
        let mut changes = Vec::new();
        for id in self.dag.task_ids() {
            let state = self.task_state(id);
            if state != TaskState::None {
                continue;
            }
            let summary = self.upstream_summary(id);
            let task = self.dag.get_task(id).unwrap();
            let eval = task.trigger_rule.evaluate(&summary);
            match eval {
                TriggerEvaluation::UpstreamFailed => {
                    changes.push((id.clone(), TaskState::UpstreamFailed));
                }
                TriggerEvaluation::Skip => {
                    changes.push((id.clone(), TaskState::Skipped));
                }
                _ => {}
            }
        }

        for (id, new_state) in changes {
            self.task_states.insert(id, new_state);
        }

        self.update_run_state();

        // Second pass: find ready tasks
        self.ready_tasks()
    }

    /// Check if the DAG run is complete (all tasks in terminal states).
    pub fn is_complete(&self) -> bool {
        self.task_states.values().all(|s| s.is_finished())
    }

    /// Update the overall run state based on task states.
    fn update_run_state(&mut self) {
        if !self.is_complete() {
            // If any task is running/queued/scheduled, we're running
            let has_active = self.task_states.values().any(|s| {
                matches!(
                    s,
                    TaskState::Running
                        | TaskState::Queued
                        | TaskState::Scheduled
                        | TaskState::UpForRetry
                )
            });
            if has_active {
                self.state = DagRunState::Running;
            }
            return;
        }

        // All tasks are in terminal states
        let any_failed = self.task_states.values().any(|s| s.is_failure());
        if any_failed {
            self.state = DagRunState::Failed;
        } else {
            self.state = DagRunState::Success;
        }
    }

    /// Push an XCom value.
    pub fn xcom_push(&mut self, task_id: &TaskId, key: &str, value: serde_json::Value) {
        self.xcom.push(task_id, key, value);
    }

    /// Pull an XCom value.
    pub fn xcom_pull(&self, task_id: &TaskId, key: &str) -> Option<&serde_json::Value> {
        self.xcom.pull(task_id, key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::Task;
    use crate::trigger_rule::TriggerRule;
    use chrono::Utc;
    use serde_json::json;
    fn simple_task(id: &str) -> Task {
        Task::builder(id).build()
    }

    fn task_with_retries(id: &str, retries: u32) -> Task {
        Task::builder(id).retries(retries).build()
    }

    fn task_with_rule(id: &str, rule: TriggerRule) -> Task {
        Task::builder(id).trigger_rule(rule).build()
    }

    fn make_linear_dag() -> Dag {
        let mut dag = Dag::new("test");
        dag.add_task(simple_task("a")).unwrap();
        dag.add_task(simple_task("b")).unwrap();
        dag.add_task(simple_task("c")).unwrap();
        dag.chain(&[TaskId::new("a"), TaskId::new("b"), TaskId::new("c")])
            .unwrap();
        dag
    }

    fn make_diamond_dag() -> Dag {
        let mut dag = Dag::new("test");
        dag.add_task(simple_task("a")).unwrap();
        dag.add_task(simple_task("b")).unwrap();
        dag.add_task(simple_task("c")).unwrap();
        dag.add_task(simple_task("d")).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("b")).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("c")).unwrap();
        dag.set_downstream(&TaskId::new("b"), &TaskId::new("d")).unwrap();
        dag.set_downstream(&TaskId::new("c"), &TaskId::new("d")).unwrap();
        dag
    }

    // --- Construction ---

    #[test]
    fn new_dag_run_all_tasks_none() {
        let dag = make_linear_dag();
        let run = DagRun::new(dag, "run_1", Utc::now());
        assert_eq!(run.task_state(&TaskId::new("a")), TaskState::None);
        assert_eq!(run.task_state(&TaskId::new("b")), TaskState::None);
        assert_eq!(run.task_state(&TaskId::new("c")), TaskState::None);
        assert_eq!(run.run_state(), DagRunState::Queued);
    }

    // --- State Transitions ---

    #[test]
    fn mark_running_from_none() {
        let dag = make_linear_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.mark_running(&TaskId::new("a")).unwrap();
        assert_eq!(run.task_state(&TaskId::new("a")), TaskState::Running);
        assert_eq!(run.run_state(), DagRunState::Running);
    }

    #[test]
    fn mark_success_from_running() {
        let dag = make_linear_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_success(&TaskId::new("a")).unwrap();
        assert_eq!(run.task_state(&TaskId::new("a")), TaskState::Success);
    }

    #[test]
    fn mark_failed_from_running() {
        let dag = make_linear_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_failed(&TaskId::new("a")).unwrap();
        assert_eq!(run.task_state(&TaskId::new("a")), TaskState::Failed);
    }

    #[test]
    fn mark_running_from_success_errors() {
        let dag = make_linear_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_success(&TaskId::new("a")).unwrap();
        let err = run.mark_running(&TaskId::new("a")).unwrap_err();
        assert!(matches!(err, DagError::InvalidStateTransition { .. }));
    }

    #[test]
    fn mark_success_from_none_errors() {
        let dag = make_linear_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());
        let err = run.mark_success(&TaskId::new("a")).unwrap_err();
        assert!(matches!(err, DagError::InvalidStateTransition { .. }));
    }

    // --- ready_tasks: Linear ---

    #[test]
    fn ready_tasks_linear_initial() {
        let dag = make_linear_dag();
        let run = DagRun::new(dag, "run_1", Utc::now());
        let ready = run.ready_tasks();
        assert_eq!(ready, vec![TaskId::new("a")]);
    }

    #[test]
    fn ready_tasks_linear_after_a_success() {
        let dag = make_linear_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_success(&TaskId::new("a")).unwrap();
        let ready = run.ready_tasks();
        assert_eq!(ready, vec![TaskId::new("b")]);
    }

    #[test]
    fn ready_tasks_linear_a_still_running() {
        let dag = make_linear_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.mark_running(&TaskId::new("a")).unwrap();
        let ready = run.ready_tasks();
        assert!(ready.is_empty());
    }

    // --- ready_tasks: Diamond ---

    #[test]
    fn ready_tasks_diamond_initial() {
        let dag = make_diamond_dag();
        let run = DagRun::new(dag, "run_1", Utc::now());
        let ready = run.ready_tasks();
        assert_eq!(ready, vec![TaskId::new("a")]);
    }

    #[test]
    fn ready_tasks_diamond_after_a_success() {
        let dag = make_diamond_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_success(&TaskId::new("a")).unwrap();
        let ready = run.ready_tasks();
        assert_eq!(ready, vec![TaskId::new("b"), TaskId::new("c")]);
    }

    #[test]
    fn ready_tasks_diamond_d_after_bc_success() {
        let dag = make_diamond_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_success(&TaskId::new("a")).unwrap();
        run.mark_running(&TaskId::new("b")).unwrap();
        run.mark_success(&TaskId::new("b")).unwrap();
        run.mark_running(&TaskId::new("c")).unwrap();
        run.mark_success(&TaskId::new("c")).unwrap();
        let ready = run.ready_tasks();
        assert_eq!(ready, vec![TaskId::new("d")]);
    }

    #[test]
    fn ready_tasks_diamond_d_not_ready_until_both() {
        let dag = make_diamond_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_success(&TaskId::new("a")).unwrap();
        run.mark_running(&TaskId::new("b")).unwrap();
        run.mark_success(&TaskId::new("b")).unwrap();
        // c still in None state
        let ready = run.ready_tasks();
        assert_eq!(ready, vec![TaskId::new("c")]); // only c, not d
    }

    // --- tick: Propagation ---

    #[test]
    fn tick_propagates_upstream_failed() {
        let dag = make_linear_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_failed(&TaskId::new("a")).unwrap();

        let ready = run.tick();
        assert!(ready.is_empty());
        assert_eq!(run.task_state(&TaskId::new("b")), TaskState::UpstreamFailed);
    }

    #[test]
    fn tick_cascades_upstream_failed() {
        let dag = make_linear_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_failed(&TaskId::new("a")).unwrap();

        run.tick(); // b -> UpstreamFailed
        run.tick(); // c -> UpstreamFailed (b is upstream_failed which counts as failure)
        assert_eq!(run.task_state(&TaskId::new("c")), TaskState::UpstreamFailed);
    }

    #[test]
    fn tick_returns_ready_tasks() {
        let dag = make_diamond_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());

        // First tick: root A is ready
        let ready = run.tick();
        assert_eq!(ready, vec![TaskId::new("a")]);

        // Execute A
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_success(&TaskId::new("a")).unwrap();

        // Second tick: B and C are ready
        let ready = run.tick();
        assert_eq!(ready, vec![TaskId::new("b"), TaskId::new("c")]);
    }

    // --- Retry Logic ---

    #[test]
    fn failed_task_with_retries_goes_to_up_for_retry() {
        let mut dag = Dag::new("test");
        dag.add_task(task_with_retries("a", 2)).unwrap();

        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_failed(&TaskId::new("a")).unwrap();

        assert_eq!(run.task_state(&TaskId::new("a")), TaskState::UpForRetry);
        assert_eq!(run.attempt_count(&TaskId::new("a")), 1);
    }

    #[test]
    fn up_for_retry_task_appears_in_ready_tasks() {
        let mut dag = Dag::new("test");
        dag.add_task(task_with_retries("a", 2)).unwrap();

        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_failed(&TaskId::new("a")).unwrap();

        let ready = run.ready_tasks();
        assert_eq!(ready, vec![TaskId::new("a")]);
    }

    #[test]
    fn retries_exhausted_stays_failed() {
        let mut dag = Dag::new("test");
        dag.add_task(task_with_retries("a", 1)).unwrap();

        let mut run = DagRun::new(dag, "run_1", Utc::now());

        // First attempt
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_failed(&TaskId::new("a")).unwrap();
        assert_eq!(run.task_state(&TaskId::new("a")), TaskState::UpForRetry);

        // Second attempt (retry)
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_failed(&TaskId::new("a")).unwrap();
        assert_eq!(run.task_state(&TaskId::new("a")), TaskState::Failed);
        assert_eq!(run.attempt_count(&TaskId::new("a")), 2);
    }

    #[test]
    fn mark_running_from_up_for_retry() {
        let mut dag = Dag::new("test");
        dag.add_task(task_with_retries("a", 2)).unwrap();

        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_failed(&TaskId::new("a")).unwrap();
        assert_eq!(run.task_state(&TaskId::new("a")), TaskState::UpForRetry);

        run.mark_running(&TaskId::new("a")).unwrap();
        assert_eq!(run.task_state(&TaskId::new("a")), TaskState::Running);
    }

    // --- Run Completion ---

    #[test]
    fn run_success_all_tasks_succeed() {
        let dag = make_linear_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());

        for id in &["a", "b", "c"] {
            run.mark_running(&TaskId::new(*id)).unwrap();
            run.mark_success(&TaskId::new(*id)).unwrap();
        }

        assert!(run.is_complete());
        assert_eq!(run.run_state(), DagRunState::Success);
    }

    #[test]
    fn run_failed_any_task_failed() {
        let dag = make_linear_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_failed(&TaskId::new("a")).unwrap();

        // Propagate failures
        run.tick(); // b -> UpstreamFailed
        run.tick(); // c -> UpstreamFailed

        assert!(run.is_complete());
        assert_eq!(run.run_state(), DagRunState::Failed);
    }

    #[test]
    fn not_complete_while_tasks_pending() {
        let dag = make_linear_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_success(&TaskId::new("a")).unwrap();

        assert!(!run.is_complete());
    }

    // --- XCom in DagRun ---

    #[test]
    fn xcom_push_and_pull_through_dag_run() {
        let dag = make_linear_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.xcom_push(&TaskId::new("a"), "output", json!({"rows": 100}));

        let val = run.xcom_pull(&TaskId::new("a"), "output");
        assert_eq!(val, Some(&json!({"rows": 100})));
    }

    #[test]
    fn xcom_cross_task_communication() {
        let dag = make_linear_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());

        // Task A pushes, Task B pulls
        run.xcom_push(&TaskId::new("a"), "return_value", json!("data_from_a"));
        let val = run.xcom_pull(&TaskId::new("a"), "return_value");
        assert_eq!(val, Some(&json!("data_from_a")));
    }

    // --- Trigger Rules Integration ---

    #[test]
    fn none_failed_trigger_rule_with_skip() {
        let mut dag = Dag::new("test");
        dag.add_task(simple_task("a")).unwrap();
        dag.add_task(simple_task("b")).unwrap();
        dag.add_task(task_with_rule("c", TriggerRule::NoneFailed)).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("c")).unwrap();
        dag.set_downstream(&TaskId::new("b"), &TaskId::new("c")).unwrap();

        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_success(&TaskId::new("a")).unwrap();
        run.mark_skipped(&TaskId::new("b")).unwrap();

        let ready = run.ready_tasks();
        assert_eq!(ready, vec![TaskId::new("c")]);
    }

    #[test]
    fn one_success_fires_early() {
        // Both A and B are upstream of C(OneSuccess).
        // A succeeds while B is still running — C should fire immediately.
        let mut dag = Dag::new("test");
        dag.add_task(simple_task("a")).unwrap();
        dag.add_task(simple_task("b")).unwrap();
        dag.add_task(task_with_rule("c", TriggerRule::OneSuccess)).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("c")).unwrap();
        dag.set_downstream(&TaskId::new("b"), &TaskId::new("c")).unwrap();

        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_success(&TaskId::new("a")).unwrap();
        run.mark_running(&TaskId::new("b")).unwrap();
        // b is Running, a succeeded — C should fire

        let ready = run.ready_tasks();
        assert_eq!(ready, vec![TaskId::new("c")]); // fires without waiting for b
    }

    #[test]
    fn always_trigger_rule_runs_despite_failure() {
        let mut dag = Dag::new("test");
        dag.add_task(simple_task("a")).unwrap();
        dag.add_task(task_with_rule("b", TriggerRule::Always)).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("b")).unwrap();

        let mut run = DagRun::new(dag, "run_1", Utc::now());
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_failed(&TaskId::new("a")).unwrap();

        let ready = run.ready_tasks();
        assert_eq!(ready, vec![TaskId::new("b")]);
    }

    // --- Full E2E: Diamond DAG ---

    #[test]
    fn e2e_diamond_dag_success() {
        let dag = make_diamond_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());

        // Tick 1: A ready
        let ready = run.tick();
        assert_eq!(ready, vec![TaskId::new("a")]);
        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_success(&TaskId::new("a")).unwrap();

        // Tick 2: B, C ready
        let ready = run.tick();
        assert_eq!(ready, vec![TaskId::new("b"), TaskId::new("c")]);
        run.mark_running(&TaskId::new("b")).unwrap();
        run.mark_success(&TaskId::new("b")).unwrap();
        run.mark_running(&TaskId::new("c")).unwrap();
        run.mark_success(&TaskId::new("c")).unwrap();

        // Tick 3: D ready
        let ready = run.tick();
        assert_eq!(ready, vec![TaskId::new("d")]);
        run.mark_running(&TaskId::new("d")).unwrap();
        run.mark_success(&TaskId::new("d")).unwrap();

        assert!(run.is_complete());
        assert_eq!(run.run_state(), DagRunState::Success);
    }

    #[test]
    fn e2e_diamond_dag_failure_propagation() {
        let dag = make_diamond_dag();
        let mut run = DagRun::new(dag, "run_1", Utc::now());

        run.mark_running(&TaskId::new("a")).unwrap();
        run.mark_success(&TaskId::new("a")).unwrap();

        // B fails
        run.mark_running(&TaskId::new("b")).unwrap();
        run.mark_failed(&TaskId::new("b")).unwrap();

        // C succeeds
        run.mark_running(&TaskId::new("c")).unwrap();
        run.mark_success(&TaskId::new("c")).unwrap();

        // Tick: D should be UpstreamFailed (AllSuccess default, b failed)
        let ready = run.tick();
        assert!(ready.is_empty());
        assert_eq!(run.task_state(&TaskId::new("d")), TaskState::UpstreamFailed);

        assert!(run.is_complete());
        assert_eq!(run.run_state(), DagRunState::Failed);
    }
}
