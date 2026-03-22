/// State of a task instance within a DAG run.
/// Mirrors Apache Airflow's task instance states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TaskState {
    /// No state yet — initial state before scheduling.
    #[default]
    None,
    /// Scheduler has marked it for execution.
    Scheduled,
    /// Placed in execution queue.
    Queued,
    /// Currently executing.
    Running,
    /// Completed successfully.
    Success,
    /// Execution failed.
    Failed,
    /// Skipped (e.g. branch not taken).
    Skipped,
    /// An upstream dependency failed.
    UpstreamFailed,
    /// Failed but will be retried.
    UpForRetry,
    /// Removed from the DAG run.
    Removed,
}

impl TaskState {
    /// Returns true if the task is in a terminal state.
    pub fn is_finished(&self) -> bool {
        matches!(
            self,
            TaskState::Success
                | TaskState::Failed
                | TaskState::Skipped
                | TaskState::UpstreamFailed
                | TaskState::Removed
        )
    }

    /// Returns true if the task completed successfully.
    pub fn is_success(&self) -> bool {
        matches!(self, TaskState::Success)
    }

    /// Returns true if the task is in a failed state.
    pub fn is_failure(&self) -> bool {
        matches!(self, TaskState::Failed | TaskState::UpstreamFailed)
    }
}

impl std::fmt::Display for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskState::None => write!(f, "none"),
            TaskState::Scheduled => write!(f, "scheduled"),
            TaskState::Queued => write!(f, "queued"),
            TaskState::Running => write!(f, "running"),
            TaskState::Success => write!(f, "success"),
            TaskState::Failed => write!(f, "failed"),
            TaskState::Skipped => write!(f, "skipped"),
            TaskState::UpstreamFailed => write!(f, "upstream_failed"),
            TaskState::UpForRetry => write!(f, "up_for_retry"),
            TaskState::Removed => write!(f, "removed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_none() {
        assert_eq!(TaskState::default(), TaskState::None);
    }

    #[test]
    fn is_finished_terminal_states() {
        assert!(TaskState::Success.is_finished());
        assert!(TaskState::Failed.is_finished());
        assert!(TaskState::Skipped.is_finished());
        assert!(TaskState::UpstreamFailed.is_finished());
        assert!(TaskState::Removed.is_finished());
    }

    #[test]
    fn is_finished_non_terminal_states() {
        assert!(!TaskState::None.is_finished());
        assert!(!TaskState::Scheduled.is_finished());
        assert!(!TaskState::Queued.is_finished());
        assert!(!TaskState::Running.is_finished());
        assert!(!TaskState::UpForRetry.is_finished());
    }

    #[test]
    fn is_success() {
        assert!(TaskState::Success.is_success());
        assert!(!TaskState::Failed.is_success());
        assert!(!TaskState::None.is_success());
        assert!(!TaskState::Skipped.is_success());
    }

    #[test]
    fn is_failure() {
        assert!(TaskState::Failed.is_failure());
        assert!(TaskState::UpstreamFailed.is_failure());
        assert!(!TaskState::Success.is_failure());
        assert!(!TaskState::None.is_failure());
        assert!(!TaskState::Skipped.is_failure());
    }

    #[test]
    fn display_values() {
        assert_eq!(format!("{}", TaskState::None), "none");
        assert_eq!(format!("{}", TaskState::Scheduled), "scheduled");
        assert_eq!(format!("{}", TaskState::Queued), "queued");
        assert_eq!(format!("{}", TaskState::Running), "running");
        assert_eq!(format!("{}", TaskState::Success), "success");
        assert_eq!(format!("{}", TaskState::Failed), "failed");
        assert_eq!(format!("{}", TaskState::Skipped), "skipped");
        assert_eq!(format!("{}", TaskState::UpstreamFailed), "upstream_failed");
        assert_eq!(format!("{}", TaskState::UpForRetry), "up_for_retry");
        assert_eq!(format!("{}", TaskState::Removed), "removed");
    }

    #[test]
    fn copy_semantics() {
        let a = TaskState::Running;
        let b = a;
        assert_eq!(a, b);
    }
}
