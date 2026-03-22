use crate::task_id::TaskId;
use crate::task_state::TaskState;

/// Errors that can occur when working with DAGs.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DagError {
    #[error("cycle detected in DAG")]
    CycleDetected,

    #[error("duplicate task id: {0}")]
    DuplicateTaskId(TaskId),

    #[error("task not found: {0}")]
    TaskNotFound(TaskId),

    #[error("task cannot depend on itself: {0}")]
    SelfDependency(TaskId),

    #[error("invalid state transition for task {task_id}: {from} -> {to}")]
    InvalidStateTransition {
        task_id: TaskId,
        from: TaskState,
        to: TaskState,
    },

    #[error("DAG run is already complete")]
    DagAlreadyComplete,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cycle_detected_display() {
        let err = DagError::CycleDetected;
        assert_eq!(format!("{}", err), "cycle detected in DAG");
    }

    #[test]
    fn duplicate_task_id_display() {
        let err = DagError::DuplicateTaskId(TaskId::new("task_a"));
        assert_eq!(format!("{}", err), "duplicate task id: task_a");
    }

    #[test]
    fn task_not_found_display() {
        let err = DagError::TaskNotFound(TaskId::new("missing"));
        assert_eq!(format!("{}", err), "task not found: missing");
    }

    #[test]
    fn self_dependency_display() {
        let err = DagError::SelfDependency(TaskId::new("task_a"));
        assert_eq!(
            format!("{}", err),
            "task cannot depend on itself: task_a"
        );
    }

    #[test]
    fn invalid_state_transition_display() {
        let err = DagError::InvalidStateTransition {
            task_id: TaskId::new("task_a"),
            from: TaskState::None,
            to: TaskState::Success,
        };
        assert_eq!(
            format!("{}", err),
            "invalid state transition for task task_a: none -> success"
        );
    }

    #[test]
    fn dag_already_complete_display() {
        let err = DagError::DagAlreadyComplete;
        assert_eq!(format!("{}", err), "DAG run is already complete");
    }

    #[test]
    fn error_is_std_error() {
        let err: Box<dyn std::error::Error> = Box::new(DagError::CycleDetected);
        assert_eq!(format!("{}", err), "cycle detected in DAG");
    }

    #[test]
    fn errors_are_eq() {
        assert_eq!(DagError::CycleDetected, DagError::CycleDetected);
        assert_ne!(
            DagError::CycleDetected,
            DagError::TaskNotFound(TaskId::new("x"))
        );
    }
}
