use crate::task_state::TaskState;

/// Trigger rule determining when a task should execute based on upstream states.
/// Trigger rule determining when a task should execute based on upstream states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TriggerRule {
    /// All upstream tasks succeeded (default).
    #[default]
    AllSuccess,
    /// All upstream tasks failed.
    AllFailed,
    /// All upstream tasks completed (any terminal state).
    AllDone,
    /// All upstream tasks done and at least one succeeded.
    AllDoneMinOneSuccess,
    /// All upstream tasks were skipped.
    AllSkipped,
    /// At least one upstream task succeeded (does not wait for all).
    OneSuccess,
    /// At least one upstream task failed (does not wait for all).
    OneFailed,
    /// At least one upstream task completed (does not wait for all).
    OneDone,
    /// No upstream tasks failed (all succeeded or were skipped).
    NoneFailed,
    /// No upstream tasks failed and at least one succeeded.
    NoneFailedMinOneSuccess,
    /// No upstream tasks were skipped.
    NoneSkipped,
    /// Always execute regardless of upstream states.
    Always,
}

/// Summary of upstream task states, precomputed for trigger rule evaluation.
#[derive(Debug, Clone, Default)]
pub struct UpstreamSummary {
    pub total: usize,
    pub success: usize,
    pub failed: usize,
    pub upstream_failed: usize,
    pub skipped: usize,
    pub removed: usize,
    pub done: usize,
    pub none_state: usize,
    pub running: usize,
    pub queued: usize,
    pub scheduled: usize,
    pub up_for_retry: usize,
}

impl UpstreamSummary {
    /// Build a summary from a slice of upstream task states.
    pub fn from_states(states: &[TaskState]) -> Self {
        let mut summary = Self {
            total: states.len(),
            ..Default::default()
        };
        for state in states {
            match state {
                TaskState::Success => {
                    summary.success += 1;
                    summary.done += 1;
                }
                TaskState::Failed => {
                    summary.failed += 1;
                    summary.done += 1;
                }
                TaskState::UpstreamFailed => {
                    summary.upstream_failed += 1;
                    summary.done += 1;
                }
                TaskState::Skipped => {
                    summary.skipped += 1;
                    summary.done += 1;
                }
                TaskState::Removed => {
                    summary.removed += 1;
                    summary.done += 1;
                }
                TaskState::None => summary.none_state += 1,
                TaskState::Running => summary.running += 1,
                TaskState::Queued => summary.queued += 1,
                TaskState::Scheduled => summary.scheduled += 1,
                TaskState::UpForRetry => summary.up_for_retry += 1,
            }
        }
        summary
    }
}

/// Result of evaluating a trigger rule against upstream states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerEvaluation {
    /// Task is ready to be scheduled.
    Ready,
    /// Task is not yet ready — still waiting on upstreams.
    Waiting,
    /// Task should be skipped.
    Skip,
    /// Task should be marked as `upstream_failed`.
    UpstreamFailed,
}

impl TriggerRule {
    /// Evaluate this trigger rule against the given upstream summary.
    #[allow(clippy::too_many_lines)]
    pub const fn evaluate(&self, summary: &UpstreamSummary) -> TriggerEvaluation {
        // No upstreams — vacuously satisfied for all rules.
        if summary.total == 0 {
            return TriggerEvaluation::Ready;
        }

        match self {
            Self::Always => TriggerEvaluation::Ready,

            Self::AllSuccess => {
                if summary.success == summary.total {
                    TriggerEvaluation::Ready
                } else if summary.failed > 0 || summary.upstream_failed > 0 {
                    TriggerEvaluation::UpstreamFailed
                } else if summary.done == summary.total {
                    // All done but not all success — must be skipped/removed
                    TriggerEvaluation::Skip
                } else {
                    TriggerEvaluation::Waiting
                }
            }

            Self::AllFailed => {
                let all_failed = summary.failed + summary.upstream_failed;
                if all_failed == summary.total {
                    TriggerEvaluation::Ready
                } else if summary.done == summary.total {
                    // All done but not all failed
                    TriggerEvaluation::Skip
                } else if summary.success > 0 || summary.skipped > 0 {
                    // Some already succeeded/skipped, can never be all-failed
                    TriggerEvaluation::Skip
                } else {
                    TriggerEvaluation::Waiting
                }
            }

            Self::AllDone => {
                if summary.done == summary.total {
                    TriggerEvaluation::Ready
                } else {
                    TriggerEvaluation::Waiting
                }
            }

            Self::AllDoneMinOneSuccess => {
                if summary.done == summary.total {
                    if summary.success >= 1 {
                        TriggerEvaluation::Ready
                    } else {
                        TriggerEvaluation::Skip
                    }
                } else {
                    TriggerEvaluation::Waiting
                }
            }

            Self::AllSkipped => {
                if summary.skipped == summary.total {
                    TriggerEvaluation::Ready
                } else if summary.done == summary.total
                    || summary.success > 0
                    || summary.failed > 0
                    || summary.upstream_failed > 0
                {
                    TriggerEvaluation::Skip
                } else {
                    TriggerEvaluation::Waiting
                }
            }

            Self::OneSuccess => {
                if summary.success >= 1 {
                    TriggerEvaluation::Ready
                } else if summary.done == summary.total {
                    // All done, none succeeded
                    TriggerEvaluation::UpstreamFailed
                } else {
                    TriggerEvaluation::Waiting
                }
            }

            Self::OneFailed => {
                if summary.failed >= 1 || summary.upstream_failed >= 1 {
                    TriggerEvaluation::Ready
                } else if summary.done == summary.total {
                    TriggerEvaluation::Skip
                } else {
                    TriggerEvaluation::Waiting
                }
            }

            Self::OneDone => {
                if summary.done >= 1 {
                    TriggerEvaluation::Ready
                } else {
                    TriggerEvaluation::Waiting
                }
            }

            Self::NoneFailed => {
                if summary.failed > 0 || summary.upstream_failed > 0 {
                    TriggerEvaluation::UpstreamFailed
                } else if summary.done == summary.total {
                    // All done, none failed (success + skipped + removed)
                    TriggerEvaluation::Ready
                } else {
                    TriggerEvaluation::Waiting
                }
            }

            Self::NoneFailedMinOneSuccess => {
                if summary.failed > 0 || summary.upstream_failed > 0 {
                    TriggerEvaluation::UpstreamFailed
                } else if summary.done == summary.total {
                    if summary.success >= 1 {
                        TriggerEvaluation::Ready
                    } else {
                        TriggerEvaluation::Skip
                    }
                } else {
                    TriggerEvaluation::Waiting
                }
            }

            Self::NoneSkipped => {
                if summary.skipped > 0 {
                    TriggerEvaluation::Skip
                } else if summary.done == summary.total {
                    TriggerEvaluation::Ready
                } else {
                    TriggerEvaluation::Waiting
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_all_success() {
        assert_eq!(TriggerRule::default(), TriggerRule::AllSuccess);
    }

    #[test]
    fn all_12_variants_exist() {
        let rules = [
            TriggerRule::AllSuccess,
            TriggerRule::AllFailed,
            TriggerRule::AllDone,
            TriggerRule::AllDoneMinOneSuccess,
            TriggerRule::AllSkipped,
            TriggerRule::OneSuccess,
            TriggerRule::OneFailed,
            TriggerRule::OneDone,
            TriggerRule::NoneFailed,
            TriggerRule::NoneFailedMinOneSuccess,
            TriggerRule::NoneSkipped,
            TriggerRule::Always,
        ];
        assert_eq!(rules.len(), 12);
    }

    // --- UpstreamSummary ---

    #[test]
    fn summary_default_all_zeros() {
        let s = UpstreamSummary::default();
        assert_eq!(s.total, 0);
        assert_eq!(s.success, 0);
        assert_eq!(s.done, 0);
    }

    #[test]
    fn summary_from_states() {
        let states = vec![
            TaskState::Success,
            TaskState::Failed,
            TaskState::Running,
            TaskState::Skipped,
        ];
        let s = UpstreamSummary::from_states(&states);
        assert_eq!(s.total, 4);
        assert_eq!(s.success, 1);
        assert_eq!(s.failed, 1);
        assert_eq!(s.running, 1);
        assert_eq!(s.skipped, 1);
        assert_eq!(s.done, 3); // success + failed + skipped
    }

    // --- AllSuccess ---

    #[test]
    fn all_success_no_upstreams() {
        let s = UpstreamSummary::from_states(&[]);
        assert_eq!(TriggerRule::AllSuccess.evaluate(&s), TriggerEvaluation::Ready);
    }

    #[test]
    fn all_success_all_succeeded() {
        let s = UpstreamSummary::from_states(&[TaskState::Success, TaskState::Success]);
        assert_eq!(TriggerRule::AllSuccess.evaluate(&s), TriggerEvaluation::Ready);
    }

    #[test]
    fn all_success_some_running() {
        let s = UpstreamSummary::from_states(&[TaskState::Success, TaskState::Running]);
        assert_eq!(TriggerRule::AllSuccess.evaluate(&s), TriggerEvaluation::Waiting);
    }

    #[test]
    fn all_success_any_failed() {
        let s = UpstreamSummary::from_states(&[TaskState::Success, TaskState::Failed]);
        assert_eq!(
            TriggerRule::AllSuccess.evaluate(&s),
            TriggerEvaluation::UpstreamFailed
        );
    }

    #[test]
    fn all_success_upstream_failed() {
        let s = UpstreamSummary::from_states(&[TaskState::Success, TaskState::UpstreamFailed]);
        assert_eq!(
            TriggerRule::AllSuccess.evaluate(&s),
            TriggerEvaluation::UpstreamFailed
        );
    }

    #[test]
    fn all_success_all_skipped() {
        let s = UpstreamSummary::from_states(&[TaskState::Skipped, TaskState::Skipped]);
        assert_eq!(TriggerRule::AllSuccess.evaluate(&s), TriggerEvaluation::Skip);
    }

    // --- AllFailed ---

    #[test]
    fn all_failed_all_failed() {
        let s = UpstreamSummary::from_states(&[TaskState::Failed, TaskState::UpstreamFailed]);
        assert_eq!(TriggerRule::AllFailed.evaluate(&s), TriggerEvaluation::Ready);
    }

    #[test]
    fn all_failed_some_succeeded() {
        let s = UpstreamSummary::from_states(&[TaskState::Failed, TaskState::Success]);
        assert_eq!(TriggerRule::AllFailed.evaluate(&s), TriggerEvaluation::Skip);
    }

    #[test]
    fn all_failed_still_running() {
        let s = UpstreamSummary::from_states(&[TaskState::Failed, TaskState::Running]);
        assert_eq!(TriggerRule::AllFailed.evaluate(&s), TriggerEvaluation::Waiting);
    }

    #[test]
    fn all_failed_no_upstreams() {
        let s = UpstreamSummary::from_states(&[]);
        assert_eq!(TriggerRule::AllFailed.evaluate(&s), TriggerEvaluation::Ready);
    }

    // --- AllDone ---

    #[test]
    fn all_done_mixed_terminal() {
        let s = UpstreamSummary::from_states(&[TaskState::Success, TaskState::Failed, TaskState::Skipped]);
        assert_eq!(TriggerRule::AllDone.evaluate(&s), TriggerEvaluation::Ready);
    }

    #[test]
    fn all_done_some_running() {
        let s = UpstreamSummary::from_states(&[TaskState::Success, TaskState::Running]);
        assert_eq!(TriggerRule::AllDone.evaluate(&s), TriggerEvaluation::Waiting);
    }

    #[test]
    fn all_done_no_upstreams() {
        let s = UpstreamSummary::from_states(&[]);
        assert_eq!(TriggerRule::AllDone.evaluate(&s), TriggerEvaluation::Ready);
    }

    // --- OneSuccess ---

    #[test]
    fn one_success_one_succeeded_others_running() {
        let s = UpstreamSummary::from_states(&[TaskState::Success, TaskState::Running]);
        assert_eq!(TriggerRule::OneSuccess.evaluate(&s), TriggerEvaluation::Ready);
    }

    #[test]
    fn one_success_all_done_none_succeeded() {
        let s = UpstreamSummary::from_states(&[TaskState::Failed, TaskState::Skipped]);
        assert_eq!(
            TriggerRule::OneSuccess.evaluate(&s),
            TriggerEvaluation::UpstreamFailed
        );
    }

    #[test]
    fn one_success_none_done() {
        let s = UpstreamSummary::from_states(&[TaskState::Running, TaskState::Queued]);
        assert_eq!(TriggerRule::OneSuccess.evaluate(&s), TriggerEvaluation::Waiting);
    }

    // --- OneFailed ---

    #[test]
    fn one_failed_one_failed_others_running() {
        let s = UpstreamSummary::from_states(&[TaskState::Failed, TaskState::Running]);
        assert_eq!(TriggerRule::OneFailed.evaluate(&s), TriggerEvaluation::Ready);
    }

    #[test]
    fn one_failed_upstream_failed_counts() {
        let s = UpstreamSummary::from_states(&[TaskState::UpstreamFailed, TaskState::Running]);
        assert_eq!(TriggerRule::OneFailed.evaluate(&s), TriggerEvaluation::Ready);
    }

    #[test]
    fn one_failed_all_done_none_failed() {
        let s = UpstreamSummary::from_states(&[TaskState::Success, TaskState::Skipped]);
        assert_eq!(TriggerRule::OneFailed.evaluate(&s), TriggerEvaluation::Skip);
    }

    #[test]
    fn one_failed_still_waiting() {
        let s = UpstreamSummary::from_states(&[TaskState::Running, TaskState::Running]);
        assert_eq!(TriggerRule::OneFailed.evaluate(&s), TriggerEvaluation::Waiting);
    }

    // --- OneDone ---

    #[test]
    fn one_done_one_finished() {
        let s = UpstreamSummary::from_states(&[TaskState::Success, TaskState::Running]);
        assert_eq!(TriggerRule::OneDone.evaluate(&s), TriggerEvaluation::Ready);
    }

    #[test]
    fn one_done_none_finished() {
        let s = UpstreamSummary::from_states(&[TaskState::Running, TaskState::Queued]);
        assert_eq!(TriggerRule::OneDone.evaluate(&s), TriggerEvaluation::Waiting);
    }

    // --- NoneFailed ---

    #[test]
    fn none_failed_all_success_or_skipped() {
        let s = UpstreamSummary::from_states(&[TaskState::Success, TaskState::Skipped]);
        assert_eq!(TriggerRule::NoneFailed.evaluate(&s), TriggerEvaluation::Ready);
    }

    #[test]
    fn none_failed_one_failed() {
        let s = UpstreamSummary::from_states(&[TaskState::Success, TaskState::Failed]);
        assert_eq!(
            TriggerRule::NoneFailed.evaluate(&s),
            TriggerEvaluation::UpstreamFailed
        );
    }

    #[test]
    fn none_failed_still_running() {
        let s = UpstreamSummary::from_states(&[TaskState::Success, TaskState::Running]);
        assert_eq!(TriggerRule::NoneFailed.evaluate(&s), TriggerEvaluation::Waiting);
    }

    // --- NoneFailedMinOneSuccess ---

    #[test]
    fn none_failed_min_one_success_ready() {
        let s = UpstreamSummary::from_states(&[TaskState::Success, TaskState::Skipped]);
        assert_eq!(
            TriggerRule::NoneFailedMinOneSuccess.evaluate(&s),
            TriggerEvaluation::Ready
        );
    }

    #[test]
    fn none_failed_min_one_success_all_skipped() {
        let s = UpstreamSummary::from_states(&[TaskState::Skipped, TaskState::Skipped]);
        assert_eq!(
            TriggerRule::NoneFailedMinOneSuccess.evaluate(&s),
            TriggerEvaluation::Skip
        );
    }

    #[test]
    fn none_failed_min_one_success_one_failed() {
        let s = UpstreamSummary::from_states(&[TaskState::Success, TaskState::Failed]);
        assert_eq!(
            TriggerRule::NoneFailedMinOneSuccess.evaluate(&s),
            TriggerEvaluation::UpstreamFailed
        );
    }

    // --- NoneSkipped ---

    #[test]
    fn none_skipped_all_success() {
        let s = UpstreamSummary::from_states(&[TaskState::Success, TaskState::Success]);
        assert_eq!(TriggerRule::NoneSkipped.evaluate(&s), TriggerEvaluation::Ready);
    }

    #[test]
    fn none_skipped_one_skipped() {
        let s = UpstreamSummary::from_states(&[TaskState::Success, TaskState::Skipped]);
        assert_eq!(TriggerRule::NoneSkipped.evaluate(&s), TriggerEvaluation::Skip);
    }

    #[test]
    fn none_skipped_still_running() {
        let s = UpstreamSummary::from_states(&[TaskState::Success, TaskState::Running]);
        assert_eq!(TriggerRule::NoneSkipped.evaluate(&s), TriggerEvaluation::Waiting);
    }

    // --- AllSkipped ---

    #[test]
    fn all_skipped_all_skipped() {
        let s = UpstreamSummary::from_states(&[TaskState::Skipped, TaskState::Skipped]);
        assert_eq!(TriggerRule::AllSkipped.evaluate(&s), TriggerEvaluation::Ready);
    }

    #[test]
    fn all_skipped_one_succeeded() {
        let s = UpstreamSummary::from_states(&[TaskState::Skipped, TaskState::Success]);
        assert_eq!(TriggerRule::AllSkipped.evaluate(&s), TriggerEvaluation::Skip);
    }

    #[test]
    fn all_skipped_still_running() {
        let s = UpstreamSummary::from_states(&[TaskState::Skipped, TaskState::Running]);
        assert_eq!(TriggerRule::AllSkipped.evaluate(&s), TriggerEvaluation::Waiting);
    }

    // --- AllDoneMinOneSuccess ---

    #[test]
    fn all_done_min_one_success_ready() {
        let s = UpstreamSummary::from_states(&[TaskState::Success, TaskState::Failed]);
        assert_eq!(
            TriggerRule::AllDoneMinOneSuccess.evaluate(&s),
            TriggerEvaluation::Ready
        );
    }

    #[test]
    fn all_done_min_one_success_no_success() {
        let s = UpstreamSummary::from_states(&[TaskState::Failed, TaskState::Skipped]);
        assert_eq!(
            TriggerRule::AllDoneMinOneSuccess.evaluate(&s),
            TriggerEvaluation::Skip
        );
    }

    #[test]
    fn all_done_min_one_success_still_running() {
        let s = UpstreamSummary::from_states(&[TaskState::Success, TaskState::Running]);
        assert_eq!(
            TriggerRule::AllDoneMinOneSuccess.evaluate(&s),
            TriggerEvaluation::Waiting
        );
    }

    // --- Always ---

    #[test]
    fn always_ready_regardless() {
        let s = UpstreamSummary::from_states(&[TaskState::Failed, TaskState::Running]);
        assert_eq!(TriggerRule::Always.evaluate(&s), TriggerEvaluation::Ready);
    }

    #[test]
    fn always_ready_no_upstreams() {
        let s = UpstreamSummary::from_states(&[]);
        assert_eq!(TriggerRule::Always.evaluate(&s), TriggerEvaluation::Ready);
    }
}
