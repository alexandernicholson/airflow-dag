use std::time::Duration;

use crate::task_id::{GroupId, TaskId};
use crate::trigger_rule::TriggerRule;

/// A task definition within a DAG.
#[derive(Debug, Clone)]
pub struct Task {
    pub task_id: TaskId,
    pub trigger_rule: TriggerRule,
    pub retries: u32,
    pub retry_delay: Duration,
    pub execution_timeout: Option<Duration>,
    pub priority_weight: i32,
    pub pool: Option<String>,
    pub group_id: Option<GroupId>,
}

/// Builder for constructing Task instances.
pub struct TaskBuilder {
    task_id: TaskId,
    trigger_rule: TriggerRule,
    retries: u32,
    retry_delay: Duration,
    execution_timeout: Option<Duration>,
    priority_weight: i32,
    pool: Option<String>,
    group_id: Option<GroupId>,
}

impl TaskBuilder {
    #[must_use]
    pub fn new(task_id: impl Into<String>) -> Self {
        Self {
            task_id: TaskId::new(task_id),
            trigger_rule: TriggerRule::default(),
            retries: 0,
            retry_delay: Duration::from_secs(300),
            execution_timeout: None,
            priority_weight: 0,
            pool: None,
            group_id: None,
        }
    }

    #[must_use]
    pub const fn trigger_rule(mut self, rule: TriggerRule) -> Self {
        self.trigger_rule = rule;
        self
    }

    #[must_use]
    pub const fn retries(mut self, n: u32) -> Self {
        self.retries = n;
        self
    }

    #[must_use]
    pub const fn retry_delay(mut self, d: Duration) -> Self {
        self.retry_delay = d;
        self
    }

    #[must_use]
    pub const fn execution_timeout(mut self, d: Duration) -> Self {
        self.execution_timeout = Some(d);
        self
    }

    #[must_use]
    pub const fn priority_weight(mut self, w: i32) -> Self {
        self.priority_weight = w;
        self
    }

    #[must_use]
    pub fn pool(mut self, p: impl Into<String>) -> Self {
        self.pool = Some(p.into());
        self
    }

    #[must_use]
    pub fn group_id(mut self, g: GroupId) -> Self {
        self.group_id = Some(g);
        self
    }

    #[must_use]
    pub fn build(self) -> Task {
        Task {
            task_id: self.task_id,
            trigger_rule: self.trigger_rule,
            retries: self.retries,
            retry_delay: self.retry_delay,
            execution_timeout: self.execution_timeout,
            priority_weight: self.priority_weight,
            pool: self.pool,
            group_id: self.group_id,
        }
    }
}

impl Task {
    #[must_use]
    pub fn builder(task_id: impl Into<String>) -> TaskBuilder {
        TaskBuilder::new(task_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_with_defaults() {
        let task = Task::builder("extract").build();
        assert_eq!(task.task_id, TaskId::new("extract"));
        assert_eq!(task.trigger_rule, TriggerRule::AllSuccess);
        assert_eq!(task.retries, 0);
        assert_eq!(task.retry_delay, Duration::from_secs(300));
        assert_eq!(task.execution_timeout, None);
        assert_eq!(task.priority_weight, 0);
        assert_eq!(task.pool, None);
        assert_eq!(task.group_id, None);
    }

    #[test]
    fn build_with_all_fields() {
        let task = Task::builder("transform")
            .trigger_rule(TriggerRule::NoneFailed)
            .retries(3)
            .retry_delay(Duration::from_secs(60))
            .execution_timeout(Duration::from_secs(3600))
            .priority_weight(10)
            .pool("heavy_compute")
            .group_id(GroupId::new("etl"))
            .build();

        assert_eq!(task.task_id, TaskId::new("transform"));
        assert_eq!(task.trigger_rule, TriggerRule::NoneFailed);
        assert_eq!(task.retries, 3);
        assert_eq!(task.retry_delay, Duration::from_secs(60));
        assert_eq!(task.execution_timeout, Some(Duration::from_secs(3600)));
        assert_eq!(task.priority_weight, 10);
        assert_eq!(task.pool, Some("heavy_compute".to_string()));
        assert_eq!(task.group_id, Some(GroupId::new("etl")));
    }

    #[test]
    fn task_is_clone() {
        let task = Task::builder("load").retries(2).build();
        let cloned = task.clone();
        assert_eq!(cloned.task_id, task.task_id);
        assert_eq!(cloned.retries, task.retries);
    }
}
