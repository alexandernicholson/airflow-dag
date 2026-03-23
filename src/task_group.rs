use std::time::Duration;

use crate::dag::Dag;
use crate::error::DagError;
use crate::task::Task;
use crate::task_id::{GroupId, TaskId};
use crate::trigger_rule::TriggerRule;

/// Default arguments that can be applied to tasks within a group.
#[derive(Debug, Clone, Default)]
pub struct TaskDefaults {
    pub retries: Option<u32>,
    pub retry_delay: Option<Duration>,
    pub trigger_rule: Option<TriggerRule>,
    pub pool: Option<String>,
}

/// A hierarchical grouping of tasks within a DAG.
/// A hierarchical grouping of tasks within a DAG.
#[derive(Debug, Clone)]
pub struct TaskGroup {
    pub group_id: GroupId,
    pub prefix_group_id: bool,
    pub tasks: Vec<Task>,
    pub children: Vec<Self>,
    pub defaults: TaskDefaults,
}

impl TaskGroup {
    #[must_use]
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: GroupId::new(group_id),
            prefix_group_id: true,
            tasks: Vec::new(),
            children: Vec::new(),
            defaults: TaskDefaults::default(),
        }
    }

    #[must_use]
    pub const fn with_prefix(mut self, prefix: bool) -> Self {
        self.prefix_group_id = prefix;
        self
    }

    #[must_use]
    pub fn with_defaults(mut self, defaults: TaskDefaults) -> Self {
        self.defaults = defaults;
        self
    }

    #[must_use]
    pub fn add_task(mut self, task: Task) -> Self {
        self.tasks.push(task);
        self
    }

    #[must_use]
    pub fn add_child_group(mut self, group: Self) -> Self {
        self.children.push(group);
        self
    }

    /// Compute the qualified task ID by prepending group prefix(es).
    #[must_use]
    pub fn qualified_task_id(&self, local_id: &str) -> TaskId {
        if self.prefix_group_id {
            TaskId::new(format!("{}.{}", self.group_id.0, local_id))
        } else {
            TaskId::new(local_id)
        }
    }

    /// Get all task IDs in this group and nested children (fully qualified).
    #[must_use]
    pub fn all_task_ids(&self) -> Vec<TaskId> {
        self.all_task_ids_with_prefix("")
    }

    fn all_task_ids_with_prefix(&self, parent_prefix: &str) -> Vec<TaskId> {
        let my_prefix = if self.prefix_group_id {
            if parent_prefix.is_empty() {
                self.group_id.0.clone()
            } else {
                format!("{}.{}", parent_prefix, self.group_id.0)
            }
        } else {
            parent_prefix.to_string()
        };

        let mut ids = Vec::new();

        for task in &self.tasks {
            if my_prefix.is_empty() {
                ids.push(task.task_id.clone());
            } else {
                ids.push(TaskId::new(format!("{}.{}", my_prefix, task.task_id.0)));
            }
        }

        for child in &self.children {
            ids.extend(child.all_task_ids_with_prefix(&my_prefix));
        }

        ids
    }

    /// Register all tasks in this group (and children) into the DAG.
    /// Tasks get qualified IDs and defaults applied.
    ///
    /// # Errors
    /// Returns `DagError::DuplicateTaskId` if a qualified task ID already exists in the DAG.
    pub fn add_to_dag(&self, dag: &mut Dag) -> Result<Vec<TaskId>, DagError> {
        self.add_to_dag_with_prefix(dag, "")
    }

    fn add_to_dag_with_prefix(
        &self,
        dag: &mut Dag,
        parent_prefix: &str,
    ) -> Result<Vec<TaskId>, DagError> {
        let my_prefix = if self.prefix_group_id {
            if parent_prefix.is_empty() {
                self.group_id.0.clone()
            } else {
                format!("{}.{}", parent_prefix, self.group_id.0)
            }
        } else {
            parent_prefix.to_string()
        };

        let mut registered_ids = Vec::new();

        for task in &self.tasks {
            let qualified_id = if my_prefix.is_empty() {
                task.task_id.clone()
            } else {
                TaskId::new(format!("{}.{}", my_prefix, task.task_id.0))
            };

            let mut builder = Task::builder(qualified_id.0.clone())
                .trigger_rule(
                    self.defaults
                        .trigger_rule
                        .unwrap_or(task.trigger_rule),
                )
                .retries(self.defaults.retries.unwrap_or(task.retries))
                .retry_delay(self.defaults.retry_delay.unwrap_or(task.retry_delay))
                .priority_weight(task.priority_weight)
                .group_id(GroupId::new(my_prefix.clone()));

            if let Some(pool) = self.defaults.pool.as_ref().or(task.pool.as_ref()) {
                builder = builder.pool(pool.clone());
            }
            if let Some(timeout) = task.execution_timeout {
                builder = builder.execution_timeout(timeout);
            }

            dag.add_task(builder.build())?;
            registered_ids.push(qualified_id);
        }

        for child in &self.children {
            let child_ids = child.add_to_dag_with_prefix(dag, &my_prefix)?;
            registered_ids.extend(child_ids);
        }

        Ok(registered_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn simple_task(id: &str) -> Task {
        Task::builder(id).build()
    }

    // --- Qualified Task IDs ---

    #[test]
    fn qualified_id_with_prefix() {
        let group = TaskGroup::new("etl");
        let id = group.qualified_task_id("extract");
        assert_eq!(id, TaskId::new("etl.extract"));
    }

    #[test]
    fn qualified_id_without_prefix() {
        let group = TaskGroup::new("etl").with_prefix(false);
        let id = group.qualified_task_id("extract");
        assert_eq!(id, TaskId::new("extract"));
    }

    // --- All Task IDs ---

    #[test]
    fn all_task_ids_flat_group() {
        let group = TaskGroup::new("etl")
            .add_task(simple_task("extract"))
            .add_task(simple_task("transform"))
            .add_task(simple_task("load"));

        let mut ids = group.all_task_ids();
        ids.sort();
        assert_eq!(
            ids,
            vec![
                TaskId::new("etl.extract"),
                TaskId::new("etl.load"),
                TaskId::new("etl.transform"),
            ]
        );
    }

    #[test]
    fn all_task_ids_nested_groups() {
        let inner = TaskGroup::new("inner").add_task(simple_task("task_c"));
        let outer = TaskGroup::new("outer")
            .add_task(simple_task("task_a"))
            .add_task(simple_task("task_b"))
            .add_child_group(inner);

        let mut ids = outer.all_task_ids();
        ids.sort();
        assert_eq!(
            ids,
            vec![
                TaskId::new("outer.inner.task_c"),
                TaskId::new("outer.task_a"),
                TaskId::new("outer.task_b"),
            ]
        );
    }

    #[test]
    fn all_task_ids_no_prefix() {
        let group = TaskGroup::new("etl")
            .with_prefix(false)
            .add_task(simple_task("extract"));

        let ids = group.all_task_ids();
        assert_eq!(ids, vec![TaskId::new("extract")]);
    }

    // --- Add to DAG ---

    #[test]
    fn add_to_dag_registers_tasks() {
        let group = TaskGroup::new("etl")
            .add_task(simple_task("extract"))
            .add_task(simple_task("load"));

        let mut dag = Dag::new("test");
        let ids = group.add_to_dag(&mut dag).unwrap();

        assert_eq!(ids.len(), 2);
        assert!(dag.get_task(&TaskId::new("etl.extract")).is_some());
        assert!(dag.get_task(&TaskId::new("etl.load")).is_some());
    }

    #[test]
    fn add_to_dag_applies_defaults() {
        let defaults = TaskDefaults {
            retries: Some(3),
            retry_delay: Some(Duration::from_secs(60)),
            ..Default::default()
        };
        let group = TaskGroup::new("etl")
            .with_defaults(defaults)
            .add_task(simple_task("extract"));

        let mut dag = Dag::new("test");
        group.add_to_dag(&mut dag).unwrap();

        let task = dag.get_task(&TaskId::new("etl.extract")).unwrap();
        assert_eq!(task.retries, 3);
        assert_eq!(task.retry_delay, Duration::from_secs(60));
    }

    #[test]
    fn add_to_dag_nested_groups() {
        let inner = TaskGroup::new("inner").add_task(simple_task("deep_task"));
        let outer = TaskGroup::new("outer")
            .add_task(simple_task("shallow_task"))
            .add_child_group(inner);

        let mut dag = Dag::new("test");
        outer.add_to_dag(&mut dag).unwrap();

        assert!(dag.get_task(&TaskId::new("outer.shallow_task")).is_some());
        assert!(dag
            .get_task(&TaskId::new("outer.inner.deep_task"))
            .is_some());
    }

    #[test]
    fn add_to_dag_duplicate_errors() {
        let group = TaskGroup::new("etl").add_task(simple_task("extract"));

        let mut dag = Dag::new("test");
        dag.add_task(Task::builder("etl.extract").build()).unwrap();

        let err = group.add_to_dag(&mut dag).unwrap_err();
        assert!(matches!(err, DagError::DuplicateTaskId(_)));
    }

    #[test]
    fn add_to_dag_sets_group_id_on_tasks() {
        let group = TaskGroup::new("etl").add_task(simple_task("extract"));

        let mut dag = Dag::new("test");
        group.add_to_dag(&mut dag).unwrap();

        let task = dag.get_task(&TaskId::new("etl.extract")).unwrap();
        assert_eq!(task.group_id, Some(GroupId::new("etl")));
    }

    #[test]
    fn group_dependencies_between_registered_tasks() {
        let group = TaskGroup::new("etl")
            .add_task(simple_task("extract"))
            .add_task(simple_task("load"));

        let mut dag = Dag::new("test");
        let ids = group.add_to_dag(&mut dag).unwrap();

        // Can set dependencies after registration
        dag.set_downstream(&ids[0], &ids[1]).unwrap();
        assert!(dag
            .downstream_of(&TaskId::new("etl.extract"))
            .contains(&TaskId::new("etl.load")));
    }
}
