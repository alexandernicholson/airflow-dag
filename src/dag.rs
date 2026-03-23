use std::collections::{HashMap, HashSet, VecDeque};

use crate::error::DagError;
use crate::task::Task;
use crate::task_id::TaskId;

/// A directed acyclic graph of tasks with dependency tracking.
/// Airflow-compatible DAG with dependency tracking.
#[derive(Debug, Clone)]
pub struct Dag {
    pub dag_id: String,
    tasks: HashMap<TaskId, Task>,
    /// downstream[A] = {B, C} means A >> B, A >> C
    downstream: HashMap<TaskId, HashSet<TaskId>>,
    /// upstream[B] = {A} means A >> B
    upstream: HashMap<TaskId, HashSet<TaskId>>,
}

impl Dag {
    pub fn new(dag_id: impl Into<String>) -> Self {
        Self {
            dag_id: dag_id.into(),
            tasks: HashMap::new(),
            downstream: HashMap::new(),
            upstream: HashMap::new(),
        }
    }

    /// Add a task to the DAG. Returns error if `task_id` already exists.
    pub fn add_task(&mut self, task: Task) -> Result<(), DagError> {
        if self.tasks.contains_key(&task.task_id) {
            return Err(DagError::DuplicateTaskId(task.task_id));
        }
        let id = task.task_id.clone();
        self.tasks.insert(id.clone(), task);
        self.downstream.entry(id.clone()).or_default();
        self.upstream.entry(id).or_default();
        Ok(())
    }

    /// Get a reference to a task by its ID.
    pub fn get_task(&self, id: &TaskId) -> Option<&Task> {
        self.tasks.get(id)
    }

    /// Iterate over all task IDs in the DAG.
    pub fn task_ids(&self) -> impl Iterator<Item = &TaskId> {
        self.tasks.keys()
    }

    /// Number of tasks in the DAG.
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    /// Set `from` as upstream of `to` (from >> to).
    pub fn set_downstream(&mut self, from: &TaskId, to: &TaskId) -> Result<(), DagError> {
        if from == to {
            return Err(DagError::SelfDependency(from.clone()));
        }
        if !self.tasks.contains_key(from) {
            return Err(DagError::TaskNotFound(from.clone()));
        }
        if !self.tasks.contains_key(to) {
            return Err(DagError::TaskNotFound(to.clone()));
        }
        self.downstream
            .entry(from.clone())
            .or_default()
            .insert(to.clone());
        self.upstream
            .entry(to.clone())
            .or_default()
            .insert(from.clone());
        Ok(())
    }

    /// Set `upstream_task` as upstream of `task` (`upstream_task` >> task).
    pub fn set_upstream(&mut self, task: &TaskId, upstream_task: &TaskId) -> Result<(), DagError> {
        self.set_downstream(upstream_task, task)
    }

    /// Create a sequential chain: tasks[0] >> tasks[1] >> ... >> tasks[n].
    pub fn chain(&mut self, tasks: &[TaskId]) -> Result<(), DagError> {
        for pair in tasks.windows(2) {
            self.set_downstream(&pair[0], &pair[1])?;
        }
        Ok(())
    }

    /// Get downstream task IDs for a given task.
    pub fn downstream_of(&self, id: &TaskId) -> HashSet<TaskId> {
        self.downstream
            .get(id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get upstream task IDs for a given task.
    pub fn upstream_of(&self, id: &TaskId) -> HashSet<TaskId> {
        self.upstream
            .get(id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get root tasks (no upstream dependencies).
    pub fn roots(&self) -> Vec<TaskId> {
        self.tasks
            .keys()
            .filter(|id| self.upstream.get(*id).is_none_or(HashSet::is_empty))
            .cloned()
            .collect()
    }

    /// Get leaf tasks (no downstream dependencies).
    pub fn leaves(&self) -> Vec<TaskId> {
        self.tasks
            .keys()
            .filter(|id| self.downstream.get(*id).is_none_or(HashSet::is_empty))
            .cloned()
            .collect()
    }

    /// Topological sort using Kahn's algorithm.
    /// Returns an error if the graph contains a cycle.
    pub fn topological_sort(&self) -> Result<Vec<TaskId>, DagError> {
        let mut in_degree: HashMap<TaskId, usize> = HashMap::new();
        for id in self.tasks.keys() {
            in_degree.insert(
                id.clone(),
                self.upstream.get(id).map_or(0, HashSet::len),
            );
        }

        let mut queue: VecDeque<TaskId> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(id, _)| id.clone())
            .collect();

        // Sort for deterministic output when multiple roots
        let mut queue_vec: Vec<TaskId> = queue.into_iter().collect();
        queue_vec.sort();
        queue = queue_vec.into();

        let mut result = Vec::with_capacity(self.tasks.len());

        while let Some(node) = queue.pop_front() {
            result.push(node.clone());
            if let Some(downstream) = self.downstream.get(&node) {
                let mut next: Vec<&TaskId> = downstream.iter().collect();
                next.sort(); // deterministic ordering
                for next_id in next {
                    if let Some(deg) = in_degree.get_mut(next_id) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(next_id.clone());
                        }
                    }
                }
            }
        }

        if result.len() == self.tasks.len() {
            Ok(result)
        } else {
            Err(DagError::CycleDetected)
        }
    }

    /// Validate the DAG (checks for cycles and structural integrity).
    pub fn validate(&self) -> Result<(), DagError> {
        self.topological_sort()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::Task;

    fn task(id: &str) -> Task {
        Task::builder(id).build()
    }

    // --- Dag::new and add_task ---

    #[test]
    fn new_dag_is_empty() {
        let dag = Dag::new("my_dag");
        assert_eq!(dag.dag_id, "my_dag");
        assert_eq!(dag.task_count(), 0);
    }

    #[test]
    fn add_task_and_retrieve() {
        let mut dag = Dag::new("test");
        dag.add_task(task("extract")).unwrap();
        assert!(dag.get_task(&TaskId::new("extract")).is_some());
        assert_eq!(dag.task_count(), 1);
    }

    #[test]
    fn add_task_duplicate_errors() {
        let mut dag = Dag::new("test");
        dag.add_task(task("extract")).unwrap();
        let err = dag.add_task(task("extract")).unwrap_err();
        assert_eq!(err, DagError::DuplicateTaskId(TaskId::new("extract")));
    }

    #[test]
    fn task_ids_returns_all() {
        let mut dag = Dag::new("test");
        dag.add_task(task("a")).unwrap();
        dag.add_task(task("b")).unwrap();
        let ids: HashSet<&TaskId> = dag.task_ids().collect();
        assert!(ids.contains(&TaskId::new("a")));
        assert!(ids.contains(&TaskId::new("b")));
        assert_eq!(ids.len(), 2);
    }

    // --- Dependencies ---

    #[test]
    fn set_downstream_creates_relationship() {
        let mut dag = Dag::new("test");
        dag.add_task(task("a")).unwrap();
        dag.add_task(task("b")).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("b"))
            .unwrap();

        assert!(dag
            .downstream_of(&TaskId::new("a"))
            .contains(&TaskId::new("b")));
        assert!(dag
            .upstream_of(&TaskId::new("b"))
            .contains(&TaskId::new("a")));
    }

    #[test]
    fn set_upstream_creates_relationship() {
        let mut dag = Dag::new("test");
        dag.add_task(task("a")).unwrap();
        dag.add_task(task("b")).unwrap();
        dag.set_upstream(&TaskId::new("b"), &TaskId::new("a"))
            .unwrap();

        assert!(dag
            .downstream_of(&TaskId::new("a"))
            .contains(&TaskId::new("b")));
        assert!(dag
            .upstream_of(&TaskId::new("b"))
            .contains(&TaskId::new("a")));
    }

    #[test]
    fn self_dependency_errors() {
        let mut dag = Dag::new("test");
        dag.add_task(task("a")).unwrap();
        let err = dag
            .set_downstream(&TaskId::new("a"), &TaskId::new("a"))
            .unwrap_err();
        assert_eq!(err, DagError::SelfDependency(TaskId::new("a")));
    }

    #[test]
    fn dependency_on_missing_task_errors() {
        let mut dag = Dag::new("test");
        dag.add_task(task("a")).unwrap();
        let err = dag
            .set_downstream(&TaskId::new("a"), &TaskId::new("missing"))
            .unwrap_err();
        assert_eq!(err, DagError::TaskNotFound(TaskId::new("missing")));
    }

    #[test]
    fn dependency_from_missing_task_errors() {
        let mut dag = Dag::new("test");
        dag.add_task(task("a")).unwrap();
        let err = dag
            .set_downstream(&TaskId::new("missing"), &TaskId::new("a"))
            .unwrap_err();
        assert_eq!(err, DagError::TaskNotFound(TaskId::new("missing")));
    }

    #[test]
    fn roots_returns_tasks_with_no_upstream() {
        let mut dag = Dag::new("test");
        dag.add_task(task("a")).unwrap();
        dag.add_task(task("b")).unwrap();
        dag.add_task(task("c")).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("b"))
            .unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("c"))
            .unwrap();

        let mut roots = dag.roots();
        roots.sort();
        assert_eq!(roots, vec![TaskId::new("a")]);
    }

    #[test]
    fn leaves_returns_tasks_with_no_downstream() {
        let mut dag = Dag::new("test");
        dag.add_task(task("a")).unwrap();
        dag.add_task(task("b")).unwrap();
        dag.add_task(task("c")).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("b"))
            .unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("c"))
            .unwrap();

        let mut leaves = dag.leaves();
        leaves.sort();
        assert_eq!(leaves, vec![TaskId::new("b"), TaskId::new("c")]);
    }

    // --- Chain ---

    #[test]
    fn chain_creates_sequential_deps() {
        let mut dag = Dag::new("test");
        dag.add_task(task("a")).unwrap();
        dag.add_task(task("b")).unwrap();
        dag.add_task(task("c")).unwrap();

        dag.chain(&[TaskId::new("a"), TaskId::new("b"), TaskId::new("c")])
            .unwrap();

        assert!(dag
            .downstream_of(&TaskId::new("a"))
            .contains(&TaskId::new("b")));
        assert!(dag
            .downstream_of(&TaskId::new("b"))
            .contains(&TaskId::new("c")));
        assert!(dag.downstream_of(&TaskId::new("c")).is_empty());
    }

    #[test]
    fn chain_single_element_is_noop() {
        let mut dag = Dag::new("test");
        dag.add_task(task("a")).unwrap();
        dag.chain(&[TaskId::new("a")]).unwrap();
        assert!(dag.downstream_of(&TaskId::new("a")).is_empty());
    }

    #[test]
    fn chain_empty_is_noop() {
        let mut dag = Dag::new("test");
        dag.chain(&[]).unwrap();
    }

    // --- Topological Sort ---

    #[test]
    fn topo_sort_linear_chain() {
        let mut dag = Dag::new("test");
        dag.add_task(task("a")).unwrap();
        dag.add_task(task("b")).unwrap();
        dag.add_task(task("c")).unwrap();
        dag.chain(&[TaskId::new("a"), TaskId::new("b"), TaskId::new("c")])
            .unwrap();

        let sorted = dag.topological_sort().unwrap();
        assert_eq!(
            sorted,
            vec![TaskId::new("a"), TaskId::new("b"), TaskId::new("c")]
        );
    }

    #[test]
    fn topo_sort_diamond() {
        let mut dag = Dag::new("test");
        dag.add_task(task("a")).unwrap();
        dag.add_task(task("b")).unwrap();
        dag.add_task(task("c")).unwrap();
        dag.add_task(task("d")).unwrap();
        // A >> {B, C} >> D
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("b"))
            .unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("c"))
            .unwrap();
        dag.set_downstream(&TaskId::new("b"), &TaskId::new("d"))
            .unwrap();
        dag.set_downstream(&TaskId::new("c"), &TaskId::new("d"))
            .unwrap();

        let sorted = dag.topological_sort().unwrap();
        assert_eq!(sorted[0], TaskId::new("a"));
        assert_eq!(sorted[3], TaskId::new("d"));
        // b and c are in positions 1 and 2 (order is deterministic due to sorting)
        assert!(sorted[1..3].contains(&TaskId::new("b")));
        assert!(sorted[1..3].contains(&TaskId::new("c")));
    }

    #[test]
    fn topo_sort_independent_tasks() {
        let mut dag = Dag::new("test");
        dag.add_task(task("a")).unwrap();
        dag.add_task(task("b")).unwrap();
        let sorted = dag.topological_sort().unwrap();
        assert_eq!(sorted.len(), 2);
        // Both appear, alphabetical due to deterministic sort
        assert_eq!(sorted, vec![TaskId::new("a"), TaskId::new("b")]);
    }

    #[test]
    fn topo_sort_cycle_detected() {
        let mut dag = Dag::new("test");
        dag.add_task(task("a")).unwrap();
        dag.add_task(task("b")).unwrap();
        dag.add_task(task("c")).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("b"))
            .unwrap();
        dag.set_downstream(&TaskId::new("b"), &TaskId::new("c"))
            .unwrap();
        dag.set_downstream(&TaskId::new("c"), &TaskId::new("a"))
            .unwrap();

        let err = dag.topological_sort().unwrap_err();
        assert_eq!(err, DagError::CycleDetected);
    }

    #[test]
    fn topo_sort_two_node_cycle() {
        let mut dag = Dag::new("test");
        dag.add_task(task("a")).unwrap();
        dag.add_task(task("b")).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("b"))
            .unwrap();
        dag.set_downstream(&TaskId::new("b"), &TaskId::new("a"))
            .unwrap();

        let err = dag.topological_sort().unwrap_err();
        assert_eq!(err, DagError::CycleDetected);
    }

    #[test]
    fn topo_sort_complex_graph() {
        // Graph:  A -> B -> D -> F
        //         A -> C -> D
        //         C -> E -> F
        let mut dag = Dag::new("test");
        for id in &["a", "b", "c", "d", "e", "f"] {
            dag.add_task(task(id)).unwrap();
        }
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("b")).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("c")).unwrap();
        dag.set_downstream(&TaskId::new("b"), &TaskId::new("d")).unwrap();
        dag.set_downstream(&TaskId::new("c"), &TaskId::new("d")).unwrap();
        dag.set_downstream(&TaskId::new("c"), &TaskId::new("e")).unwrap();
        dag.set_downstream(&TaskId::new("d"), &TaskId::new("f")).unwrap();
        dag.set_downstream(&TaskId::new("e"), &TaskId::new("f")).unwrap();

        let sorted = dag.topological_sort().unwrap();
        assert_eq!(sorted.len(), 6);
        assert_eq!(sorted[0], TaskId::new("a"));
        assert_eq!(sorted[5], TaskId::new("f"));

        // Verify ordering invariant: every task appears after all its upstreams
        let pos: HashMap<&TaskId, usize> = sorted.iter().enumerate().map(|(i, id)| (id, i)).collect();
        for (id, upstreams) in &dag.upstream {
            for up in upstreams {
                assert!(
                    pos[up] < pos[id],
                    "{up} should appear before {id}"
                );
            }
        }
    }

    #[test]
    fn validate_acyclic_ok() {
        let mut dag = Dag::new("test");
        dag.add_task(task("a")).unwrap();
        dag.add_task(task("b")).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("b"))
            .unwrap();
        dag.validate().unwrap();
    }

    #[test]
    fn validate_cyclic_errors() {
        let mut dag = Dag::new("test");
        dag.add_task(task("a")).unwrap();
        dag.add_task(task("b")).unwrap();
        dag.set_downstream(&TaskId::new("a"), &TaskId::new("b"))
            .unwrap();
        dag.set_downstream(&TaskId::new("b"), &TaskId::new("a"))
            .unwrap();
        assert_eq!(dag.validate().unwrap_err(), DagError::CycleDetected);
    }
}
