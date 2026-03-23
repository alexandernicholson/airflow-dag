use std::collections::HashMap;

use crate::task_id::TaskId;

/// Cross-communication store for passing data between tasks.
/// Cross-task communication store for passing data between tasks.
/// Nested map: `task_id` -> key -> value.
#[derive(Debug, Clone, Default)]
pub struct XComStore {
    store: HashMap<TaskId, HashMap<String, serde_json::Value>>,
}

impl XComStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Push a value for a task with a given key.
    pub fn push(&mut self, task_id: &TaskId, key: &str, value: serde_json::Value) {
        self.store
            .entry(task_id.clone())
            .or_default()
            .insert(key.to_string(), value);
    }

    /// Pull a value for a task by key.
    #[must_use]
    pub fn pull(&self, task_id: &TaskId, key: &str) -> Option<&serde_json::Value> {
        self.store.get(task_id)?.get(key)
    }

    /// Pull all key-value pairs for a task.
    #[must_use]
    pub fn pull_all(&self, task_id: &TaskId) -> Option<&HashMap<String, serde_json::Value>> {
        self.store.get(task_id)
    }

    /// Clear all `XCom` data for a task.
    pub fn clear_task(&mut self, task_id: &TaskId) {
        self.store.remove(task_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn push_and_pull() {
        let mut store = XComStore::new();
        let task = TaskId::new("extract");
        store.push(&task, "row_count", json!(42));

        assert_eq!(store.pull(&task, "row_count"), Some(&json!(42)));
    }

    #[test]
    fn pull_nonexistent_key_returns_none() {
        let store = XComStore::new();
        assert_eq!(store.pull(&TaskId::new("x"), "missing"), None);
    }

    #[test]
    fn pull_nonexistent_task_returns_none() {
        let store = XComStore::new();
        assert_eq!(store.pull(&TaskId::new("missing"), "key"), None);
    }

    #[test]
    fn overwrite_same_key() {
        let mut store = XComStore::new();
        let task = TaskId::new("extract");
        store.push(&task, "result", json!("old"));
        store.push(&task, "result", json!("new"));

        assert_eq!(store.pull(&task, "result"), Some(&json!("new")));
    }

    #[test]
    fn pull_all_returns_all_keys() {
        let mut store = XComStore::new();
        let task = TaskId::new("extract");
        store.push(&task, "a", json!(1));
        store.push(&task, "b", json!(2));

        let all = store.pull_all(&task).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all.get("a"), Some(&json!(1)));
        assert_eq!(all.get("b"), Some(&json!(2)));
    }

    #[test]
    fn pull_all_nonexistent_task_returns_none() {
        let store = XComStore::new();
        assert!(store.pull_all(&TaskId::new("missing")).is_none());
    }

    #[test]
    fn clear_task_removes_all_keys() {
        let mut store = XComStore::new();
        let task = TaskId::new("extract");
        store.push(&task, "a", json!(1));
        store.push(&task, "b", json!(2));
        store.clear_task(&task);

        assert_eq!(store.pull(&task, "a"), None);
        assert_eq!(store.pull(&task, "b"), None);
    }

    #[test]
    fn clear_task_nonexistent_is_noop() {
        let mut store = XComStore::new();
        store.clear_task(&TaskId::new("missing")); // should not panic
    }

    #[test]
    fn multiple_tasks_independent() {
        let mut store = XComStore::new();
        let t1 = TaskId::new("task_1");
        let t2 = TaskId::new("task_2");
        store.push(&t1, "key", json!("from_t1"));
        store.push(&t2, "key", json!("from_t2"));

        assert_eq!(store.pull(&t1, "key"), Some(&json!("from_t1")));
        assert_eq!(store.pull(&t2, "key"), Some(&json!("from_t2")));
    }
}
