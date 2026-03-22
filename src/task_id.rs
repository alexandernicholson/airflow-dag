/// Unique identifier for a task within a DAG.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TaskId(pub String);

impl TaskId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for TaskId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Unique identifier for a task group.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupId(pub String);

impl GroupId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl std::fmt::Display for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for GroupId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn task_id_from_str() {
        let id = TaskId::new("my_task");
        assert_eq!(id.0, "my_task");
    }

    #[test]
    fn task_id_from_string() {
        let id = TaskId::new(String::from("my_task"));
        assert_eq!(id.0, "my_task");
    }

    #[test]
    fn task_id_from_trait() {
        let id: TaskId = "my_task".into();
        assert_eq!(id.0, "my_task");
    }

    #[test]
    fn task_id_display() {
        let id = TaskId::new("extract_data");
        assert_eq!(format!("{}", id), "extract_data");
    }

    #[test]
    fn task_id_equality() {
        let a = TaskId::new("task_a");
        let b = TaskId::new("task_a");
        let c = TaskId::new("task_b");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn task_id_hashing() {
        let mut set = HashSet::new();
        set.insert(TaskId::new("task_a"));
        set.insert(TaskId::new("task_a"));
        set.insert(TaskId::new("task_b"));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn task_id_ordering() {
        let a = TaskId::new("alpha");
        let b = TaskId::new("beta");
        assert!(a < b);
    }

    #[test]
    fn task_id_clone() {
        let a = TaskId::new("task_a");
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn group_id_from_str() {
        let id = GroupId::new("my_group");
        assert_eq!(id.0, "my_group");
    }

    #[test]
    fn group_id_display() {
        let id = GroupId::new("processing");
        assert_eq!(format!("{}", id), "processing");
    }

    #[test]
    fn group_id_equality() {
        let a = GroupId::new("grp");
        let b = GroupId::new("grp");
        assert_eq!(a, b);
    }

    #[test]
    fn group_id_from_trait() {
        let id: GroupId = "grp".into();
        assert_eq!(id.0, "grp");
    }
}
