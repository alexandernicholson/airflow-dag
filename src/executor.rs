use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::task_id::TaskId;
use crate::xcom::XComStore;

/// Context passed to a task during execution.
/// Provides access to XCom push/pull and task metadata.
pub struct TaskContext {
    pub task_id: TaskId,
    pub run_id: String,
    pub logical_date: DateTime<Utc>,
    pub attempt: u32,
    xcom: XComStore,
}

impl TaskContext {
    pub fn new(
        task_id: TaskId,
        run_id: String,
        logical_date: DateTime<Utc>,
        attempt: u32,
    ) -> Self {
        Self {
            task_id,
            run_id,
            logical_date,
            attempt,
            xcom: XComStore::new(),
        }
    }

    /// Push an XCom value for this task.
    pub fn xcom_push(&mut self, key: &str, value: serde_json::Value) {
        self.xcom.push(&self.task_id, key, value);
    }

    /// Get all XCom values produced by this task.
    pub fn xcom_values(&self) -> Option<&HashMap<String, serde_json::Value>> {
        self.xcom.pull_all(&self.task_id)
    }
}

/// Trait for defining what a task does when executed.
/// Implement this to provide task logic.
#[async_trait::async_trait]
pub trait TaskExecutor: Send + Sync + 'static {
    async fn execute(&self, ctx: &mut TaskContext) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

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

    struct XComExecutor;

    #[async_trait::async_trait]
    impl TaskExecutor for XComExecutor {
        async fn execute(
            &self,
            ctx: &mut TaskContext,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            ctx.xcom_push("return_value", json!(42));
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
            Err("task failed".into())
        }
    }

    #[tokio::test]
    async fn noop_executor_succeeds() {
        let mut ctx = TaskContext::new(
            TaskId::new("test"),
            "run_1".to_string(),
            Utc::now(),
            1,
        );
        let result = NoopExecutor.execute(&mut ctx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn xcom_executor_pushes_values() {
        let mut ctx = TaskContext::new(
            TaskId::new("test"),
            "run_1".to_string(),
            Utc::now(),
            1,
        );
        XComExecutor.execute(&mut ctx).await.unwrap();
        let values = ctx.xcom_values().unwrap();
        assert_eq!(values.get("return_value"), Some(&json!(42)));
    }

    #[tokio::test]
    async fn failing_executor_returns_error() {
        let mut ctx = TaskContext::new(
            TaskId::new("test"),
            "run_1".to_string(),
            Utc::now(),
            1,
        );
        let result = FailingExecutor.execute(&mut ctx).await;
        assert!(result.is_err());
    }

    #[test]
    fn task_context_fields() {
        let ctx = TaskContext::new(
            TaskId::new("my_task"),
            "run_42".to_string(),
            Utc::now(),
            3,
        );
        assert_eq!(ctx.task_id, TaskId::new("my_task"));
        assert_eq!(ctx.run_id, "run_42");
        assert_eq!(ctx.attempt, 3);
    }
}
