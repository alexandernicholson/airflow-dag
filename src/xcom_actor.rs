use std::collections::HashMap;

use rebar::gen_server::{GenServer, GenServerContext};
use rebar::process::ProcessId;

use crate::task_id::TaskId;
use crate::xcom::XComStore;

/// Call messages for the XCom actor.
pub enum XComCall {
    Pull { task_id: TaskId, key: String },
    PullAll { task_id: TaskId },
}

/// Cast messages for the XCom actor.
pub enum XComCast {
    Push {
        task_id: TaskId,
        key: String,
        value: serde_json::Value,
    },
    ClearTask {
        task_id: TaskId,
    },
}

/// Reply messages from the XCom actor.
pub enum XComReply {
    Value(Option<serde_json::Value>),
    All(Option<HashMap<String, serde_json::Value>>),
}

/// GenServer actor for shared XCom state.
/// Provides serialized access to XCom data via call/cast.
pub struct XComActor;

#[async_trait::async_trait]
impl GenServer for XComActor {
    type State = XComStore;
    type Call = XComCall;
    type Cast = XComCast;
    type Reply = XComReply;

    async fn init(&self, _ctx: &GenServerContext) -> Result<Self::State, String> {
        Ok(XComStore::new())
    }

    async fn handle_call(
        &self,
        msg: Self::Call,
        _from: ProcessId,
        state: &mut Self::State,
        _ctx: &GenServerContext,
    ) -> Self::Reply {
        match msg {
            XComCall::Pull { task_id, key } => {
                let value = state.pull(&task_id, &key).cloned();
                XComReply::Value(value)
            }
            XComCall::PullAll { task_id } => {
                let all = state.pull_all(&task_id).cloned();
                XComReply::All(all)
            }
        }
    }

    async fn handle_cast(
        &self,
        msg: Self::Cast,
        state: &mut Self::State,
        _ctx: &GenServerContext,
    ) {
        match msg {
            XComCast::Push {
                task_id,
                key,
                value,
            } => {
                state.push(&task_id, &key, value);
            }
            XComCast::ClearTask { task_id } => {
                state.clear_task(&task_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rebar::gen_server::spawn_gen_server;
    use rebar::runtime::Runtime;
    use serde_json::json;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn xcom_actor_push_and_pull() {
        let rt = Arc::new(Runtime::new(1));
        let xcom_ref = spawn_gen_server(Arc::clone(&rt), XComActor).await;

        // Push a value
        xcom_ref
            .cast(XComCast::Push {
                task_id: TaskId::new("task_a"),
                key: "result".to_string(),
                value: json!(42),
            })
            .unwrap();

        // Give the cast time to process
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Pull it back
        let reply = xcom_ref
            .call(
                XComCall::Pull {
                    task_id: TaskId::new("task_a"),
                    key: "result".to_string(),
                },
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        match reply {
            XComReply::Value(Some(v)) => assert_eq!(v, json!(42)),
            _ => panic!("expected Some(42)"),
        }
    }

    #[tokio::test]
    async fn xcom_actor_pull_nonexistent() {
        let rt = Arc::new(Runtime::new(1));
        let xcom_ref = spawn_gen_server(Arc::clone(&rt), XComActor).await;

        let reply = xcom_ref
            .call(
                XComCall::Pull {
                    task_id: TaskId::new("missing"),
                    key: "x".to_string(),
                },
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        match reply {
            XComReply::Value(None) => {}
            _ => panic!("expected None"),
        }
    }

    #[tokio::test]
    async fn xcom_actor_pull_all() {
        let rt = Arc::new(Runtime::new(1));
        let xcom_ref = spawn_gen_server(Arc::clone(&rt), XComActor).await;

        xcom_ref
            .cast(XComCast::Push {
                task_id: TaskId::new("task_a"),
                key: "k1".to_string(),
                value: json!("v1"),
            })
            .unwrap();
        xcom_ref
            .cast(XComCast::Push {
                task_id: TaskId::new("task_a"),
                key: "k2".to_string(),
                value: json!("v2"),
            })
            .unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;

        let reply = xcom_ref
            .call(
                XComCall::PullAll {
                    task_id: TaskId::new("task_a"),
                },
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        match reply {
            XComReply::All(Some(map)) => {
                assert_eq!(map.len(), 2);
                assert_eq!(map.get("k1"), Some(&json!("v1")));
            }
            _ => panic!("expected map with 2 entries"),
        }
    }

    #[tokio::test]
    async fn xcom_actor_clear_task() {
        let rt = Arc::new(Runtime::new(1));
        let xcom_ref = spawn_gen_server(Arc::clone(&rt), XComActor).await;

        xcom_ref
            .cast(XComCast::Push {
                task_id: TaskId::new("task_a"),
                key: "k".to_string(),
                value: json!(1),
            })
            .unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;

        xcom_ref
            .cast(XComCast::ClearTask {
                task_id: TaskId::new("task_a"),
            })
            .unwrap();

        tokio::time::sleep(Duration::from_millis(10)).await;

        let reply = xcom_ref
            .call(
                XComCall::Pull {
                    task_id: TaskId::new("task_a"),
                    key: "k".to_string(),
                },
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        match reply {
            XComReply::Value(None) => {}
            _ => panic!("expected None after clear"),
        }
    }
}
