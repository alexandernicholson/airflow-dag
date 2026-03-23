use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use ironpipe::{Dag, Task, TaskExecutor, TaskId, TriggerRule};

use crate::executors::{AggregateExecutor, CleanExecutor, DownloadExecutor, ReportExecutor};

/// The months of data to process (one per worker node).
pub const MONTHS: [&str; 4] = ["01", "02", "03", "04"];
pub const YEAR: &str = "2024";

/// Build the NYC taxi ETL DAG.
///
/// ```text
/// download_01 → clean_01 ──┐
/// download_02 → clean_02 ──┤
/// download_03 → clean_03 ──├── aggregate → report
/// download_04 → clean_04 ──┘
/// ```
pub fn build_dag() -> Dag {
    let mut dag = Dag::new("nyc_taxi_etl");

    // Create download + clean tasks for each month
    for month in &MONTHS {
        dag.add_task(Task::builder(format!("download_{month}")).retries(2).build())
            .unwrap();
        dag.add_task(Task::builder(format!("clean_{month}")).build())
            .unwrap();
        dag.set_downstream(
            &TaskId::new(format!("download_{month}")),
            &TaskId::new(format!("clean_{month}")),
        )
        .unwrap();
    }

    // Aggregate waits for all clean tasks
    dag.add_task(
        Task::builder("aggregate")
            .trigger_rule(TriggerRule::AllSuccess)
            .build(),
    )
    .unwrap();

    for month in &MONTHS {
        dag.set_downstream(&TaskId::new(format!("clean_{month}")), &TaskId::new("aggregate"))
            .unwrap();
    }

    // Report follows aggregate
    dag.add_task(Task::builder("report").build()).unwrap();
    dag.set_downstream(&TaskId::new("aggregate"), &TaskId::new("report"))
        .unwrap();

    dag
}

/// Build executors for all tasks. In the distributed case, each node
/// only needs the executors for tasks it will run.
pub fn build_all_executors() -> HashMap<TaskId, Arc<dyn TaskExecutor>> {
    let counter = Arc::new(AtomicU64::new(0));
    let mut executors: HashMap<TaskId, Arc<dyn TaskExecutor>> = HashMap::new();

    for month in &MONTHS {
        executors.insert(
            TaskId::new(format!("download_{month}")),
            Arc::new(DownloadExecutor {
                month: (*month).to_string(),
                year: YEAR.to_string(),
            }),
        );
        executors.insert(
            TaskId::new(format!("clean_{month}")),
            Arc::new(CleanExecutor {
                month: (*month).to_string(),
            }),
        );
    }

    executors.insert(
        TaskId::new("aggregate"),
        Arc::new(AggregateExecutor {
            total_tasks: Arc::clone(&counter),
        }),
    );
    executors.insert(TaskId::new("report"), Arc::new(ReportExecutor));

    executors
}

/// Build executors for a specific worker node.
/// Worker N (2-5) handles month N-2 (index 0-3).
pub fn build_worker_executors(worker_index: usize) -> HashMap<TaskId, Arc<dyn TaskExecutor>> {
    let mut executors: HashMap<TaskId, Arc<dyn TaskExecutor>> = HashMap::new();

    // Each worker handles one month's download + clean
    if worker_index < MONTHS.len() {
        let month = MONTHS[worker_index];
        executors.insert(
            TaskId::new(format!("download_{month}")),
            Arc::new(DownloadExecutor {
                month: month.to_string(),
                year: YEAR.to_string(),
            }),
        );
        executors.insert(
            TaskId::new(format!("clean_{month}")),
            Arc::new(CleanExecutor {
                month: month.to_string(),
            }),
        );
    }

    executors
}
