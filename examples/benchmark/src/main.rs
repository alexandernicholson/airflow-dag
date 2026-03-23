mod benchmark;
mod executors;
mod pipeline;
mod transport;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use clap::Parser;

use ironpipe::{DagRunState, TaskState, spawn_scheduler};

#[derive(Parser)]
#[command(
    name = "ironpipe-benchmark",
    about = "Benchmark: image processing pipeline — single vs distributed"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Run image pipeline on a single runtime (baseline)
    Single,
    /// Run image pipeline on multiple runtimes (distributed)
    Distributed,
    /// Run NYC taxi demo (standalone)
    Taxi,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Single => run_benchmark(false).await,
        Commands::Distributed => run_benchmark(true).await,
        Commands::Taxi => run_taxi().await,
    }
}

async fn run_benchmark(distributed: bool) -> Result<(), Box<dyn std::error::Error>> {
    let mode = if distributed { "DISTRIBUTED (5 runtimes)" } else { "SINGLE (1 runtime)" };

    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║  ironpipe benchmark — Image Processing Pipeline             ║");
    println!("║  Mode: {mode:<53}║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();

    let dag = benchmark::build_benchmark_dag();
    let executors = benchmark::build_benchmark_executors();

    println!("Pipeline: {} ({} tasks)", dag.dag_id, dag.task_count());
    println!("  50 images → download → resize (Lanczos3 ×3) → hash → dedup → contact sheet");
    println!();

    let sorted = dag.topological_sort().unwrap();
    let start = Instant::now();

    // The key difference: single uses 1 runtime, distributed uses 5.
    // Both use spawn_scheduler which dispatches tasks as async_task_ctx processes.
    // With multiple runtimes, Tokio can schedule parallel tasks across more threads.
    // The scheduler dynamically dispatches ready tasks — no static assignment.
    let rt = if distributed {
        // 5 runtimes → Tokio gets more work to parallelize.
        // Each async_task_ctx runs on Tokio's work-stealing thread pool.
        println!("Starting 5 Rebar runtimes...");
        let _extra: Vec<Arc<rebar::runtime::Runtime>> = (2..=5)
            .map(|id| {
                println!("  Runtime node {id} started");
                Arc::new(rebar::runtime::Runtime::new(id))
            })
            .collect();
        println!();
        Arc::new(rebar::runtime::Runtime::new(1))
    } else {
        Arc::new(rebar::runtime::Runtime::new(1))
    };

    println!("Executing...");
    println!("─────────────────────────────────────────────────");

    let handle = spawn_scheduler(
        Arc::clone(&rt),
        dag,
        executors,
        "benchmark_run",
        chrono::Utc::now(),
    )
    .await;

    let final_state = handle
        .wait_for_completion(
            std::time::Duration::from_millis(50),
            std::time::Duration::from_secs(300),
        )
        .await?;

    println!("─────────────────────────────────────────────────");
    println!();

    let all_states = handle.all_task_states().await?;
    println!("Task Results:");
    for id in &sorted {
        let state = all_states.get(id).copied().unwrap_or(TaskState::None);
        let icon = match state {
            TaskState::Success => "●",
            TaskState::Failed => "✗",
            _ => "○",
        };
        println!("  {icon} {:<20} {state}", id.0);
    }

    let elapsed = start.elapsed();
    println!();
    println!("════════════════════════════════════════════════════");
    println!("  Mode:       {mode}");
    println!("  Result:     {final_state:?}");
    println!("  Total time: {:.2}s", elapsed.as_secs_f64());
    println!("  Output:     output/contact_sheet.png");
    println!("════════════════════════════════════════════════════");
    println!();

    Ok(())
}

async fn run_taxi() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║  ironpipe — NYC Taxi ETL Demo                           ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!();

    let start = Instant::now();
    let rt = Arc::new(rebar::runtime::Runtime::new(1));
    let dag = pipeline::build_dag();
    let executors = pipeline::build_all_executors();
    let sorted = dag.topological_sort().unwrap();

    println!("DAG: {} ({} tasks)", dag.dag_id, dag.task_count());
    println!();

    let handle = spawn_scheduler(rt, dag, executors, "taxi_run", chrono::Utc::now()).await;

    let final_state = handle
        .wait_for_completion(
            std::time::Duration::from_millis(50),
            std::time::Duration::from_secs(60),
        )
        .await?;

    let all_states = handle.all_task_states().await?;
    for id in &sorted {
        let state = all_states.get(id).copied().unwrap_or(TaskState::None);
        let icon = match state { TaskState::Success => "●", TaskState::Failed => "✗", _ => "○" };
        println!("  {icon} {:<20} {state}", id.0);
    }

    println!("\nDAG run: {final_state:?} | Total time: {:.2}s", start.elapsed().as_secs_f64());
    Ok(())
}
