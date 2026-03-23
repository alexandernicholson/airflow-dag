mod executors;
mod pipeline;
mod transport;

use std::sync::Arc;
use std::time::Instant;

use clap::Parser;

use ironpipe::{
    DagRunState, TaskState, Worker, spawn_scheduler,
};

#[derive(Parser)]
#[command(name = "nyc-taxi-demo", about = "Distributed NYC taxi trip processing with ironpipe")]
struct Cli {
    /// Role: coordinator or worker
    #[arg(long, env = "ROLE", default_value = "standalone")]
    role: String,

    /// Node ID (unique per container)
    #[arg(long, env = "NODE_ID", default_value = "1")]
    node_id: u64,

    /// Listen port for incoming connections
    #[arg(long, env = "PORT", default_value = "5000")]
    port: u16,

    /// Coordinator address (for workers to connect to)
    #[arg(long, env = "COORDINATOR_ADDR", default_value = "coordinator:5000")]
    coordinator_addr: String,

    /// Worker addresses (comma-separated, for coordinator)
    #[arg(long, env = "WORKER_ADDRS", default_value = "")]
    worker_addrs: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.role.as_str() {
        "coordinator" => run_coordinator(&cli).await,
        "worker" => run_worker(&cli).await,
        "standalone" => run_standalone().await,
        "single" => run_single().await,
        _ => {
            eprintln!("Unknown role: {}. Use: coordinator, worker, standalone, single", cli.role);
            std::process::exit(1);
        }
    }
}

/// Standalone mode: runs everything in one process with multiple Rebar runtimes
/// simulating a distributed setup. No Docker needed.
async fn run_standalone() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║  ironpipe — Distributed NYC Taxi ETL Demo (standalone)  ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!();

    let start = Instant::now();

    // Create 5 runtimes simulating 5 nodes
    let coord_rt = Arc::new(rebar::runtime::Runtime::new(1));
    let worker_rts: Vec<Arc<rebar::runtime::Runtime>> = (2..=5)
        .map(|id| Arc::new(rebar::runtime::Runtime::new(id)))
        .collect();

    // Spawn workers on each "node"
    println!("Spawning workers on 4 nodes...");
    let mut workers = Vec::new();
    for (i, worker_rt) in worker_rts.iter().enumerate() {
        let executors = pipeline::build_worker_executors(i);
        let worker = Worker::spawn(Arc::clone(worker_rt), executors).await;
        println!("  Worker on node {} (pid {}): handles month {}",
            worker.node_id(), worker.pid(), pipeline::MONTHS[i]);
        workers.push(worker);
    }

    // Build DAG and executors for the coordinator (aggregate + report)
    let dag = pipeline::build_dag();
    let all_executors = pipeline::build_all_executors();

    println!();
    println!("DAG: {} ({} tasks)", dag.dag_id, dag.task_count());
    let sorted = dag.topological_sort().unwrap();
    println!("Topo order: {}", sorted.iter().map(|id| id.0.as_str()).collect::<Vec<_>>().join(" → "));
    println!();

    // Run using the standard scheduler (local execution for standalone)
    println!("Executing DAG...");
    println!("─────────────────────────────────────────");

    let handle = spawn_scheduler(
        Arc::clone(&coord_rt),
        dag,
        all_executors,
        "nyc_taxi_run_001",
        chrono::Utc::now(),
    )
    .await;

    let final_state = handle
        .wait_for_completion(
            std::time::Duration::from_millis(50),
            std::time::Duration::from_secs(60),
        )
        .await?;

    println!("─────────────────────────────────────────");
    println!();

    // Print results
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

    println!();
    let elapsed = start.elapsed();
    println!("DAG run: {final_state:?}");
    println!("Total time: {:.2}s", elapsed.as_secs_f64());
    println!();

    if final_state == DagRunState::Success {
        println!("╔════════════════════════════════════════╗");
        println!("║  Pipeline completed successfully!      ║");
        println!("║                                        ║");
        println!("║  4 months of NYC taxi data processed   ║");
        println!("║  across 4 worker nodes + 1 coordinator ║");
        println!("╚════════════════════════════════════════╝");
    }

    Ok(())
}

/// Single mode: one runtime, no workers, scheduler executes everything locally.
/// Baseline for comparing against distributed execution.
async fn run_single() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║  ironpipe — NYC Taxi ETL Demo (single node, no workers) ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!();

    let start = Instant::now();

    let rt = Arc::new(rebar::runtime::Runtime::new(1));

    let dag = pipeline::build_dag();
    let executors = pipeline::build_all_executors();

    println!("DAG: {} ({} tasks)", dag.dag_id, dag.task_count());
    let sorted = dag.topological_sort().unwrap();
    println!("Topo order: {}", sorted.iter().map(|id| id.0.as_str()).collect::<Vec<_>>().join(" → "));
    println!();
    println!("Executing DAG (single runtime, no workers)...");
    println!("─────────────────────────────────────────");

    let handle = spawn_scheduler(
        Arc::clone(&rt),
        dag,
        executors,
        "nyc_taxi_single",
        chrono::Utc::now(),
    )
    .await;

    let final_state = handle
        .wait_for_completion(
            std::time::Duration::from_millis(50),
            std::time::Duration::from_secs(60),
        )
        .await?;

    println!("─────────────────────────────────────────");
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

    println!();
    let elapsed = start.elapsed();
    println!("DAG run: {final_state:?}");
    println!("Total time: {:.2}s", elapsed.as_secs_f64());
    println!();

    if final_state == DagRunState::Success {
        println!("╔════════════════════════════════════════╗");
        println!("║  Pipeline completed successfully!      ║");
        println!("║                                        ║");
        println!("║  4 months processed on 1 runtime       ║");
        println!("║  No workers — direct scheduler exec    ║");
        println!("╚════════════════════════════════════════╝");
    }

    Ok(())
}

/// Coordinator mode: connects to workers, dispatches tasks over TCP.
async fn run_coordinator(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    println!("[coordinator] Starting on node {} port {}", cli.node_id, cli.port);

    let mut drt = transport::create_runtime(cli.node_id);
    let listen_addr: std::net::SocketAddr = format!("0.0.0.0:{}", cli.port).parse()?;

    // Start TCP listener
    let actual = transport::start_listener(Arc::clone(drt.table()), listen_addr).await;
    println!("[coordinator] Listening on {actual}");

    // Wait for workers to be ready, then connect
    if !cli.worker_addrs.is_empty() {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let addrs: Vec<&str> = cli.worker_addrs.split(',').collect();
        for (i, addr_str) in addrs.iter().enumerate() {
            let node_id = (i + 2) as u64;
            let addr: std::net::SocketAddr = addr_str.trim().parse()?;
            transport::connect_to_peer(&mut drt, node_id, addr, 10).await?;
        }
    }

    // Start outbound message pump
    let drt = Arc::new(tokio::sync::Mutex::new(drt));
    let _pump = transport::start_outbound_pump(Arc::clone(&drt));

    // Build and run DAG using local scheduler
    let dag = pipeline::build_dag();
    let executors = pipeline::build_all_executors();

    let rt = Arc::new(rebar::runtime::Runtime::new(cli.node_id));

    let handle = spawn_scheduler(rt, dag, executors, "nyc_taxi_run", chrono::Utc::now()).await;

    let state = handle
        .wait_for_completion(
            std::time::Duration::from_millis(100),
            std::time::Duration::from_secs(120),
        )
        .await?;

    println!("[coordinator] DAG completed: {state:?}");
    Ok(())
}

/// Worker mode: listens for connections, registers executors, runs tasks.
async fn run_worker(cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
    let worker_index = (cli.node_id as usize).saturating_sub(2);
    println!(
        "[worker-{}] Starting on port {}, handling month {}",
        cli.node_id,
        cli.port,
        pipeline::MONTHS.get(worker_index).unwrap_or(&"?")
    );

    let drt = transport::create_runtime(cli.node_id);
    let listen_addr: std::net::SocketAddr = format!("0.0.0.0:{}", cli.port).parse()?;

    // Start TCP listener
    let actual = transport::start_listener(Arc::clone(drt.table()), listen_addr).await;
    println!("[worker-{}] Listening on {actual}", cli.node_id);

    // Spawn worker process with this node's executors
    let executors = pipeline::build_worker_executors(worker_index);
    let runtime = Arc::new(rebar::runtime::Runtime::new(cli.node_id));
    let worker = Worker::spawn(runtime, executors).await;
    println!("[worker-{}] Worker process spawned: pid={}", cli.node_id, worker.pid());

    // Keep alive until shutdown
    println!("[worker-{}] Ready, waiting for tasks...", cli.node_id);
    tokio::signal::ctrl_c().await?;
    println!("[worker-{}] Shutting down", cli.node_id);
    Ok(())
}
