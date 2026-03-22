use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders, Cell, Padding, Paragraph, Row, Table, Tabs};
use ratatui::Terminal;

use airflow_dag::{
    Dag, DagRun, DagRunState, Task, TaskContext, TaskExecutor, TaskId, TaskState, TriggerRule,
    spawn_scheduler,
};

#[derive(Parser)]
#[command(name = "dag-cli", about = "Airflow-compatible DAG design and execution tool")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Launch interactive TUI for DAG visualization
    Tui,
    /// Run a demo DAG and show execution
    Demo,
    /// Print a DAG diagram to stdout
    Diagram {
        /// Which demo DAG to render: etl, diamond, ml
        #[arg(default_value = "diamond")]
        dag: String,
        /// Render vertically (top-to-bottom) instead of horizontally (left-to-right)
        #[arg(short, long)]
        vertical: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Demo) => run_demo().await?,
        Some(Commands::Diagram { dag, vertical }) => print_diagram(&dag, vertical),
        Some(Commands::Tui) | None => run_tui()?,
    }

    Ok(())
}

// =============================================================================
// Demo DAGs
// =============================================================================

fn build_etl_dag() -> Dag {
    let mut dag = Dag::new("etl_pipeline");
    dag.add_task(Task::builder("extract").build()).unwrap();
    dag.add_task(Task::builder("validate").build()).unwrap();
    dag.add_task(
        Task::builder("transform")
            .trigger_rule(TriggerRule::AllSuccess)
            .retries(2)
            .build(),
    )
    .unwrap();
    dag.add_task(Task::builder("load").build()).unwrap();
    dag.add_task(
        Task::builder("notify")
            .trigger_rule(TriggerRule::AllDone)
            .build(),
    )
    .unwrap();

    dag.set_downstream(&TaskId::new("extract"), &TaskId::new("validate"))
        .unwrap();
    dag.set_downstream(&TaskId::new("validate"), &TaskId::new("transform"))
        .unwrap();
    dag.set_downstream(&TaskId::new("transform"), &TaskId::new("load"))
        .unwrap();
    dag.set_downstream(&TaskId::new("load"), &TaskId::new("notify"))
        .unwrap();
    dag
}

fn build_diamond_dag() -> Dag {
    let mut dag = Dag::new("diamond_pipeline");
    dag.add_task(Task::builder("start").build()).unwrap();
    dag.add_task(Task::builder("branch_a").retries(1).build())
        .unwrap();
    dag.add_task(Task::builder("branch_b").build()).unwrap();
    dag.add_task(
        Task::builder("join")
            .trigger_rule(TriggerRule::AllSuccess)
            .build(),
    )
    .unwrap();
    dag.add_task(
        Task::builder("cleanup")
            .trigger_rule(TriggerRule::Always)
            .build(),
    )
    .unwrap();

    dag.set_downstream(&TaskId::new("start"), &TaskId::new("branch_a"))
        .unwrap();
    dag.set_downstream(&TaskId::new("start"), &TaskId::new("branch_b"))
        .unwrap();
    dag.set_downstream(&TaskId::new("branch_a"), &TaskId::new("join"))
        .unwrap();
    dag.set_downstream(&TaskId::new("branch_b"), &TaskId::new("join"))
        .unwrap();
    dag.set_downstream(&TaskId::new("join"), &TaskId::new("cleanup"))
        .unwrap();
    dag
}

fn build_complex_dag() -> Dag {
    let mut dag = Dag::new("ml_training");
    dag.add_task(Task::builder("fetch_data").build()).unwrap();
    dag.add_task(Task::builder("clean_data").retries(1).build())
        .unwrap();
    dag.add_task(Task::builder("feature_eng").build()).unwrap();
    dag.add_task(Task::builder("train_model_a").retries(2).build())
        .unwrap();
    dag.add_task(Task::builder("train_model_b").retries(2).build())
        .unwrap();
    dag.add_task(
        Task::builder("evaluate")
            .trigger_rule(TriggerRule::AllDoneMinOneSuccess)
            .build(),
    )
    .unwrap();
    dag.add_task(Task::builder("deploy").build()).unwrap();
    dag.add_task(
        Task::builder("alert")
            .trigger_rule(TriggerRule::OneFailed)
            .build(),
    )
    .unwrap();

    dag.chain(&[
        TaskId::new("fetch_data"),
        TaskId::new("clean_data"),
        TaskId::new("feature_eng"),
    ])
    .unwrap();
    dag.set_downstream(&TaskId::new("feature_eng"), &TaskId::new("train_model_a"))
        .unwrap();
    dag.set_downstream(&TaskId::new("feature_eng"), &TaskId::new("train_model_b"))
        .unwrap();
    dag.set_downstream(&TaskId::new("train_model_a"), &TaskId::new("evaluate"))
        .unwrap();
    dag.set_downstream(&TaskId::new("train_model_b"), &TaskId::new("evaluate"))
        .unwrap();
    dag.set_downstream(&TaskId::new("evaluate"), &TaskId::new("deploy"))
        .unwrap();
    dag.set_downstream(&TaskId::new("train_model_a"), &TaskId::new("alert"))
        .unwrap();
    dag.set_downstream(&TaskId::new("train_model_b"), &TaskId::new("alert"))
        .unwrap();
    dag
}

// =============================================================================
// Demo executor
// =============================================================================

struct DemoExecutor {
    delay_ms: u64,
    fail: bool,
}

#[async_trait::async_trait]
impl TaskExecutor for DemoExecutor {
    async fn execute(
        &self,
        ctx: &mut TaskContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
        if self.fail {
            return Err(format!("Task {} failed", ctx.task_id).into());
        }
        ctx.xcom_push("return_value", serde_json::json!(format!("{} done", ctx.task_id)));
        Ok(())
    }
}

// =============================================================================
// Demo runner
// =============================================================================

async fn run_demo() -> Result<(), Box<dyn std::error::Error>> {
    let dag = build_diamond_dag();
    let rt = Arc::new(rebar::runtime::Runtime::new(1));

    let mut executors: HashMap<TaskId, Arc<dyn TaskExecutor>> = HashMap::new();
    for id in dag.task_ids() {
        executors.insert(
            id.clone(),
            Arc::new(DemoExecutor {
                delay_ms: 200,
                fail: false,
            }),
        );
    }

    println!("Running DAG: {}", dag.dag_id);
    println!("Tasks: {:?}", dag.topological_sort().unwrap());

    let handle = spawn_scheduler(rt, dag, executors, "demo_run", chrono::Utc::now()).await;

    let state = handle
        .wait_for_completion(Duration::from_millis(50), Duration::from_secs(30))
        .await?;

    println!("\nRun complete: {state:?}");
    let all = handle.all_task_states().await?;
    for (id, s) in &all {
        println!("  {id} -> {s}");
    }

    Ok(())
}

// =============================================================================
// TUI Application
// =============================================================================

struct App {
    dags: Vec<Dag>,
    dag_runs: Vec<Option<DagRun>>,
    selected_dag: usize,
    selected_tab: usize,
    should_quit: bool,
    run_states: Vec<HashMap<TaskId, TaskState>>,
    tick_count: usize,
    auto_tick: bool,
}

impl App {
    fn new() -> Self {
        let dags = vec![build_etl_dag(), build_diamond_dag(), build_complex_dag()];
        let dag_runs: Vec<Option<DagRun>> = dags.iter().map(|_| None).collect();
        let run_states: Vec<HashMap<TaskId, TaskState>> =
            dags.iter().map(|_| HashMap::new()).collect();
        Self {
            dags,
            dag_runs,
            selected_dag: 0,
            selected_tab: 0,
            should_quit: false,
            run_states,
            tick_count: 0,
            auto_tick: false,
        }
    }

    fn current_dag(&self) -> &Dag {
        &self.dags[self.selected_dag]
    }

    fn start_run(&mut self) {
        let dag = self.dags[self.selected_dag].clone();
        let run = DagRun::new(dag, format!("run_{}", self.tick_count), chrono::Utc::now());
        self.dag_runs[self.selected_dag] = Some(run);
        self.run_states[self.selected_dag] = HashMap::new();
        self.tick_count = 0;
        self.auto_tick = false;
    }

    fn tick_run(&mut self) {
        if let Some(ref mut run) = self.dag_runs[self.selected_dag] {
            if run.is_complete() {
                return;
            }

            let ready = run.tick();

            // Auto-mark ready tasks as running, then succeed them on next tick
            for task_id in &ready {
                let _ = run.mark_running(task_id);
            }

            // Succeed all running tasks (simulating instant execution)
            let running: Vec<TaskId> = self
                .dags[self.selected_dag]
                .task_ids()
                .filter(|id| run.task_state(id) == TaskState::Running && !ready.contains(id))
                .cloned()
                .collect();

            for task_id in &running {
                let _ = run.mark_success(task_id);
            }

            // Update state snapshot
            let states: HashMap<TaskId, TaskState> = self.dags[self.selected_dag]
                .task_ids()
                .map(|id| (id.clone(), run.task_state(id)))
                .collect();
            self.run_states[self.selected_dag] = states;
            self.tick_count += 1;
        }
    }
}

// =============================================================================
// TUI main loop
// =============================================================================

fn run_tui() -> Result<(), Box<dyn std::error::Error>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new();

    loop {
        terminal.draw(|f| ui(f, &app))?;

        if event::poll(Duration::from_millis(if app.auto_tick { 300 } else { 50 }))?
            && let Event::Key(key) = event::read()?
        {
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => app.should_quit = true,
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    app.should_quit = true;
                }
                KeyCode::Tab => {
                    app.selected_tab = (app.selected_tab + 1) % 3;
                }
                KeyCode::BackTab => {
                    app.selected_tab = if app.selected_tab == 0 {
                        2
                    } else {
                        app.selected_tab - 1
                    };
                }
                KeyCode::Left => {
                    app.selected_dag = if app.selected_dag == 0 {
                        app.dags.len() - 1
                    } else {
                        app.selected_dag - 1
                    };
                }
                KeyCode::Right => {
                    app.selected_dag = (app.selected_dag + 1) % app.dags.len();
                }
                KeyCode::Char('r') => {
                    app.start_run();
                }
                KeyCode::Char('t') => {
                    app.tick_run();
                }
                KeyCode::Char('a') => {
                    app.auto_tick = !app.auto_tick;
                }
                _ => {}
            }
        }

        if app.auto_tick {
            app.tick_run();
        }

        if app.should_quit {
            break;
        }
    }

    disable_raw_mode()?;
    crossterm::execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}

// =============================================================================
// UI rendering
// =============================================================================

fn ui(f: &mut ratatui::Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Title bar
            Constraint::Length(3), // DAG selector
            Constraint::Length(3), // Tab bar
            Constraint::Min(10),   // Main content
            Constraint::Length(3), // Status bar
        ])
        .split(f.area());

    render_title(f, chunks[0]);
    render_dag_selector(f, app, chunks[1]);
    render_tabs(f, app, chunks[2]);

    match app.selected_tab {
        0 => render_dag_structure(f, app, chunks[3]),
        1 => render_task_table(f, app, chunks[3]),
        2 => render_execution_view(f, app, chunks[3]),
        _ => {}
    }

    render_status_bar(f, app, chunks[4]);
}

fn render_title(f: &mut ratatui::Frame, area: Rect) {
    let title = Paragraph::new(Line::from(vec![
        Span::styled(
            " airflow-dag ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            "Airflow-compatible DAG engine for Rust + Rebar",
            Style::default().fg(Color::DarkGray),
        ),
    ]))
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(title, area);
}

fn render_dag_selector(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let dag_names: Vec<Span> = app
        .dags
        .iter()
        .enumerate()
        .map(|(i, dag)| {
            if i == app.selected_dag {
                Span::styled(
                    format!(" {} ", dag.dag_id),
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                )
            } else {
                Span::styled(
                    format!(" {} ", dag.dag_id),
                    Style::default().fg(Color::White),
                )
            }
        })
        .collect();

    let selector = Paragraph::new(Line::from(dag_names)).block(
        Block::default()
            .title(" DAGs (←/→) ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::Yellow)),
    );
    f.render_widget(selector, area);
}

fn render_tabs(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let titles = vec!["Structure", "Tasks", "Execution"];
    let tabs = Tabs::new(titles)
        .select(app.selected_tab)
        .style(Style::default().fg(Color::White))
        .highlight_style(
            Style::default()
                .fg(Color::Black)
                .bg(Color::Green)
                .add_modifier(Modifier::BOLD),
        )
        .divider(" | ")
        .block(
            Block::default()
                .title(" View (Tab) ")
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(Color::Green)),
        );
    f.render_widget(tabs, area);
}

fn render_dag_structure(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let dag = app.current_dag();
    let sorted = dag.topological_sort().unwrap_or_default();

    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(vec![
        Span::styled("DAG: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            &dag.dag_id,
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!("  ({} tasks)", dag.task_count()),
            Style::default().fg(Color::DarkGray),
        ),
    ]));
    lines.push(Line::from(""));

    // Render each task with its edges
    for task_id in &sorted {
        let task = dag.get_task(task_id).unwrap();
        let downstream = dag.downstream_of(task_id);

        let state_in_run = app.run_states[app.selected_dag]
            .get(task_id)
            .copied()
            .unwrap_or(TaskState::None);

        let state_color = state_color(state_in_run);
        let state_icon = state_icon(state_in_run);

        // Task line
        let mut spans = vec![
            Span::styled(
                format!("  {state_icon} "),
                Style::default().fg(state_color),
            ),
            Span::styled(
                format!("{:<20}", task_id.0),
                Style::default()
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            ),
        ];

        if task.trigger_rule != TriggerRule::AllSuccess {
            spans.push(Span::styled(
                format!(" [{:?}]", task.trigger_rule),
                Style::default().fg(Color::Magenta),
            ));
        }

        if task.retries > 0 {
            spans.push(Span::styled(
                format!(" retries={}", task.retries),
                Style::default().fg(Color::Yellow),
            ));
        }

        lines.push(Line::from(spans));

        // Dependency arrows
        if !downstream.is_empty() {
            let mut ds: Vec<&TaskId> = downstream.iter().collect();
            ds.sort();
            let arrow_str = ds
                .iter()
                .map(|id| id.0.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(Line::from(vec![
                Span::styled("       └─▶ ", Style::default().fg(Color::DarkGray)),
                Span::styled(arrow_str, Style::default().fg(Color::Blue)),
            ]));
        }
    }

    let block = Block::default()
        .title(" DAG Structure ")
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Cyan))
        .padding(Padding::new(1, 1, 1, 1));

    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
}

fn render_task_table(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let dag = app.current_dag();
    let sorted = dag.topological_sort().unwrap_or_default();

    let header = Row::new(vec![
        Cell::from("Task ID").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("State").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("Trigger Rule").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("Retries").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("Upstream").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("Downstream").style(Style::default().add_modifier(Modifier::BOLD)),
    ])
    .style(Style::default().fg(Color::Yellow));

    let rows: Vec<Row> = sorted
        .iter()
        .map(|task_id| {
            let task = dag.get_task(task_id).unwrap();
            let state = app.run_states[app.selected_dag]
                .get(task_id)
                .copied()
                .unwrap_or(TaskState::None);

            let upstream = dag.upstream_of(task_id);
            let downstream = dag.downstream_of(task_id);

            let mut up_ids: Vec<&str> = upstream.iter().map(|id| id.0.as_str()).collect();
            up_ids.sort_unstable();
            let mut down_ids: Vec<&str> = downstream.iter().map(|id| id.0.as_str()).collect();
            down_ids.sort_unstable();

            Row::new(vec![
                Cell::from(task_id.0.clone())
                    .style(Style::default().add_modifier(Modifier::BOLD)),
                Cell::from(format!("{} {}", state_icon(state), state))
                    .style(Style::default().fg(state_color(state))),
                Cell::from(format!("{:?}", task.trigger_rule)),
                Cell::from(format!("{}", task.retries)),
                Cell::from(up_ids.join(", ")),
                Cell::from(down_ids.join(", ")),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(20),
            Constraint::Length(18),
            Constraint::Length(28),
            Constraint::Length(8),
            Constraint::Min(15),
            Constraint::Min(15),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .title(" Task Details ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::Cyan))
            .padding(Padding::new(1, 1, 0, 0)),
    );

    f.render_widget(table, area);
}

fn render_execution_view(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    // Left: visual DAG with state colors
    render_dag_visual(f, app, chunks[0]);

    // Right: execution log / run info
    render_run_info(f, app, chunks[1]);
}

fn render_dag_visual(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let dag = app.current_dag();
    let sorted = dag.topological_sort().unwrap_or_default();

    // Assign layers for visual layout
    let mut layers: Vec<Vec<&TaskId>> = Vec::new();
    let mut assigned: HashMap<&TaskId, usize> = HashMap::new();

    for task_id in &sorted {
        let upstream = dag.upstream_of(task_id);
        let layer = if upstream.is_empty() {
            0
        } else {
            upstream
                .iter()
                .map(|u| assigned.get(u).copied().unwrap_or(0) + 1)
                .max()
                .unwrap_or(0)
        };
        assigned.insert(task_id, layer);
        while layers.len() <= layer {
            layers.push(Vec::new());
        }
        layers[layer].push(task_id);
    }

    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(Span::styled(
        "DAG Execution Graph",
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )));
    lines.push(Line::from(""));

    for (layer_idx, layer) in layers.iter().enumerate() {
        if layer_idx > 0 {
            // Draw connector
            let connector: String = layer
                .iter()
                .map(|_| "    │     ")
                .collect::<Vec<_>>()
                .join("");
            lines.push(Line::from(Span::styled(
                connector,
                Style::default().fg(Color::DarkGray),
            )));
            let arrow: String = layer
                .iter()
                .map(|_| "    ▼     ")
                .collect::<Vec<_>>()
                .join("");
            lines.push(Line::from(Span::styled(
                arrow,
                Style::default().fg(Color::DarkGray),
            )));
        }

        let mut spans: Vec<Span> = Vec::new();
        for (i, task_id) in layer.iter().enumerate() {
            if i > 0 {
                spans.push(Span::raw("  "));
            }
            let state = app.run_states[app.selected_dag]
                .get(*task_id)
                .copied()
                .unwrap_or(TaskState::None);
            let color = state_color(state);
            let icon = state_icon(state);

            spans.push(Span::styled(
                format!(" {} {} ", icon, task_id.0),
                Style::default().fg(color).add_modifier(Modifier::BOLD),
            ));
        }
        lines.push(Line::from(spans));
    }

    let block = Block::default()
        .title(" DAG Graph ")
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Cyan))
        .padding(Padding::new(2, 2, 1, 1));

    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
}

fn render_run_info(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let mut lines: Vec<Line> = Vec::new();

    if let Some(ref run) = app.dag_runs[app.selected_dag] {
        lines.push(Line::from(vec![
            Span::styled("Run ID: ", Style::default().fg(Color::DarkGray)),
            Span::styled(&run.run_id, Style::default().fg(Color::White)),
        ]));
        lines.push(Line::from(vec![
            Span::styled("State:  ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{:?}", run.run_state()),
                Style::default().fg(match run.run_state() {
                    DagRunState::Queued => Color::Gray,
                    DagRunState::Running => Color::Blue,
                    DagRunState::Success => Color::Green,
                    DagRunState::Failed => Color::Red,
                }),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Tick:   ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                format!("{}", app.tick_count),
                Style::default().fg(Color::Yellow),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Auto:   ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                if app.auto_tick { "ON" } else { "OFF" },
                Style::default().fg(if app.auto_tick {
                    Color::Green
                } else {
                    Color::Red
                }),
            ),
        ]));
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "Task States:",
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::from(""));

        let dag = app.current_dag();
        let sorted = dag.topological_sort().unwrap_or_default();
        for task_id in &sorted {
            let state = run.task_state(task_id);
            let attempt = run.attempt_count(task_id);
            lines.push(Line::from(vec![
                Span::styled(
                    format!("  {} ", state_icon(state)),
                    Style::default().fg(state_color(state)),
                ),
                Span::styled(
                    format!("{:<18}", task_id.0),
                    Style::default().fg(Color::White),
                ),
                Span::styled(
                    format!("{:<16}", format!("{}", state)),
                    Style::default().fg(state_color(state)),
                ),
                if attempt > 0 {
                    Span::styled(
                        format!("(attempt {attempt})"),
                        Style::default().fg(Color::DarkGray),
                    )
                } else {
                    Span::raw("")
                },
            ]));
        }
    } else {
        lines.push(Line::from(Span::styled(
            "No active run",
            Style::default().fg(Color::DarkGray),
        )));
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "Press 'r' to start a run",
            Style::default().fg(Color::Yellow),
        )));
    }

    let block = Block::default()
        .title(" Run Status ")
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(Color::Cyan))
        .padding(Padding::new(1, 1, 1, 1));

    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
}

fn render_status_bar(f: &mut ratatui::Frame, _app: &App, area: Rect) {
    let help = Line::from(vec![
        Span::styled(" q", Style::default().fg(Color::Yellow)),
        Span::styled(" quit  ", Style::default().fg(Color::DarkGray)),
        Span::styled("←/→", Style::default().fg(Color::Yellow)),
        Span::styled(" switch DAG  ", Style::default().fg(Color::DarkGray)),
        Span::styled("Tab", Style::default().fg(Color::Yellow)),
        Span::styled(" switch view  ", Style::default().fg(Color::DarkGray)),
        Span::styled("r", Style::default().fg(Color::Yellow)),
        Span::styled(" new run  ", Style::default().fg(Color::DarkGray)),
        Span::styled("t", Style::default().fg(Color::Yellow)),
        Span::styled(" tick  ", Style::default().fg(Color::DarkGray)),
        Span::styled("a", Style::default().fg(Color::Yellow)),
        Span::styled(" auto-tick", Style::default().fg(Color::DarkGray)),
    ]);

    let status = Paragraph::new(help).block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(Color::DarkGray)),
    );
    f.render_widget(status, area);
}

// =============================================================================
// Diagram renderer (stdout)
// =============================================================================

fn print_diagram(dag_name: &str, vertical: bool) {
    let dag = match dag_name {
        "etl" => build_etl_dag(),
        "diamond" => build_diamond_dag(),
        "ml" => build_complex_dag(),
        _ => {
            eprintln!("Unknown DAG: {dag_name}. Choose: etl, diamond, ml");
            std::process::exit(1);
        }
    };

    let sorted = dag.topological_sort().unwrap();
    let layers = compute_layers(&dag, &sorted);

    print_title(&dag);

    if vertical {
        print_vertical(&dag, &layers);
    } else {
        print_horizontal(&dag, &layers);
    }

    print_summary(&dag, &sorted);
}

fn compute_layers<'a>(dag: &Dag, sorted: &'a [TaskId]) -> Vec<Vec<&'a TaskId>> {
    let mut depth: HashMap<&TaskId, usize> = HashMap::new();
    let mut layers: Vec<Vec<&TaskId>> = Vec::new();

    for task_id in sorted {
        let upstream = dag.upstream_of(task_id);
        let d = if upstream.is_empty() {
            0
        } else {
            upstream
                .iter()
                .map(|u| depth.get(u).copied().unwrap_or(0) + 1)
                .max()
                .unwrap_or(0)
        };
        depth.insert(task_id, d);
        while layers.len() <= d {
            layers.push(Vec::new());
        }
        layers[d].push(task_id);
    }
    layers
}

fn print_title(dag: &Dag) {
    println!();
    let title = format!("DAG: {}", dag.dag_id);
    println!("╔{}╗", "═".repeat(title.len() + 2));
    println!("║ {title} ║");
    println!("╚{}╝", "═".repeat(title.len() + 2));
    println!();
}

fn print_summary(dag: &Dag, sorted: &[TaskId]) {
    println!();
    println!("  Tasks: {}", dag.task_count());
    println!(
        "  Roots: {}",
        dag.roots()
            .iter()
            .map(|id| id.0.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "  Leaves: {}",
        dag.leaves()
            .iter()
            .map(|id| id.0.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!(
        "  Topo order: {}",
        sorted
            .iter()
            .map(|id| id.0.as_str())
            .collect::<Vec<_>>()
            .join(" → ")
    );
    println!();
}

// =============================================================================
// Horizontal layout (left-to-right) — DEFAULT
// =============================================================================

#[allow(clippy::too_many_lines, clippy::needless_range_loop)]
fn print_horizontal(dag: &Dag, layers: &[Vec<&TaskId>]) {
    let box_width = 20;
    let h_gap = 5; // gap between layer columns (for arrows)
    let row_height = 4; // lines per task box (top, name, attr, bottom)
    let v_gap = 1; // blank lines between tasks in same layer

    // Max tasks in any layer determines grid height
    let max_rows = layers.iter().map(Vec::len).max().unwrap_or(1);
    let total_rows = max_rows * (row_height + v_gap);

    // Build a 2D character grid
    let total_cols = layers.len() * (box_width + h_gap);
    let mut grid: Vec<Vec<char>> = vec![vec![' '; total_cols]; total_rows];

    // Track center positions for connectors: (layer_idx, task_idx) -> (col, row)
    let mut centers: HashMap<&TaskId, (usize, usize)> = HashMap::new();

    for (layer_idx, layer) in layers.iter().enumerate() {
        let col_start = layer_idx * (box_width + h_gap);
        let layer_size = layer.len();

        // Vertically center tasks within the grid
        let total_task_height = layer_size * row_height + (layer_size.saturating_sub(1)) * v_gap;
        let row_offset = (total_rows.saturating_sub(total_task_height)) / 2;

        for (task_idx, task_id) in layer.iter().enumerate() {
            let row_start = row_offset + task_idx * (row_height + v_gap);
            // Draw box
            draw_box(&mut grid, row_start, col_start, box_width, &task_id.0, &label_attr(dag, task_id));

            // Center point for connectors (right edge for outgoing, left edge for incoming)
            let center_row = row_start + row_height / 2;
            centers.insert(task_id, (col_start, center_row));
        }
    }

    // Draw connectors (horizontal arrows between layers)
    for (layer_idx, layer) in layers.iter().enumerate() {
        if layer_idx == 0 {
            continue;
        }

        for task_id in layer {
            let upstream = dag.upstream_of(task_id);
            let &(dest_col, dest_row) = centers.get(task_id).unwrap();

            for up_id in &upstream {
                if let Some(&(src_col, src_row)) = centers.get(up_id) {
                    let arrow_start = src_col + box_width;
                    let arrow_end = dest_col;

                    if src_row == dest_row {
                        // Straight horizontal arrow
                        for c in (arrow_start + 1)..arrow_end {
                            if grid[src_row][c] == ' ' {
                                grid[src_row][c] = '─';
                            }
                        }
                        if arrow_end > 0 {
                            grid[src_row][arrow_end - 1] = '▶';
                        }
                    } else {
                        // L-shaped or Z-shaped connector
                        let mid_col = arrow_start + (arrow_end - arrow_start) / 2;

                        // Horizontal from source to mid
                        for c in (arrow_start + 1)..=mid_col {
                            if grid[src_row][c] == ' ' {
                                grid[src_row][c] = '─';
                            }
                        }

                        // Vertical from src_row to dest_row at mid_col
                        let (top, bot) = if src_row < dest_row {
                            (src_row, dest_row)
                        } else {
                            (dest_row, src_row)
                        };
                        for r in top..=bot {
                            if grid[r][mid_col] == '─' {
                                grid[r][mid_col] = '┼';
                            } else if grid[r][mid_col] == ' ' {
                                grid[r][mid_col] = '│';
                            }
                        }
                        // Corner at source
                        grid[src_row][mid_col] = if src_row < dest_row { '┐' } else { '┘' };
                        // Corner at dest
                        grid[dest_row][mid_col] = if src_row < dest_row { '└' } else { '┌' };

                        // Horizontal from mid to dest
                        for c in (mid_col + 1)..arrow_end {
                            if grid[dest_row][c] == ' ' {
                                grid[dest_row][c] = '─';
                            }
                        }
                        if arrow_end > 0 {
                            grid[dest_row][arrow_end - 1] = '▶';
                        }
                    }
                }
            }
        }
    }

    // Print grid
    for row in &grid {
        let line: String = row.iter().collect();
        let trimmed = line.trim_end();
        if !trimmed.is_empty() {
            println!("{trimmed}");
        }
    }
}

fn label_attr(dag: &Dag, task_id: &TaskId) -> String {
    let task = dag.get_task(task_id).unwrap();
    let mut attrs = Vec::new();
    if task.trigger_rule != TriggerRule::AllSuccess {
        attrs.push(format!("{:?}", task.trigger_rule));
    }
    if task.retries > 0 {
        attrs.push(format!("retries={}", task.retries));
    }
    attrs.join(" ")
}

fn draw_box(grid: &mut [Vec<char>], row: usize, col: usize, width: usize, name: &str, attr: &str) {
    let inner = width - 2; // space inside │...│

    // Top: ┌────────────────────┐
    set_str(grid, row, col, &format!("┌{}┐", "─".repeat(inner)));

    // Name: │    task_name       │
    let name_trunc: String = name.chars().take(inner).collect();
    let pad = inner.saturating_sub(name_trunc.len());
    let lpad = pad / 2;
    let rpad = pad - lpad;
    set_str(grid, row + 1, col, &format!("│{}{}{}│", " ".repeat(lpad), name_trunc, " ".repeat(rpad)));

    // Attr: │    attr_str        │
    let attr_trunc: String = attr.chars().take(inner).collect();
    let attr_space = inner.saturating_sub(attr_trunc.len());
    let attr_left_pad = attr_space / 2;
    let attr_right_pad = attr_space - attr_left_pad;
    set_str(grid, row + 2, col, &format!("│{}{}{}│", " ".repeat(attr_left_pad), attr_trunc, " ".repeat(attr_right_pad)));

    // Bottom: └────────────────────┘
    set_str(grid, row + 3, col, &format!("└{}┘", "─".repeat(inner)));
}

fn set_str(grid: &mut [Vec<char>], row: usize, col: usize, s: &str) {
    for (i, ch) in s.chars().enumerate() {
        if row < grid.len() && col + i < grid[row].len() {
            grid[row][col + i] = ch;
        }
    }
}

// =============================================================================
// Vertical layout (top-to-bottom) — with --vertical flag
// =============================================================================

#[allow(clippy::too_many_lines)]
fn print_vertical(dag: &Dag, layers: &[Vec<&TaskId>]) {
    let col_width = 22;
    let max_cols = layers.iter().map(Vec::len).max().unwrap_or(1);
    let total_width = max_cols * col_width;

    for (layer_idx, layer) in layers.iter().enumerate() {
        // Draw connectors from previous layer
        if layer_idx > 0 {
            let prev = &layers[layer_idx - 1];
            let mut connector_line = vec![' '; total_width];
            let mut arrow_line = vec![' '; total_width];

            for task_id in layer {
                let upstream = dag.upstream_of(task_id);
                let task_col_idx = layer.iter().position(|t| t == task_id).unwrap();
                let task_center = vcenter(task_col_idx, layer.len(), total_width);

                for up_id in &upstream {
                    if let Some(up_col_idx) = prev.iter().position(|t| *t == up_id) {
                        let up_center = vcenter(up_col_idx, prev.len(), total_width);
                        let (left, right) = if up_center <= task_center {
                            (up_center, task_center)
                        } else {
                            (task_center, up_center)
                        };

                        if up_center == task_center {
                            if connector_line[task_center] == ' ' {
                                connector_line[task_center] = '│';
                            }
                        } else {
                            for ch in &mut connector_line[left..=right] {
                                if *ch == ' ' { *ch = '─'; }
                                else if *ch == '│' { *ch = '┼'; }
                            }
                            connector_line[up_center] = if connector_line[up_center] == '─' { '┴' } else { '│' };
                            connector_line[task_center] = if connector_line[task_center] == '─' { '┬' } else { '│' };
                        }
                    }
                }

                if arrow_line[task_center] == ' ' {
                    arrow_line[task_center] = '▼';
                }
            }

            let conn_str: String = connector_line.iter().collect();
            let arrow_str: String = arrow_line.iter().collect();
            println!("{}", conn_str.trim_end());
            println!("{}", arrow_str.trim_end());
        }

        // Draw task boxes
        let box_width = 18;
        let mut line1 = vec![' '; total_width];
        let mut line2 = vec![' '; total_width];
        let mut line3 = vec![' '; total_width];
        let mut line4 = vec![' '; total_width];

        for (col_idx, task_id) in layer.iter().enumerate() {
            let task = dag.get_task(task_id).unwrap();
            let center = vcenter(col_idx, layer.len(), total_width);
            let left = center.saturating_sub(box_width / 2);

            let top = format!("┌{}┐", "─".repeat(box_width));
            for (i, ch) in top.chars().enumerate() {
                if left + i < total_width { line1[left + i] = ch; }
            }

            let name = &task_id.0;
            let name_pad = box_width.saturating_sub(name.len());
            let name_left = name_pad / 2;
            let name_right = name_pad - name_left;
            let mid = format!("│{}{}{}│", " ".repeat(name_left), name, " ".repeat(name_right));
            for (i, ch) in mid.chars().enumerate() {
                if left + i < total_width { line2[left + i] = ch; }
            }

            let mut attrs = Vec::new();
            if task.trigger_rule != TriggerRule::AllSuccess {
                attrs.push(format!("{:?}", task.trigger_rule));
            }
            if task.retries > 0 {
                attrs.push(format!("retries={}", task.retries));
            }
            let attr_str = attrs.join(" ");
            let attr_pad = box_width.saturating_sub(attr_str.len());
            let attr_left = attr_pad / 2;
            let attr_right = attr_pad - attr_left;
            let attr_line = format!("│{}{}{}│", " ".repeat(attr_left), attr_str, " ".repeat(attr_right));
            for (i, ch) in attr_line.chars().enumerate() {
                if left + i < total_width { line3[left + i] = ch; }
            }

            let bot = format!("└{}┘", "─".repeat(box_width));
            for (i, ch) in bot.chars().enumerate() {
                if left + i < total_width { line4[left + i] = ch; }
            }
        }

        let s1: String = line1.iter().collect();
        let s2: String = line2.iter().collect();
        let s3: String = line3.iter().collect();
        let s4: String = line4.iter().collect();
        println!("{}", s1.trim_end());
        println!("{}", s2.trim_end());
        println!("{}", s3.trim_end());
        println!("{}", s4.trim_end());
    }
}

const fn vcenter(col_idx: usize, num_cols: usize, total_width: usize) -> usize {
    if num_cols == 1 {
        total_width / 2
    } else {
        let spacing = total_width / num_cols;
        spacing / 2 + col_idx * spacing
    }
}

// =============================================================================
// Helpers
// =============================================================================

const fn state_color(state: TaskState) -> Color {
    match state {
        TaskState::Scheduled | TaskState::Queued => Color::Gray,
        TaskState::Running => Color::Blue,
        TaskState::Success => Color::Green,
        TaskState::Failed => Color::Red,
        TaskState::Skipped => Color::Magenta,
        TaskState::UpstreamFailed => Color::LightRed,
        TaskState::UpForRetry => Color::Yellow,
        TaskState::None | TaskState::Removed => Color::DarkGray,
    }
}

const fn state_icon(state: TaskState) -> &'static str {
    match state {
        TaskState::None => "○",
        TaskState::Scheduled => "◔",
        TaskState::Queued => "◑",
        TaskState::Running => "◉",
        TaskState::Success => "●",
        TaskState::Failed => "✗",
        TaskState::Skipped => "⊘",
        TaskState::UpstreamFailed => "⊗",
        TaskState::UpForRetry => "↻",
        TaskState::Removed => "⊖",
    }
}
