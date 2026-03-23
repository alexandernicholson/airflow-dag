use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ironpipe::{TaskContext, TaskExecutor};

/// Downloads a month of NYC taxi trip data (CSV summary from TLC).
/// Stores row count and sample data in XCom.
pub struct DownloadExecutor {
    pub month: String,
    pub year: String,
}

#[async_trait::async_trait]
impl TaskExecutor for DownloadExecutor {
    async fn execute(
        &self,
        ctx: &mut TaskContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}-{}.parquet",
            self.year, self.month
        );

        println!(
            "[node {}] Downloading {} data: {}",
            ctx.task_id,
            self.month,
            url
        );

        // Do a HEAD request to get file size (avoid downloading GBs in a demo)
        let client = reqwest::Client::new();
        let resp = client.head(&url).send().await?;
        let file_size = resp
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);

        // Simulate processing time proportional to file size
        let simulated_rows = file_size / 200; // ~200 bytes per row estimate
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        ctx.xcom_push(
            "return_value",
            serde_json::json!({
                "month": self.month,
                "year": self.year,
                "file_size_bytes": file_size,
                "estimated_rows": simulated_rows,
                "url": url,
                "status": "downloaded"
            }),
        );

        println!(
            "[node {}] Downloaded {}: {:.1} MB, ~{} rows",
            ctx.task_id,
            self.month,
            file_size as f64 / 1_048_576.0,
            simulated_rows
        );

        Ok(())
    }
}

/// Cleans/validates taxi trip records.
/// Simulates filtering invalid records and normalizing data.
pub struct CleanExecutor {
    pub month: String,
}

#[async_trait::async_trait]
impl TaskExecutor for CleanExecutor {
    async fn execute(
        &self,
        ctx: &mut TaskContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!(
            "[node {}] Cleaning {} data...",
            ctx.task_id,
            self.month
        );

        // Simulate cleaning: 95% of rows pass validation
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        let clean_ratio = 0.95;
        ctx.xcom_push(
            "return_value",
            serde_json::json!({
                "month": self.month,
                "clean_ratio": clean_ratio,
                "status": "cleaned",
                "filters_applied": [
                    "removed_zero_fare",
                    "removed_zero_distance",
                    "removed_future_dates",
                    "normalized_payment_type"
                ]
            }),
        );

        println!(
            "[node {}] Cleaned {}: {:.0}% rows passed validation",
            ctx.task_id,
            self.month,
            clean_ratio * 100.0
        );

        Ok(())
    }
}

/// Aggregates cleaned data from all months.
pub struct AggregateExecutor {
    pub total_tasks: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl TaskExecutor for AggregateExecutor {
    async fn execute(
        &self,
        ctx: &mut TaskContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!(
            "[node {}] Aggregating all months...",
            ctx.task_id,
        );

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let tasks_completed = self.total_tasks.load(Ordering::SeqCst);

        ctx.xcom_push(
            "return_value",
            serde_json::json!({
                "status": "aggregated",
                "months_processed": 4,
                "tasks_completed": tasks_completed,
                "stats": {
                    "avg_fare": 18.52,
                    "avg_trip_distance_miles": 3.2,
                    "total_estimated_trips": 12_500_000,
                    "top_pickup_zones": [
                        "JFK Airport",
                        "LaGuardia Airport",
                        "Midtown Manhattan",
                        "Upper East Side",
                        "Times Square"
                    ],
                    "trips_by_hour": {
                        "morning_rush_7_9": "18%",
                        "midday_10_15": "25%",
                        "evening_rush_16_19": "22%",
                        "night_20_6": "35%"
                    }
                }
            }),
        );

        println!(
            "[node {}] Aggregation complete: {} months, {} tasks",
            ctx.task_id,
            4,
            tasks_completed
        );

        Ok(())
    }
}

/// Generates a final report from aggregated stats.
pub struct ReportExecutor;

#[async_trait::async_trait]
impl TaskExecutor for ReportExecutor {
    async fn execute(
        &self,
        ctx: &mut TaskContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!(
            "[node {}] Generating report...",
            ctx.task_id,
        );

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        ctx.xcom_push(
            "return_value",
            serde_json::json!({
                "status": "report_generated",
                "format": "json",
                "output_path": "/data/output/nyc_taxi_report.json"
            }),
        );

        println!(
            "[node {}] Report generated successfully",
            ctx.task_id,
        );

        Ok(())
    }
}
