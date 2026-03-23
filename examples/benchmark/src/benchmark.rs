use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use image::{DynamicImage, GenericImageView, ImageBuffer, Rgba, imageops::FilterType};
use ironpipe::{Dag, Task, TaskContext, TaskExecutor, TaskId, TriggerRule};

const BATCHES: usize = 5;
const IMAGES_PER_BATCH: usize = 10;
const TOTAL_IMAGES: usize = BATCHES * IMAGES_PER_BATCH;
const RESIZE_SIZES: [u32; 6] = [64, 150, 320, 640, 1200, 1920];

fn output_dir() -> PathBuf {
    let dir = PathBuf::from("output");
    std::fs::create_dir_all(&dir).ok();
    dir
}

fn batch_dir(batch: usize) -> PathBuf {
    let dir = output_dir().join(format!("batch_{batch}"));
    std::fs::create_dir_all(&dir).ok();
    dir
}

// ─── Download Executor ──────────────────────────────────────────────────────

/// Downloads IMAGES_PER_BATCH images from Lorem Picsum (real JPEGs).
pub struct DownloadBatchExecutor {
    pub batch: usize,
}

#[async_trait::async_trait]
impl TaskExecutor for DownloadBatchExecutor {
    async fn execute(
        &self,
        ctx: &mut TaskContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        let client = reqwest::Client::new();
        let dir = batch_dir(self.batch);
        let mut sizes = Vec::new();

        for i in 0..IMAGES_PER_BATCH {
            let image_id = self.batch * IMAGES_PER_BATCH + i + 1;
            let url = format!("https://picsum.photos/id/{image_id}/2000/1500");
            let path = dir.join(format!("img_{i:03}.jpg"));

            let resp = client.get(&url).send().await?;
            let bytes = resp.bytes().await?;
            std::fs::write(&path, &bytes)?;
            sizes.push(bytes.len());
        }

        let total_bytes: usize = sizes.iter().sum();
        let elapsed = start.elapsed();
        println!(
            "  [{}] Downloaded {} images ({:.1} KB total) in {:.1}s",
            ctx.task_id,
            IMAGES_PER_BATCH,
            total_bytes as f64 / 1024.0,
            elapsed.as_secs_f64()
        );
        ctx.xcom_push(
            "return_value",
            serde_json::json!({
                "batch": self.batch,
                "images": IMAGES_PER_BATCH,
                "total_bytes": total_bytes,
                "ms": elapsed.as_millis(),
            }),
        );
        Ok(())
    }
}

// ─── Resize Executor ────────────────────────────────────────────────────────

/// Resizes each image to 6 sizes using Lanczos3 resampling.
/// 2000x1500 source images × 6 target sizes × 10 images = 60 Lanczos3 operations.
/// This is genuinely CPU-heavy — Lanczos3 is the most expensive resampling filter.
pub struct ResizeBatchExecutor {
    pub batch: usize,
}

#[async_trait::async_trait]
impl TaskExecutor for ResizeBatchExecutor {
    async fn execute(
        &self,
        ctx: &mut TaskContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        let dir = batch_dir(self.batch);
        let mut resize_count = 0u32;

        for i in 0..IMAGES_PER_BATCH {
            let src_path = dir.join(format!("img_{i:03}.jpg"));
            let img = image::open(&src_path)?;

            for &size in &RESIZE_SIZES {
                let resized = img.resize(size, size, FilterType::Lanczos3);
                resized.save(dir.join(format!("img_{i:03}_{size}px.jpg")))?;
                resize_count += 1;
            }
        }

        let elapsed = start.elapsed();
        println!(
            "  [{}] Resized {} images → {} variants (Lanczos3, 6 sizes) in {:.1}s",
            ctx.task_id, IMAGES_PER_BATCH, resize_count, elapsed.as_secs_f64()
        );
        ctx.xcom_push(
            "return_value",
            serde_json::json!({
                "batch": self.batch,
                "resizes": resize_count,
                "ms": elapsed.as_millis(),
            }),
        );
        Ok(())
    }
}

// ─── Hash Executor ──────────────────────────────────────────────────────────

/// Compute a perceptual hash (average hash) for each thumbnail.
/// Downscale to 8x8 grayscale, threshold against mean.
pub struct HashBatchExecutor {
    pub batch: usize,
}

fn average_hash(img: &DynamicImage) -> u64 {
    let small = img.resize_exact(8, 8, FilterType::Lanczos3).to_luma8();
    let pixels: Vec<u8> = small.pixels().map(|p| p[0]).collect();
    let mean: u64 = pixels.iter().map(|&p| u64::from(p)).sum::<u64>() / 64;
    let mut hash: u64 = 0;
    for (i, &p) in pixels.iter().enumerate() {
        if u64::from(p) > mean {
            hash |= 1 << i;
        }
    }
    hash
}

#[async_trait::async_trait]
impl TaskExecutor for HashBatchExecutor {
    async fn execute(
        &self,
        ctx: &mut TaskContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();
        let dir = batch_dir(self.batch);
        let mut hashes = Vec::new();

        for i in 0..IMAGES_PER_BATCH {
            let path = dir.join(format!("img_{i:03}_150px.jpg"));
            let img = image::open(&path)?;
            let hash = average_hash(&img);
            hashes.push(hash);
        }

        let elapsed = start.elapsed();
        println!(
            "  [{}] Hashed {} thumbnails in {:.1}s",
            ctx.task_id, hashes.len(), elapsed.as_secs_f64()
        );
        ctx.xcom_push(
            "return_value",
            serde_json::json!({
                "batch": self.batch,
                "hashes": hashes,
                "ms": elapsed.as_millis(),
            }),
        );
        Ok(())
    }
}

// ─── Dedup Executor ─────────────────────────────────────────────────────────

pub struct DedupExecutor;

#[async_trait::async_trait]
impl TaskExecutor for DedupExecutor {
    async fn execute(
        &self,
        ctx: &mut TaskContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();

        // Re-hash all thumbnails to find duplicates
        let mut all_hashes: HashMap<u64, Vec<String>> = HashMap::new();
        for batch in 1..=BATCHES {
            let dir = batch_dir(batch);
            for i in 0..IMAGES_PER_BATCH {
                let path = dir.join(format!("img_{i:03}_150px.jpg"));
                if let Ok(img) = image::open(&path) {
                    let hash = average_hash(&img);
                    all_hashes
                        .entry(hash)
                        .or_default()
                        .push(format!("batch_{batch}/img_{i:03}"));
                }
            }
        }

        let duplicates: Vec<_> = all_hashes
            .values()
            .filter(|v| v.len() > 1)
            .cloned()
            .collect();

        let elapsed = start.elapsed();
        println!(
            "  [{}] Dedup across {} images: {} unique hashes, {} duplicate groups in {:.1}s",
            ctx.task_id, TOTAL_IMAGES, all_hashes.len(), duplicates.len(), elapsed.as_secs_f64()
        );
        ctx.xcom_push(
            "return_value",
            serde_json::json!({
                "total_images": TOTAL_IMAGES,
                "unique_hashes": all_hashes.len(),
                "duplicate_groups": duplicates.len(),
                "ms": elapsed.as_millis(),
            }),
        );
        Ok(())
    }
}

// ─── Contact Sheet Executor ─────────────────────────────────────────────────

/// Stitches all thumbnails into a grid PNG. Produces a real visible output file.
pub struct ContactSheetExecutor;

#[async_trait::async_trait]
impl TaskExecutor for ContactSheetExecutor {
    async fn execute(
        &self,
        ctx: &mut TaskContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start = Instant::now();

        let cols = 10;
        let rows = (TOTAL_IMAGES + cols - 1) / cols;
        let cell = 150u32;
        let padding = 4u32;
        let width = cols as u32 * (cell + padding) + padding;
        let height = rows as u32 * (cell + padding) + padding;

        let mut sheet: ImageBuffer<Rgba<u8>, Vec<u8>> =
            ImageBuffer::from_pixel(width, height, Rgba([30, 30, 30, 255]));

        let mut idx = 0;
        for batch in 1..=BATCHES {
            let dir = batch_dir(batch);
            for i in 0..IMAGES_PER_BATCH {
                let path = dir.join(format!("img_{i:03}_150px.jpg"));
                if let Ok(img) = image::open(&path) {
                    let thumb = img.resize_exact(cell, cell, FilterType::Lanczos3);
                    let col = idx % cols;
                    let row = idx / cols;
                    let x = padding + col as u32 * (cell + padding);
                    let y = padding + row as u32 * (cell + padding);

                    for (px, py, pixel) in thumb.to_rgba8().enumerate_pixels() {
                        if x + px < width && y + py < height {
                            sheet.put_pixel(x + px, y + py, *pixel);
                        }
                    }
                }
                idx += 1;
            }
        }

        let out_path = output_dir().join("contact_sheet.png");
        sheet.save(&out_path)?;

        let file_size = std::fs::metadata(&out_path)?.len();
        let elapsed = start.elapsed();
        println!(
            "  [{}] Contact sheet: {}x{} ({} images, {:.1} KB) in {:.1}s → {}",
            ctx.task_id,
            width,
            height,
            TOTAL_IMAGES,
            file_size as f64 / 1024.0,
            elapsed.as_secs_f64(),
            out_path.display()
        );
        ctx.xcom_push(
            "return_value",
            serde_json::json!({
                "path": out_path.to_string_lossy(),
                "width": width,
                "height": height,
                "images": TOTAL_IMAGES,
                "file_size": file_size,
                "ms": elapsed.as_millis(),
            }),
        );
        Ok(())
    }
}

// ─── Report Executor ────────────────────────────────────────────────────────

pub struct BenchmarkReportExecutor;

#[async_trait::async_trait]
impl TaskExecutor for BenchmarkReportExecutor {
    async fn execute(
        &self,
        ctx: &mut TaskContext,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        ctx.xcom_push("return_value", serde_json::json!({"status": "complete"}));
        println!("  [{}] Final report generated", ctx.task_id);
        Ok(())
    }
}

// ─── DAG builder ────────────────────────────────────────────────────────────

pub fn build_benchmark_dag() -> Dag {
    let mut dag = Dag::new("image_processing_benchmark");

    for b in 1..=BATCHES {
        dag.add_task(Task::builder(format!("download_{b}")).retries(1).build()).unwrap();
        dag.add_task(Task::builder(format!("resize_{b}")).build()).unwrap();
        dag.add_task(Task::builder(format!("hash_{b}")).build()).unwrap();

        dag.chain(&[
            TaskId::new(format!("download_{b}")),
            TaskId::new(format!("resize_{b}")),
            TaskId::new(format!("hash_{b}")),
        ])
        .unwrap();
    }

    dag.add_task(Task::builder("dedup").trigger_rule(TriggerRule::AllSuccess).build()).unwrap();
    dag.add_task(Task::builder("contact_sheet").build()).unwrap();
    dag.add_task(Task::builder("report").build()).unwrap();

    for b in 1..=BATCHES {
        dag.set_downstream(&TaskId::new(format!("hash_{b}")), &TaskId::new("dedup")).unwrap();
    }

    dag.chain(&[
        TaskId::new("dedup"),
        TaskId::new("contact_sheet"),
        TaskId::new("report"),
    ])
    .unwrap();

    dag
}

pub fn build_benchmark_executors() -> HashMap<TaskId, Arc<dyn TaskExecutor>> {
    let mut executors: HashMap<TaskId, Arc<dyn TaskExecutor>> = HashMap::new();

    for b in 1..=BATCHES {
        executors.insert(
            TaskId::new(format!("download_{b}")),
            Arc::new(DownloadBatchExecutor { batch: b }),
        );
        executors.insert(
            TaskId::new(format!("resize_{b}")),
            Arc::new(ResizeBatchExecutor { batch: b }),
        );
        executors.insert(
            TaskId::new(format!("hash_{b}")),
            Arc::new(HashBatchExecutor { batch: b }),
        );
    }

    executors.insert(TaskId::new("dedup"), Arc::new(DedupExecutor));
    executors.insert(TaskId::new("contact_sheet"), Arc::new(ContactSheetExecutor));
    executors.insert(TaskId::new("report"), Arc::new(BenchmarkReportExecutor));

    executors
}
