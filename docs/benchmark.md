# Benchmark: Image Processing Pipeline

A real-world benchmark demonstrating ironpipe's distributed DAG execution. Downloads 50 images, resizes each to 6 sizes using Lanczos3 resampling, computes perceptual hashes, deduplicates, and stitches a contact sheet PNG.

## Results

Docker containers with CPU limits:

| Mode | CPUs | Time | Speedup |
|------|------|------|---------|
| Single (1 runtime) | 1 | 27.25s | 1.0x |
| Distributed (5 runtimes) | 4 | 7.69s | **3.5x** |

## DAG

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│    download_1    │     │     resize_1     │     │      hash_1      │
│    retries=1     │ ───▶│                  │ ───▶│                  │ ─┐
└──────────────────┘     └──────────────────┘     └──────────────────┘  │
                                                                        │
│    download_2    │ ───▶│     resize_2     │ ───▶│      hash_2      │ ─┐
                                                                        │
│    download_3    │ ───▶│     resize_3     │ ───▶│      hash_3      │ ─┼─▶│ dedup │───▶│ contact_sheet │───▶│ report │
                                                                        │
│    download_4    │ ───▶│     resize_4     │ ───▶│      hash_4      │ ─┘
                                                                        │
│    download_5    │ ───▶│     resize_5     │ ───▶│      hash_5      │ ─┘
```

18 tasks. 5 parallel branches fan into a dedup → contact sheet → report serial tail.

## What Each Task Does

| Task | Work | CPU-bound? |
|------|------|------------|
| `download_N` | Fetch 10 images (2000x1500 JPEG) from Lorem Picsum | I/O |
| `resize_N` | Resize each image to 6 sizes (64, 150, 320, 640, 1200, 1920px) using Lanczos3 | **Yes** |
| `hash_N` | Compute average perceptual hash (downscale to 8x8 grayscale, threshold) | Light |
| `dedup` | Re-hash all 50 thumbnails, find duplicate hash groups | Light |
| `contact_sheet` | Stitch all 50 thumbnails into a single grid PNG | Moderate |
| `report` | Write summary | Trivial |

The bottleneck is `resize_N` — Lanczos3 is the most expensive resampling filter, and each batch does 60 resize operations (10 images x 6 sizes) on 2000x1500 source images.

## Running

### Native (no CPU limits, uses all cores)

```bash
cd examples/benchmark

# Single runtime
cargo run -- single

# 5 runtimes (distributed)
cargo run -- distributed
```

Both modes complete in similar time natively because Tokio's work-stealing thread pool uses all available cores regardless. The difference shows under CPU constraints.

### Docker (with CPU limits)

```bash
# Build
docker build -f examples/benchmark/Dockerfile -t ironpipe-benchmark .

# Single: 1 CPU core
docker run --rm --cpus=1.0 --memory=1g ironpipe-benchmark single

# Distributed: 4 CPU cores
docker run --rm --cpus=4.0 --memory=2g ironpipe-benchmark distributed
```

Or using compose:

```bash
cd examples/benchmark
docker compose run single
docker compose run distributed
```

## Why It Scales

The scheduler dynamically dispatches ready tasks — no static assignment of tasks to workers. When `download_1` finishes, `resize_1` becomes ready and gets picked up by whichever Tokio worker thread is free. With 4 CPUs, up to 4 resize batches run truly in parallel.

The serial tail (dedup → contact sheet → report) runs after all 5 branches complete, limiting theoretical max speedup to ~4x by Amdahl's law. The measured 3.5x accounts for this plus download I/O overlap.

## Output

The pipeline produces `output/contact_sheet.png` — a grid of all 50 thumbnails:

```
output/
  batch_1/
    img_000.jpg          (original 2000x1500)
    img_000_64px.jpg     (resized)
    img_000_150px.jpg
    img_000_320px.jpg
    img_000_640px.jpg
    img_000_1200px.jpg
    img_000_1920px.jpg
    ...
  batch_2/
    ...
  contact_sheet.png      (grid of all 50 thumbnails)
```
