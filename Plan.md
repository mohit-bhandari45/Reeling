# video-service — Architecture & Build Plan

An async video processing service written in Go. This document is the full
build plan: architecture, every component in detail, and the order to build
them in. Written to be built piece by piece, MVP first, scaling out later
without rewrites.

---

## 1. High-level architecture

```
                         ┌────────────────┐
Client ─── upload ──────▶│  Load balancer  │
                         └───────┬─────────┘
                                 │
                    ┌────────────▼────────────┐
                    │  API servers (stateless)  │◀──────────────┐
                    └────────────┬────────────┘                │
                                 │ enqueue job                  │ status
                                 ▼                              │ updates
                    ┌────────────────────────┐                 │
                    │     Durable queue          │                 │
                    │  (channel → Kafka/SQS/     │                 │
                    │   NATS JetStream)           │                 │
                    └────────────┬────────────┘                 │
                                 │ pull job                     │
                                 ▼                              │
                    ┌────────────────────────┐                 │
                    │      Worker pool           │─────────────────┘
                    │  (autoscaled goroutines/   │
                    │   pods)                     │
                    └────────────┬────────────┘
                                 │ run
                                 ▼
                    ┌────────────────────────┐
                    │        FFmpeg              │
                    │ transcode / thumbnail /    │
                    │ multi-rendition             │
                    └────────────┬────────────┘
                                 │ write output
                                 ▼
                    ┌────────────────────────┐        ┌───────────────┐
                    │   Object storage           │──────▶│ CDN + webhooks │
                    │   (local disk → S3/MinIO)  │        └───────────────┘
                    └────────────────────────┘

                    ┌────────────────────────┐
                    │       Postgres              │◀── job/video metadata,
                    │  (job status, metadata)    │    read/written by API + workers
                    └────────────────────────┘
```

**Why it's shaped this way:**
- API servers hold no state, so any number of replicas can sit behind a
  load balancer.
- The queue decouples "accepting an upload" from "doing the CPU-heavy
  work," so a slow transcode never blocks incoming requests.
- Workers scale independently of the API layer, since transcoding load is
  bursty and CPU/GPU-bound while API traffic tends to be steadier.
- Job metadata lives in Postgres, not in memory, so state survives
  restarts and is visible to every API/worker instance at once.

---

## 2. Build phases

| Phase | Goal | What's added |
|---|---|---|
| **Phase 1 — MVP** | One video through the full pipeline | In-memory queue, local disk storage, in-memory job store, single binary |
| **Phase 2 — Durability** | Survive restarts, run >1 instance | Postgres for jobs, S3/MinIO for files, real queue (Kafka/SQS/NATS) |
| **Phase 3 — Scale out** | Handle real volume | Containerize, separate API/worker deployments, autoscale workers on queue depth |
| **Phase 4 — Production hardening** | Safe to expose to real users | Auth, retries, idempotency, structured logging/metrics, webhooks |
| **Phase 5 — Advanced** | "Very big" territory | Multi-rendition/HLS output, GPU-accelerated transcoding, CDN integration, live streaming |

Each phase below is broken into concrete steps. Build in order — each step
depends on the interfaces defined in earlier ones, not their concrete
implementations, so later swaps (e.g. local disk → S3) touch one file, not
the whole codebase.

---

## 3. Phase 1 — MVP

**Approach: top-down** Get a bare server running first,
then add exactly one real endpoint, and only build the supporting
packages (`job`, `storage`, `ffmpeg`, `worker`) at the moment an endpoint
actually needs them. This avoids building abstractions in isolation
before there's a real caller to prove they're shaped correctly.

Each step below ends with something you can actually run and hit with
`curl` — no step is "just types with nothing to test."

### Step 1.1 — Bare server
- `cmd/server/main.go` with a plain `net/http` server and a single
  `/health` route returning `"ok"`.
- No other packages exist yet.
- **Check:** `go run ./cmd/server`, then `curl localhost:8080/health` →
  `ok`.

### Step 1.2 — Upload endpoint (stubbed)
- Add `POST /videos` directly in `main.go` (or a new `internal/api`
  package once it stops being trivial). At this point it can just accept
  the multipart file and return a fake job ID — no real processing yet.
- This is where you first decide the request/response shape (what the
  client sends, what JSON comes back), before building anything underneath
  it.
- **Check:** `curl -F "video=@sample.mp4" localhost:8080/videos` returns
  something like `{"job_id": "..."}`.

### Step 1.3 — Real file storage (`internal/storage`)
- The upload handler needs somewhere real to put the uploaded file — this
  is the trigger to build `internal/storage` now, not before.
- `Storage` interface: `Save(name string, r io.Reader) (key string, err
  error)`, `Open(key string) (io.ReadCloser, error)`.
- `LocalDisk` implementation: writes to a base directory, keys are
  UUID-prefixed filenames.
- Wire it into the upload handler from Step 1.2: it now actually saves the
  file and returns a real key.
- **Check:** upload a file, confirm it actually appears on disk under the
  configured directory.

### Step 1.4 — Job tracking (`internal/job`)
- The client needs to check progress after uploading — that requirement
  is what justifies building this now.
- `Status` enum: `queued`, `processing`, `done`, `failed`.
- `Job` struct: `ID`, `InputKey`, `OutputKeys`, `Status`, `Error`,
  `CreatedAt`, `UpdatedAt`.
- `Store` interface + in-memory implementation (`map[string]*Job` guarded
  by a mutex).
- Upload handler now creates a real `Job` (status `queued`) instead of a
  fake ID.
- Add `GET /jobs/{id}` returning the job's current status.
- **Check:** upload a file, then `curl localhost:8080/jobs/{id}` shows
  `"status": "queued"`.

### Step 1.5 — FFmpeg wrapper (`internal/ffmpeg`)
- Nothing processes the video yet — jobs just sit at `queued` forever.
  This step adds the ability to actually transcode, tested completely on
  its own before it's wired into anything concurrent.
- Shell out via `os/exec` (not a Go binding) — full control over flags,
  and it's what the bindings do internally anyway.
- `Transcode(ctx, input, output, preset string) error` — libx264 + aac.
- `Thumbnail(ctx, input, output string, atSeconds int) error` — single
  frame extraction.
- **Check:** call `Transcode` directly from a throwaway test against a
  real sample file — confirm the output plays. Don't wire it into the
  server yet.

### Step 1.6 — Worker pool (`internal/worker`)
- Now that storage, job tracking, and FFmpeg all exist, this is the piece
  that connects them: pull a job → mark `processing` → call FFmpeg → mark
  `done`/`failed` → save.
- Fixed-size pool of goroutines reading from a buffered channel of
  `*Job`. Pool size starts at `runtime.NumCPU()` since transcoding is
  CPU-bound.
- Upload handler now enqueues the job into the pool instead of leaving it
  at `queued` forever.
- This is where concurrency bugs like to hide (double-processing, races on
  job state) — keep the worker function small and push all shared-state
  access through the `Store` interface.
- **Check:** upload a file, poll `GET /jobs/{id}` and watch status move
  `queued` → `processing` → `done`, and confirm the transcoded output
  exists in storage.

### Step 1.7 — Consolidate into `internal/api`
- By now `main.go` has grown a few inline handlers — move them into
  `internal/api` as the routes stop being trivial, so `main.go` goes back
  to just wiring dependencies together and starting the server.

### Step 1.8 — End-to-end check
- Fresh run: upload one real video with `curl`, poll status until `done`,
  confirm the output file exists in local storage. This is the MVP
  checkpoint — everything after this is about durability and scale, not
  new pipeline behavior.

---

## 4. Phase 2 — Durability

### Step 2.1 — Postgres job store
Implement `Store` against Postgres instead of memory. Suggested schema:
```sql
CREATE TABLE jobs (
  id           UUID PRIMARY KEY,
  input_key    TEXT NOT NULL,
  output_keys  TEXT[],
  status       TEXT NOT NULL,
  error        TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```
Because `Store` was already an interface, this is a drop-in swap in
`main.go` — no changes to the API or worker code.

### Step 2.2 — S3-compatible storage
Implement `Storage` against S3 (or MinIO for local/self-hosted testing).
Same idea — the interface hides the swap from every caller.

### Step 2.3 — Real message queue
Replace the in-process channel with Kafka, SQS, or NATS JetStream.
Decision guide:
- **SQS** — least ops overhead if you're already on AWS
- **NATS JetStream** — lightweight to self-host, good middle ground
- **Kafka** — highest throughput and replay capability, most ops overhead

Whichever you choose, keep an `Enqueue(job)` / `Consume() <-chan Job`
abstraction so workers don't care which backend is underneath.

---

## 5. Phase 3 — Scale out

### Step 3.1 — Containerize
Dockerfile for the API and a separate one (or a build target) for the
worker, since they'll scale differently.

### Step 3.2 — Split deployments
Deploy API and worker as separate Kubernetes Deployments. API stays
stateless and scales on request volume; workers scale on queue depth.

### Step 3.3 — Autoscaling
Kubernetes HPA on a custom metric (queue depth) or KEDA, which has
built-in scalers for SQS/Kafka/NATS queue length. This is what lets the
system go from 2 worker pods at idle to 200 during a load spike without
manual intervention.

### Step 3.4 — Local dev environment
`docker-compose.yml` with Postgres, MinIO, and your chosen queue backend,
so the full stack runs locally without touching real cloud infra.

---

## 6. Phase 4 — Production hardening

### Step 4.1 — Idempotency
Design job processing so re-running a job (after a crash mid-transcode)
is always safe — e.g. check if output already exists before re-writing,
or use a job "attempt" counter.

### Step 4.2 — Retry logic
Failed jobs get requeued with backoff, up to a max attempt count, then
moved to a dead-letter state for manual inspection.

### Step 4.3 — Auth
API keys or OAuth on the upload/status endpoints, depending on who's
calling this service.

### Step 4.4 — Observability
Structured logging (e.g. `slog`), Prometheus metrics (queue depth, job
duration, failure rate), and tracing across API → queue → worker.

### Step 4.5 — Webhooks
Notify a client-provided URL on job completion instead of requiring them
to poll `GET /jobs/{id}` forever.

---

## 7. Phase 5 — Advanced / "very big"

### Step 5.1 — Multi-rendition output
One input video produces multiple resolutions/bitrates in a single job
(e.g. 1080p, 720p, 480p). Requires the job schema to support multiple
outputs from the start — retrofit this later and it touches almost
everything, so plan for it even if you don't build it immediately.

### Step 5.2 — HLS/DASH packaging
Generate `.m3u8`/`.mpd` manifests referencing the multi-rendition outputs,
for adaptive bitrate streaming.

### Step 5.3 — GPU-accelerated transcoding
Swap `-c:v libx264` for `-c:v h264_nvenc` (NVIDIA) or VAAPI, 5-10x faster
for high volume. Requires GPU-enabled worker nodes.

### Step 5.4 — CDN integration
Put a CDN in front of the object storage bucket so playback traffic never
hits storage directly.

### Step 5.5 — Live streaming (if needed)
A meaningfully different pipeline (RTMP/WebRTC ingest, low-latency
segment generation) — treat this as a separate service sharing the same
storage/CDN layer rather than bolting it onto the VOD pipeline.

---

## 8. Feature checklist

**Core pipeline** — async upload → queue → transcode → deliver, stateless
API, durable queue, autoscaled workers, Postgres metadata, S3-compatible
storage

**Processing** — FFmpeg transcoding, thumbnails, optional GPU
acceleration, multi-rendition output

**Reliability** — idempotent processing, retry logic, full job status
tracking

**Delivery** — CDN, webhook notifications

**Not included by default** — live streaming, content moderation/virus
scanning (add explicitly if needed)

---

## 9. Decisions worth locking in early

- **Queue choice** — shapes a lot of the operational surface later. NATS
  JetStream or SQS are the least painful starting points if unsure.
- **Job schema** — support "one input → many outputs" from the start if
  adaptive bitrate streaming is ever on the roadmap; retrofitting this is
  expensive.
- **Idempotency** — assume workers will crash mid-job and jobs will get
  retried; design for safety from Step 1.6 onward, not as an afterthought
  in Phase 4.

---

## 10. Getting started

Requirements: Go 1.22+, FFmpeg on `PATH`.

```bash
go mod tidy
go run ./cmd/server
```

Build in the order laid out in Phase 1 above — bare server, upload
endpoint, storage, job tracking, FFmpeg wrapper, worker pool, then
consolidate into `internal/api` and do the end-to-end check.