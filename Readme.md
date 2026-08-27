# video-service

An async video processing service written in Go. Clients upload a video, the
service queues a job, a pool of workers runs FFmpeg against it, and the
result lands back in object storage. Built to start small (single binary,
in-memory queue) and scale out (durable queue, autoscaled workers,
Kubernetes) without a rewrite in between.

## How it works

1. Client uploads a file to the API.
2. The API stores the raw file and enqueues a job.
3. A worker pool pulls jobs off the queue and runs FFmpeg (transcode,
   thumbnail generation, etc).
4. Output files are written back to storage; job status is updated
   throughout (`queued` → `processing` → `done`/`failed`).
5. Clients poll the status endpoint, or get notified via webhook once
   that's wired up.

## Architecture

```
Client → Load balancer → API servers (stateless) → Durable queue → Worker pool → FFmpeg
                              │                                        │
                              └──────────► Postgres (job/metadata)     │
                                                                        │
                                     Object storage (S3/MinIO) ◄───────┘
                                              │
                                         CDN + webhooks
```

- **API servers** are stateless — no local state, so you can run any
  number of replicas behind a load balancer.
- **Job/metadata** lives in Postgres, not in memory, so it survives
  restarts and works across multiple API instances.
- **Queue** decouples the API from processing. Start with an in-process
  channel; move to Kafka, SQS, or NATS JetStream once you need
  durability or multiple consumers.
- **Worker pool** scales independently of the API layer, since
  transcoding is CPU/GPU-bound and bursty while API traffic tends to be
  steadier. Can autoscale on queue depth.
- **Object storage** holds raw uploads and processed outputs — local
  disk for dev, S3-compatible storage for anything beyond one machine.
- **CDN + webhooks** keep playback traffic off storage directly and let
  clients get notified instead of polling forever.

## Features

**Core pipeline**
- Async upload → queue → transcode → deliver flow
- Stateless, horizontally scalable API servers
- Durable job queue (swappable: channel → Kafka/SQS/NATS)
- Autoscaled worker pool, independent of the API layer
- Postgres for job status and video metadata
- S3-compatible object storage for raw and processed files

**Processing**
- FFmpeg-based transcoding
- Thumbnail/preview generation
- Optional GPU acceleration (NVENC/VAAPI) for high-volume transcoding
- Multi-rendition output support (for adaptive bitrate streaming)

**Reliability**
- Idempotent job processing (safe to retry a job that crashed
  mid-transcode)
- Retry logic for failed jobs
- Job status tracking through queued → processing → done/failed

**Delivery**
- CDN in front of object storage
- Webhook notifications on job completion

**Not included yet** — add as needed:
- Auth/authorization on the API
- HLS/DASH manifest generation
- Live streaming (this assumes VOD/file-based input)
- Rate limiting, quotas, multi-tenancy
- Observability (metrics, tracing, structured logging, alerting)
- Content moderation / virus scanning on upload

## Project layout

```
video-service/
  cmd/server/        # main entrypoint
  internal/
    api/             # HTTP handlers (upload, status)
    job/             # Job struct, status, store interface
    worker/          # Worker pool
    ffmpeg/          # FFmpeg wrapper (transcode, thumbnail)
    storage/         # Storage interface (local disk / S3)
  deploy/k8s/         # Kubernetes manifests (worker autoscaling, etc.)
  go.mod
```

## Getting started

Requirements: Go 1.22+, FFmpeg on `PATH`.

```bash
go mod tidy
go run ./cmd/server
```

## Roadmap

1. **MVP** — in-memory queue, local disk storage, single binary. Get one
   video through the full pipeline end to end.
2. **Durability** — swap in Postgres for job storage, S3/MinIO for
   files, a real queue (Kafka/SQS/NATS).
3. **Scale out** — containerize, deploy API and workers separately,
   autoscale workers on queue depth (Kubernetes HPA or KEDA).
4. **Production hardening** — auth, retries, idempotency, structured
   logging/metrics, webhooks.
5. **Advanced** — multi-rendition/HLS output, GPU-accelerated
   transcoding, CDN integration.

## Design decisions worth locking in early

- **Queue choice** shapes a lot of the operational surface later. NATS
  JetStream or SQS are the least painful starting points if unsure.
- **Job schema** should support "one input → many outputs" from the
  start if adaptive bitrate streaming is ever on the roadmap.
- **Idempotency** — workers will crash mid-job and jobs will get
  retried; re-running a job should always be safe.
