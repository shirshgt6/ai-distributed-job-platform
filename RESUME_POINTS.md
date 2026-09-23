# Resume Bullet Points

Pick 3–4 depending on space. Ordered roughly by interview-signal strength
(distributed-systems depth first, breadth later).

- Designed and built a distributed job queue system (Node.js, MongoDB,
  Redis, Kafka) supporting priority scheduling, exponential-backoff
  retries, and dead-lettering, processing jobs with **at-least-once**
  delivery guarantees across concurrent worker processes.

- Implemented distributed locking with **fencing tokens** (ownership-
  conditional atomic writes) to eliminate lost-update races between a
  worker's normal completion and a separate crash-recovery scan reclaiming
  the same job — verified via targeted concurrency testing.

- Built a multi-lane worker architecture (N concurrent polling loops per
  process) using dedicated Redis connections per lane to avoid blocking
  commands (`BZPOPMIN`) from starving other Redis operations on a shared
  connection.

- Implemented crash recovery via Redis TTL-based worker heartbeats and
  per-job lock expiry, allowing any live worker to detect and reclaim work
  abandoned by a dead or hung worker without manual intervention.

- Built cooperative job cancellation and per-job timeouts using
  `Promise.race`, reusing the same ownership-fencing mechanism to safely
  discard late results from cancelled or timed-out jobs.

- Integrated LLM workloads (Claude) as queue-backed AI jobs (summarize,
  classify, structured extraction) with an ordered model-fallback chain,
  retryable/non-retryable error classification that sends poison messages
  straight to the DLQ, `AbortController`-based request cancellation,
  validated model output, and per-job token-usage tracking.

- Designed a JWT authentication system with short-lived access tokens and
  revocable, rotating refresh tokens (hashed at rest), plus IDOR protection
  via per-request ownership checks on job resources.

- Decoupled event streaming from the core processing pipeline using Kafka
  (fire-and-forget publishing keyed by job ID for per-job ordering), so a
  Kafka outage never blocks job submission or processing.

- Containerized the full stack (API, worker, MongoDB, Redis, Kafka in KRaft
  mode) with Docker Compose, supporting horizontal worker scaling via
  `docker compose up --scale worker=N`.

- Documented all endpoints with an auto-generated OpenAPI/Swagger spec
  derived directly from route-level JSDoc annotations, keeping docs in
  sync with the code by construction.

## One-liner (for a project list / portfolio page)

> Distributed job queue and task processing system built from scratch in
> Node.js — Redis-backed priority queueing, MongoDB-backed fenced
> distributed locking, Kafka event streaming, fault-tolerant LLM jobs with
> model fallback, JWT auth, and Docker Compose deployment with horizontally
> scalable workers.
