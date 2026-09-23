# Distributed Job Queue / Task Processing System

A backend system that accepts jobs over an HTTP API, queues them in Redis,
and processes them with a pool of independent worker processes — with
priority ordering, exponential-backoff retries, a dead-letter queue, crash
recovery, cooperative cancellation, per-job timeouts, scheduled/delayed
jobs, JWT auth, rate limiting, and a Kafka event stream for observability.
It also runs **AI jobs** (summarize / classify / extract via Claude) on the
same pipeline, with model fallback and non-retryable-error short-circuiting.

Built as a from-scratch, plain-JavaScript (no TypeScript) portfolio project
to demonstrate core distributed-systems concepts: atomic queue operations,
distributed locking with fencing, at-least-once processing, and failure
recovery — not just CRUD.

---

## Table of Contents

- [Architecture](#architecture)
- [Job Lifecycle](#job-lifecycle)
- [Why Redis / MongoDB / Kafka](#why-redis--mongodb--kafka)
- [Core Concepts](#core-concepts)
- [AI Jobs](#ai-jobs)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Setup](#setup)
- [Running with Docker](#running-with-docker)
- [API Reference](#api-reference)
- [Environment Variables](#environment-variables)
- [Known Trade-offs / Simplifications](#known-trade-offs--simplifications)

---

## Architecture

```
                    ┌─────────────┐
   HTTP clients ───▶│   API (Express) │───▶ MongoDB (source of truth: job docs, users)
                    └─────────────┘
                          │
                          │ push jobId (ZADD, scored by priority)
                          ▼
                    ┌─────────────┐
                    │ Redis ZSET  │◀── delayed queue promotes ready jobs back here
                    │ jobs:queue  │
                    └─────────────┘
                          │
                          │ BZPOPMIN (blocking pop, atomic — exactly one
                          │ worker gets each job)
                          ▼
              ┌───────────────────────┐
              │   Worker process(es)   │  (N concurrent "lanes" per process,
              │  - lock job in Mongo   │   N processes via docker compose --scale)
              │  - run handler         │
              │  - finalize (fenced)   │
              └───────────────────────┘
                     │         │
        success/fail │         │ fire-and-forget
                      ▼         ▼
             MongoDB (status,     Kafka (job-events topic)
             history, result)          │
                                        ▼
                              Analytics consumer (separate process)
```

Three independent background loops run inside every worker process,
decoupled from the main claim/process loop so a slow or hung job never
blocks them:

- **Heartbeat** — writes a TTL key to Redis every few seconds, proving this
  worker is alive.
- **Promotion** — scans the delayed-jobs ZSET for anything whose "ready at"
  time has passed (a retry backoff, or a scheduled job) and moves it back
  into the main queue.
- **Recovery** — scans MongoDB for jobs stuck in `PROCESSING` whose lock has
  expired (`lockExpiresAt < now`), meaning the worker holding them died or
  hung, and hands them back into the retry/DLQ flow.

## Job Lifecycle

```
QUEUED ──(worker claims via BZPOPMIN)──▶ PROCESSING
                                              │
                       ┌──────────────────────┼───────────────────────┐
                       ▼                      ▼                       ▼
                  COMPLETED            attempts < max?           cancellation
                (handler resolved,     RETRYING (backoff,        requested while
                 not cancelled)        back to delayed queue)    PROCESSING
                                              │                       │
                                       attempts >= max?                ▼
                                       DEAD_LETTER              CANCELLED (honored
                                                                 once handler
                                                                 finishes)
```

`QUEUED` / `RETRYING` jobs can also be cancelled immediately (they're just
sitting in Redis, nothing is running them yet). A job already in a terminal
state (`COMPLETED` / `FAILED` / `DEAD_LETTER` / `CANCELLED`) cannot be
cancelled.

## Why Redis / MongoDB / Kafka

Each dependency has one job, deliberately not overlapping:

| Store | Role | Why |
|---|---|---|
| **MongoDB** | Source of truth for job state, history, results | Durable, queryable, supports atomic conditional updates (`findOneAndUpdate`) needed for fencing |
| **Redis** | The queue itself (who's next), locks' TTL data, rate-limit counters, worker heartbeats | In-memory speed, atomic primitives (`BZPOPMIN`, `ZADD`, `INCR`) that make race-free queueing cheap |
| **Kafka** | Event stream for anything downstream (analytics, notifications) | Decouples "a job happened" from "something needs to react to it" — and it's fire-and-forget, so a Kafka outage never breaks the core pipeline |

## Core Concepts

**Atomic claiming.** Workers pop off the Redis queue with `BZPOPMIN`, an
atomic blocking pop — Redis guarantees only one caller ever gets a given
element, so two workers can never process the same job at the same time
just from the claim step.

**Priority queue via score.** Jobs aren't stored with separate priority and
timestamp fields in the ZSET — they're combined into one sortable score:
`score = priority * 1e12 + timestamp`. The weight (`1e12`) is large enough
that priority always dominates the comparison, and timestamp only breaks
ties within the same priority (FIFO among equals).

**Distributed locking with fencing.** When a worker claims a job it writes
`lockedBy: <workerId>` and `lockExpiresAt: now + 30s` onto the Mongo
document. Every *write back* (completing, failing, retrying) goes through
`findOneAndUpdate({ _id, lockedBy: expectedOwner }, ...)` — conditional on
still owning the lock. If a recovery scan already reassigned the job to
someone else, this conditional update matches nothing and is silently
skipped, instead of clobbering whatever the new owner already wrote. This
is what prevents the classic "two workers, one job, lost update" race.

**Exponential backoff.** `delay = min(baseDelay * 2^(attempts-1), maxDelay)`.
The `attempts >= maxAttempts` check happens *after* incrementing attempts,
guaranteeing exactly `maxAttempts` tries, not one extra.

**Dead-letter queue.** A job that exhausts all retries moves to
`DEAD_LETTER` instead of disappearing — nothing about a distributed job
system should silently lose work.

**At-least-once processing.** If a worker dies mid-job, the job gets
retried by someone else — which means a handler can run more than once for
the same job. This system does not force idempotent handlers, but the
`idempotencyKey` on job *submission* prevents duplicate job creation from
the client side (e.g. a retried HTTP request).

**Cooperative cancellation.** JavaScript cannot forcibly kill an in-flight
async function from outside it. Cancelling a `PROCESSING` job just sets a
flag (`cancellationRequested`); the worker checks that flag at the one
checkpoint it actually controls — right after the handler resolves — and
discards the result instead of marking it `COMPLETED` if cancellation was
requested meanwhile.

**Per-job timeout.** `Promise.race()` between the handler and a timer. Like
cancellation, this only stops *waiting* on the handler — it can still be
running in the background. If it eventually finishes and tries to write a
result, the same fencing check (lock no longer owned) silently discards it.

**Worker concurrency.** Each worker process runs N independent polling
"lanes," each with its **own dedicated Redis connection** for the blocking
`BZPOPMIN` call. Sharing one connection between a blocking command and
other commands (heartbeat, promotion) would silently delay those other
commands for as long as the blocking call is waiting — a real bug caught
while building this.

**Scheduled jobs.** A job submitted with a future `scheduledFor` reuses the
exact same delayed-queue infrastructure retries already use — "not ready
yet, check back later" is the same mechanism whether the reason is a retry
backoff or a future schedule.

**Non-retryable errors (poison messages).** An error marked
`retryable: false` (bad input, invalid API key, unknown job type) goes
straight to `DEAD_LETTER` instead of burning all its retries — it would fail
identically every time. Errors that don't set the flag keep the normal
retry-with-backoff behavior.

**Lock duration follows the job timeout.** A job's lock lasts
`max(30s, timeoutMs + 5s)`. A fixed 30s lock would let the recovery scan
"rescue" a healthy worker's 60s AI job at second 31 and run it twice.

## AI Jobs

LLM calls are slow (seconds), flaky (rate limits, overload) and billed per
call — the exact kind of work that belongs behind a queue rather than inside
an HTTP request. So AI work is just three more job types; retries, timeouts,
fencing and DLQ apply unchanged.

| Job type | Payload | Result |
|---|---|---|
| `ai-summarize` | `{ text, maxWords? }` | `{ summary, ai }` |
| `ai-classify` | `{ text, labels: [...] }` | `{ label, ai }` |
| `ai-extract` | `{ text, fields: [...] }` | `{ data: { field: value \| null }, ai }` |

Every result includes `ai: { model, fallbackUsed, failedModels, inputTokens,
outputTokens }` for cost and reliability tracking.

How it works (`src/ai/`):

- **Model fallback.** `AI_MODELS` is an ordered chain
  (default `claude-sonnet-5` → `claude-haiku-4-5`). On a temporary error
  (429, 5xx, 529 overloaded, network, timeout) the next model is tried
  immediately, within the same job attempt. If every model fails, the job
  falls back to normal job-level retry with backoff.
- **Error classification.** Permanent errors (400, 401, 403) stop the chain
  at once and send the job straight to DLQ, since no model or retry can fix
  a bad request or a bad key. 404 (unknown model) still falls through,
  because the next model in the chain may exist.
- **Real cancellation.** Each HTTP call has an `AbortController` timeout that
  actually closes the connection, unlike the generic `Promise.race` job
  timeout, which can only stop waiting.
- **No SDK, on purpose.** The official SDK retries internally. Stacked on the
  queue's own retries that could mean 9 paid calls for one failing job.
  Plain `fetch` keeps one retry policy, owned by the queue.
- **Output validation.** Classification labels are matched against the
  allowed list, and extracted JSON is parsed and trimmed to the requested
  keys. Invalid output is a *retryable* error, since LLM output is
  non-deterministic.
- **Prompt-injection guard.** User text is wrapped in `<document>` tags and
  the system prompt marks it as data only. This is a first line of defence,
  not a guarantee.
- **Cost guard.** Input is capped at 20,000 characters.
- **Least privilege.** Only the worker container gets `ANTHROPIC_API_KEY`;
  the API never calls the LLM.

Example:

```bash
curl -X POST http://localhost:4000/api/jobs \
  -H "Authorization: Bearer <accessToken>" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "ai-classify",
    "payload": { "text": "The delivery was late and the box was crushed.", "labels": ["positive", "negative", "neutral"] },
    "idempotencyKey": "classify-001"
  }'
# then poll GET /api/jobs/<jobId> until status is COMPLETED
```

## Tech Stack

Node.js (ES Modules, plain JavaScript) · Express 5 · MongoDB + Mongoose ·
Redis (ioredis) · Kafka (KRaft mode, no Zookeeper) · JWT (jsonwebtoken +
bcrypt) · Docker + Docker Compose · Swagger/OpenAPI (swagger-jsdoc +
swagger-ui-express)

## Project Structure

```
src/
  config/         env loading, Mongo/Redis connections, Swagger spec
  models/         Mongoose schemas (Job, User)
  queue/          Redis queue operations (push/pop/promote), heartbeat
  worker/         the worker process + job handler registry
  modules/
    auth/         register/login/refresh/logout
    jobs/         submit/status/cancel
  middlewares/    authenticate (JWT), rateLimiter (fixed window)
  kafka/          producer (fire-and-forget publish) + analytics consumer
  ai/             Claude client (fetch + error classification) + model fallback
  utils/          structured logger
  app.js          Express app, routes, health checks, Swagger UI
  server.js       API entry point
docker/
  docker-compose.yml
```

## Setup

```bash
npm install
cp .env.example .env   # fill in secrets

# requires MongoDB + Redis + Kafka running locally, or use Docker (below)

npm run dev             # API with autoreload
npm run worker          # a worker process (run this in a separate terminal)
npm run consumer:analytics   # optional: logs every Kafka job-event
```

## Running with Docker

```bash
cd docker
docker compose up --build

# scale to 3 worker processes, each with WORKER_CONCURRENCY lanes inside it
docker compose up --build --scale worker=3
```

This brings up MongoDB, Redis, Kafka (KRaft mode), the API, and one worker
service — all on the same Docker network, using service names (`mongo`,
`redis`, `kafka`) instead of `localhost` for inter-container communication.

## API Reference

Full interactive docs (request/response schemas, try-it-out) are served at:

```
GET /api-docs
```

once the API is running. Quick summary:

| Method | Path | Auth | Description |
|---|---|---|---|
| POST | `/api/auth/register` | — | Create an account, receive tokens |
| POST | `/api/auth/login` | — | Log in, receive tokens |
| POST | `/api/auth/refresh` | — | Rotate a refresh token for a new pair |
| POST | `/api/auth/logout` | — | Revoke the stored refresh token |
| POST | `/api/jobs` | Bearer | Submit (or schedule) a job |
| GET | `/api/jobs/:id` | Bearer | Get a job's status + history |
| POST | `/api/jobs/:id/cancel` | Bearer | Cancel a job |
| GET | `/health` | — | Combined Mongo + Redis health |
| GET | `/health/mongodb` | — | MongoDB health only |
| GET | `/health/redis` | — | Redis health only |
| GET | `/health/kafka` | — | Kafka health only |

Example: submit a job

```bash
curl -X POST http://localhost:4000/api/jobs \
  -H "Authorization: Bearer <accessToken>" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "send-email",
    "payload": { "to": "user@example.com" },
    "priority": 3,
    "idempotencyKey": "unique-key-123",
    "maxAttempts": 3
  }'
```

## Environment Variables

See `.env.example`:

| Variable | Purpose |
|---|---|
| `PORT` | API port |
| `MONGO_URI` | MongoDB connection string |
| `REDIS_HOST` / `REDIS_PORT` | Redis connection |
| `KAFKA_BROKER` | Kafka bootstrap broker |
| `JWT_ACCESS_SECRET` / `JWT_REFRESH_SECRET` | Signing secrets (separate, so leaking one doesn't compromise the other) |
| `WORKER_CONCURRENCY` | Number of concurrent polling lanes per worker process |
| `ANTHROPIC_API_KEY` | Claude API key (optional; without it only AI jobs fail, straight to DLQ) |
| `AI_MODELS` | Comma-separated fallback chain, tried left to right |
| `AI_REQUEST_TIMEOUT_MS` | Per-LLM-call timeout (default 25000) |

With Docker, put `ANTHROPIC_API_KEY=...` in a `docker/.env` file (Compose
reads it automatically) or export it in your shell before `docker compose up`.

## Known Trade-offs / Simplifications

These were deliberate calls, made explicit rather than accidental gaps:

- **Single active session per user** — `User.refreshTokenHash` is one field,
  not an array. Logging in on a new device invalidates the old session's
  refresh token. A real multi-device system would store a hash per device.
- **Fixed-window rate limiting**, not sliding-window/token-bucket — simpler
  to reason about, at the cost of allowing a burst right at a window
  boundary.
- **No forced idempotent handlers** — at-least-once delivery is real; the
  system doesn't (and can't generically) guarantee a handler only has
  side-effects once. `idempotencyKey` solves duplicate *submission*, not
  duplicate *execution* after a crash-and-retry.
- **No dedicated Redis lock structure** — locking is done via
  `lockedBy`/`lockExpiresAt` fields on the Mongo document rather than a
  Redis-native lock (e.g. Redlock). Simpler, and consistent with Mongo
  being the single source of truth for job state.
- **AI fallback is model-level, single provider.** The chain is Claude
  models only. A full Anthropic outage fails every model, and the job then
  relies on job-level retries. Adding a second provider means one more
  client file; handlers don't change.
- **At-least-once applies to AI jobs too.** A worker crash after the LLM
  answered but before the result was saved means the call happens (and is
  billed) again on retry.
- **Structured logging is a minimal hand-rolled JSON logger**, not
  winston/pino — deliberately, to avoid a dependency and configuration
  surface this project doesn't need (no log rotation, no multiple
  transports).
