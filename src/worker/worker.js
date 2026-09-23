import os from "os";
import { env } from "../config/env.js";
import { connectMongo } from "../config/db.js";
import { redisClient } from "../config/redis.js";
import { claimNextJob, promoteDueDelayedJobs, createBlockingClient } from "../queue/claim.js";
import { pushJobToQueue, pushToDelayedQueue } from "../queue/queue.js";
import { startHeartbeatLoop } from "../queue/heartbeat.js";
import { Job } from "../models/job.model.js";
import { getHandler } from "./jobHandlers/index.js";
import { publishEvent } from "../kafka/producer.js";
import { logger } from "../utils/logger.js";

const WORKER_ID = `${os.hostname()}-${process.pid}`;

const BASE_DELAY_MS = 2000;
const MAX_DELAY_MS = 60000;
const LOCK_TTL_MS = 30000;

// Independent background intervals — deliberately NOT nested inside the
// main claim/process loop, because that loop can block for a long time
// (or forever) inside a hung handler. These run alongside it.
const PROMOTE_INTERVAL_MS = 1000;
const RECOVERY_INTERVAL_MS = 5000;
const HEARTBEAT_INTERVAL_MS = 3000;
const HEARTBEAT_TTL_SECONDS = 10;

function computeBackoffMs(attempts) {
  const delay = BASE_DELAY_MS * Math.pow(2, attempts - 1);
  return Math.min(delay, MAX_DELAY_MS);
}

// THE OWNERSHIP CHECK (fencing) — every finishing write says: "only apply
// this update if the job is STILL locked by the owner I expect." If someone
// else (a recovery scan, or another worker) already took it over, `lockedBy`
// won't match anymore, and findOneAndUpdate returns null — we skip the
// write instead of clobbering whatever the new owner already did. This is
// what prevents a slow-but-alive worker's late result from overwriting a
// newer outcome (the "two workers on the same job" lost-update bug).
async function finalizeJob(jobId, expectedOwner, updates, historyEntry) {
  const result = await Job.findOneAndUpdate(
    { _id: jobId, lockedBy: expectedOwner },
    { $set: updates, $push: { history: historyEntry } },
    { returnDocument: "after" }
  );

  if (!result) {
    logger.warn("skipped stale write — ownership changed since this attempt started", {
      workerId: WORKER_ID,
      jobId,
      expectedOwner,
    });
  }

  return result;
}

// expectedOwner: whoever we believe currently "owns" this job's lock.
// - Normal handler failure: it's US (WORKER_ID) — we just claimed it.
// - Recovery-scan failure: it's the STALE worker we're taking over from —
//   we're conditionally transitioning it away from them, not from ourselves.
async function handleFailure(job, err, expectedOwner) {
  // job.attempts was already incremented before the handler ran, so by the
  // time we get here it reflects "this was attempt number N." Checking
  // >= (not >) after that increment is what guarantees exactly maxAttempts
  // total tries, not one extra.
  const exhausted = job.attempts >= job.maxAttempts;

  // POISON MESSAGE short-circuit: an error explicitly marked
  // retryable === false (bad input, invalid API key, unknown job type) will
  // fail identically on every attempt. Retrying only wastes backoff time —
  // and for AI jobs, real money per call. Send it straight to DLQ.
  // Note the strict `=== false`: errors that don't set the flag at all
  // (every pre-existing handler) keep the old behavior and ARE retried.
  const nonRetryable = err.retryable === false;

  if (exhausted || nonRetryable) {
    const reason = nonRetryable
      ? `Non-retryable error on attempt ${job.attempts}/${job.maxAttempts}: ${err.message}`
      : `Exhausted ${job.attempts}/${job.maxAttempts} attempts: ${err.message}`;

    const updated = await finalizeJob(
      job._id,
      expectedOwner,
      { status: "DEAD_LETTER", error: err.message, lockedBy: null, lockExpiresAt: null },
      {
        status: "DEAD_LETTER",
        workerId: WORKER_ID,
        note: reason,
      }
    );
    if (updated) {
      logger.error("job moved to DEAD_LETTER", {
        workerId: WORKER_ID,
        jobId: job._id,
        type: job.type,
        attempts: job.attempts,
        maxAttempts: job.maxAttempts,
        nonRetryable,
        error: err.message,
      });
      publishEvent("JOB_DLQ", { jobId: job._id, type: job.type, attempts: job.attempts, error: err.message });
    }
    return;
  }

  const backoffMs = computeBackoffMs(job.attempts);
  const readyAt = Date.now() + backoffMs;

  const updated = await finalizeJob(
    job._id,
    expectedOwner,
    { status: "RETRYING", error: err.message, lockedBy: null, lockExpiresAt: null },
    {
      status: "RETRYING",
      workerId: WORKER_ID,
      note: `Attempt ${job.attempts}/${job.maxAttempts} failed: ${err.message}. Retrying in ${backoffMs}ms`,
    }
  );

  // Only schedule the retry if OUR write actually landed. If it didn't,
  // whoever already owns this job is responsible for its own retry logic —
  // us pushing to the delayed queue too would double-schedule it.
  if (updated) {
    await pushToDelayedQueue(job._id, readyAt);
    logger.info("job scheduled for retry", {
      workerId: WORKER_ID,
      jobId: job._id,
      backoffMs,
      attempt: job.attempts,
      maxAttempts: job.maxAttempts,
    });
    publishEvent("JOB_RETRYING", {
      jobId: job._id,
      type: job.type,
      attempt: job.attempts,
      maxAttempts: job.maxAttempts,
      backoffMs,
      error: err.message,
    });
  }
}

async function processJob(jobId, laneId = 0) {
  const job = await Job.findById(jobId);

  if (!job) {
    logger.error("claimed jobId not found in DB, skipping", { workerId: WORKER_ID, laneId, jobId });
    return;
  }

  logger.info("claimed job", { workerId: WORKER_ID, laneId, jobId: job._id, type: job.type });

  // This initial claim write is safe as a plain save — nothing else can be
  // racing us for a job that was JUST atomically popped off the queue.
  job.status = "PROCESSING";
  job.lockedBy = WORKER_ID;
  // The lock must outlive the job's own timeout. If a job is allowed 60s
  // (AI jobs) but the lock only lasts 30s, the recovery scan decides at
  // second 31 that this (perfectly alive) worker is dead and re-queues the
  // job — fencing prevents corruption, but the job runs twice and, for AI
  // jobs, gets billed twice. The +5s buffer covers the post-handler writes.
  job.lockExpiresAt = new Date(Date.now() + Math.max(LOCK_TTL_MS, job.timeoutMs + 5000));
  job.startedAt = new Date();
  job.attempts += 1;
  job.history.push({ status: "PROCESSING", workerId: WORKER_ID, note: `Attempt ${job.attempts}` });
  await job.save();

  publishEvent("JOB_STARTED", { jobId: job._id, type: job.type, workerId: WORKER_ID, attempt: job.attempts });

  try {
    const handler = getHandler(job.type);

    // JOB TIMEOUT: Promise.race only stops US from WAITING on the handler
    // any longer — it does NOT actually cancel the handler's execution.
    // JavaScript has no mechanism to forcibly kill an in-flight async
    // function from outside it. So if the handler is genuinely still
    // running in the background after we've already moved this job to
    // RETRYING/DEAD_LETTER, that's fine: when it eventually finishes and
    // tries to write its result, finalizeJob()'s ownership check will see
    // this worker no longer holds the lock and silently discard the write
    // — the same fencing mechanism that already protects against the
    // slow-worker lost-update bug protects against this too, for free.
    const result = await Promise.race([
      handler(job.payload),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error(`Job timed out after ${job.timeoutMs}ms`)), job.timeoutMs)
      ),
    ]);

    // COOPERATIVE CANCELLATION CHECKPOINT: our in-memory `job` object is a
    // snapshot from BEFORE the handler ran — it has no idea whether the
    // cancel API was called while we were busy. We re-read cancellationRequested
    // fresh from Mongo right now, at the one point we actually CAN act on it
    // (we can't interrupt the handler itself, but we CAN choose not to report
    // success once it's done).
    const freshJob = await Job.findById(job._id).select("cancellationRequested");
    const wasCancelled = freshJob?.cancellationRequested === true;

    const updated = await finalizeJob(
      job._id,
      WORKER_ID,
      wasCancelled
        ? { status: "CANCELLED", lockedBy: null, lockExpiresAt: null }
        : { status: "COMPLETED", result, completedAt: new Date(), lockedBy: null, lockExpiresAt: null },
      wasCancelled
        ? { status: "CANCELLED", workerId: WORKER_ID, note: "Cancellation honored after handler finished" }
        : { status: "COMPLETED", workerId: WORKER_ID, note: "Job completed successfully" }
    );

    if (updated) {
      if (wasCancelled) {
        logger.info("job finished but was cancelled — discarding result", {
          workerId: WORKER_ID,
          jobId: job._id,
        });
        publishEvent("JOB_CANCELLED", { jobId: job._id, previousStatus: "PROCESSING" });
      } else {
        logger.info("completed job", { workerId: WORKER_ID, jobId: job._id, type: job.type, attempts: job.attempts });
        publishEvent("JOB_COMPLETED", { jobId: job._id, type: job.type, attempts: job.attempts });
      }
    }
  } catch (err) {
    logger.error("job attempt failed", {
      workerId: WORKER_ID,
      jobId: job._id,
      attempt: job.attempts,
      error: err.message,
    });
    await handleFailure(job, err, WORKER_ID);
  }
}

// Independent interval: moves due retries/delayed jobs back into the main
// queue. Runs on its own clock, NOT tied to how long the main loop's
// BZPOPMIN block happens to be waiting (that coupling was a real bug we
// found: promotion was silently delayed up to 5s because it only ran
// between blocking claims).
async function promoteReadyJobs() {
  const promotedIds = await promoteDueDelayedJobs();

  for (const jobId of promotedIds) {
    const job = await Job.findById(jobId);
    if (!job) continue;

    job.status = "QUEUED";
    job.history.push({ status: "QUEUED", workerId: WORKER_ID, note: "Promoted from delayed queue, ready for retry" });
    await job.save();

    await pushJobToQueue(job._id, job.priority);
    logger.info("promoted job back to main queue", { workerId: WORKER_ID, jobId: job._id });
  }
}

// Independent interval: finds jobs stuck in PROCESSING whose lock has
// expired — meaning either the worker holding them died, or that worker
// is hung on this specific job. Either way, from the system's perspective
// the outcome is identical: reclaim it and let normal retry/DLQ logic decide
// what happens next. Any worker can recover any abandoned job, since a
// truly dead worker obviously can't recover its own.
async function recoverStaleJobs() {
  const now = new Date();
  const staleJobs = await Job.find({ status: "PROCESSING", lockExpiresAt: { $lt: now } });

  for (const job of staleJobs) {
    const staleOwner = job.lockedBy;
    logger.warn("attempting to recover stale job", {
      workerId: WORKER_ID,
      jobId: job._id,
      staleOwner,
      lockExpiresAt: job.lockExpiresAt,
    });
    await handleFailure(job, new Error(`Worker ${staleOwner} did not complete job before lock expired`), staleOwner);
  }
}

async function runWorker() {
  await connectMongo();
  logger.info("worker started, waiting for jobs", { workerId: WORKER_ID });

  // These three run independently and concurrently with the main loop below.
  // A hung job in the main loop does NOT block these — that's the entire point.
  startHeartbeatLoop(WORKER_ID, HEARTBEAT_INTERVAL_MS, HEARTBEAT_TTL_SECONDS);
  setInterval(() => {
    promoteReadyJobs().catch((err) => logger.error("promote error", { workerId: WORKER_ID, error: err.message }));
  }, PROMOTE_INTERVAL_MS);
  setInterval(() => {
    recoverStaleJobs().catch((err) => logger.error("recovery error", { workerId: WORKER_ID, error: err.message }));
  }, RECOVERY_INTERVAL_MS);

  // CONCURRENCY: run N independent "lanes" inside this one worker process,
  // each with its OWN dedicated blocking Redis connection (see the comment
  // in claim.js on why sharing one connection for BZPOPMIN is a problem).
  // Each lane is its own infinite loop: block for a job, process it, repeat
  // — same logic as before, just N of them running concurrently instead of
  // one. We deliberately do NOT await these in sequence (that would make
  // them run one after another, defeating the purpose) — we let all N
  // start immediately and run forever side by side.
  logger.info("starting lanes", { workerId: WORKER_ID, concurrency: env.workerConcurrency });

  const lanes = [];
  for (let laneId = 0; laneId < env.workerConcurrency; laneId++) {
    const blockingClient = createBlockingClient();
    lanes.push(runLane(laneId, blockingClient));
  }

  // Keep the process alive on these forever-running loops. If any lane's
  // loop ever throws out of its own internal handling, that's a real bug
  // worth crashing loudly for rather than silently losing a lane.
  await Promise.all(lanes);
}

async function runLane(laneId, blockingClient) {
  while (true) {
    const jobId = await claimNextJob(blockingClient, 5);
    if (!jobId) continue;
    await processJob(jobId, laneId);
  }
}

runWorker().catch((err) => {
  logger.error("fatal error, exiting", { workerId: WORKER_ID, error: err.message, stack: err.stack });
  process.exit(1);
});
