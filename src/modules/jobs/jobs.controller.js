import { Job } from "../../models/job.model.js";
import {
  pushJobToQueue,
  pushToDelayedQueue,
  removeJobFromQueue,
  removeJobFromDelayedQueue,
} from "../../queue/queue.js";
import { isRedisHealthy } from "../../config/redis.js";
import { publishEvent } from "../../kafka/producer.js";

// POST /api/jobs
// userId now comes from req.user.id — set by the `authenticate` middleware
// after verifying the access token. We no longer trust a client-supplied
// userId in the body; a client could otherwise submit jobs "as" any user
// simply by putting a different id in the request.
export async function submitJob(req, res) {
  const { type, payload, priority, idempotencyKey, timeoutMs, maxAttempts, scheduledFor } = req.body;
  const userId = req.user.id;

  if (!type || !idempotencyKey) {
    return res.status(400).json({
      error: "type and idempotencyKey are required",
    });
  }

  // Redis is a hard dependency for this specific route (queueing).
  // We check it explicitly rather than letting a downstream call fail ugly.
  const redisOk = await isRedisHealthy();
  if (!redisOk) {
    return res.status(503).json({
      error: "Queue temporarily unavailable, please retry",
    });
  }

  // Duplicate submission check — real idempotency guarantee comes from
  // the unique index at the DB level; this pre-check just gives a nicer
  // error message instead of a raw duplicate-key error.
  const existing = await Job.findOne({ idempotencyKey });
  if (existing) {
    return res.status(200).json({
      message: "Job already submitted",
      job: existing,
    });
  }

  // SCHEDULED JOBS: if the caller passed a future scheduledFor timestamp,
  // this job shouldn't be eligible for a worker to pick up until then. We
  // reuse the exact same delayed-queue infrastructure retries already use
  // (jobs:delayed, scored by ready-at time) — a scheduled job and a
  // retrying job are the same underlying mechanism: "not ready yet, check
  // back later." No new Redis structure needed.
  // AI jobs need a bigger default timeout: the fallback chain may make
  // 2 LLM calls of up to 25s each (see env.aiRequestTimeoutMs), and the
  // job-level timeout must leave room for BOTH, or the fallback model never
  // gets a turn. An explicit timeoutMs from the client still wins.
  const defaultTimeoutMs = type.startsWith("ai-") ? 60000 : 30000;

  const scheduledDate = scheduledFor ? new Date(scheduledFor) : null;
  const isScheduledForFuture = scheduledDate && scheduledDate.getTime() > Date.now();

  const job = await Job.create({
    userId,
    type,
    payload: payload || {},
    priority: priority ?? 5,
    idempotencyKey,
    maxAttempts: maxAttempts ?? 3,
    timeoutMs: timeoutMs ?? defaultTimeoutMs,
    scheduledFor: isScheduledForFuture ? scheduledDate : null,
    history: [
      {
        status: "QUEUED",
        note: isScheduledForFuture ? `Job created, scheduled for ${scheduledDate.toISOString()}` : "Job created",
      },
    ],
  });

  if (isScheduledForFuture) {
    // NOT pushed to the main queue — sits in the delayed queue until its
    // time comes. The worker's existing promoteReadyJobs() interval (built
    // for retries) already knows how to move a "ready" job from there into
    // the main queue — scheduled jobs get that behavior for free.
    await pushToDelayedQueue(job._id, scheduledDate.getTime());
  } else {
    await pushJobToQueue(job._id, job.priority);
  }

  // Fire-and-forget event for anything downstream that cares a job was
  // created (analytics, notifications later). Never blocks/fails the
  // response — see the comment in kafka/producer.js.
  publishEvent("JOB_CREATED", {
    jobId: job._id,
    userId: job.userId,
    type: job.type,
    priority: job.priority,
    scheduledFor: job.scheduledFor,
  });

  return res.status(201).json({
    message: isScheduledForFuture ? "Job scheduled" : "Job submitted",
    jobId: job._id,
    status: job.status,
    scheduledFor: job.scheduledFor,
  });
}

// GET /api/jobs/:id
export async function getJobStatus(req, res) {
  const { id } = req.params;

  const job = await Job.findById(id);
  if (!job) {
    return res.status(404).json({ error: "Job not found" });
  }

  // AUTHORIZATION, not authentication: we already know WHO is asking
  // (authenticate middleware verified that), but we still need to check
  // whether THEY are allowed to see THIS specific job. Without this check,
  // any logged-in user could view any other user's job just by guessing/
  // incrementing job IDs — a real vulnerability class (insecure direct
  // object reference / IDOR).
  if (job.userId.toString() !== req.user.id) {
    return res.status(403).json({ error: "You do not have access to this job" });
  }

  return res.status(200).json({ job });
}

// POST /api/jobs/:id/cancel
export async function cancelJob(req, res) {
  const { id } = req.params;

  const job = await Job.findById(id);
  if (!job) {
    return res.status(404).json({ error: "Job not found" });
  }

  if (job.userId.toString() !== req.user.id) {
    return res.status(403).json({ error: "You do not have access to this job" });
  }

  // Terminal states can't be cancelled — the work already happened (or
  // permanently failed), there's nothing left to stop.
  const terminalStates = ["COMPLETED", "FAILED", "DEAD_LETTER", "CANCELLED"];
  if (terminalStates.includes(job.status)) {
    return res.status(409).json({
      error: `Cannot cancel a job that is already ${job.status}`,
    });
  }

  if (job.status === "QUEUED" || job.status === "RETRYING") {
    // Easy case: nothing is actively running this job right now. We can
    // remove it from whichever Redis structure it's sitting in and mark
    // it CANCELLED immediately — no race to worry about.
    await removeJobFromQueue(job._id);
    await removeJobFromDelayedQueue(job._id);

    job.status = "CANCELLED";
    job.history.push({ status: "CANCELLED", note: "Cancelled by user before processing started" });
    await job.save();

    publishEvent("JOB_CANCELLED", { jobId: job._id, previousStatus: "QUEUED" });
    return res.status(200).json({ message: "Job cancelled", job });
  }

  if (job.status === "PROCESSING") {
    // Hard case: a worker is actively running this job's handler RIGHT NOW,
    // in a separate process. We have no mechanism to reach into that
    // process and forcibly stop a JavaScript function mid-execution — the
    // best we can do is record the request and let the worker notice it
    // at its next checkpoint (when the handler eventually finishes).
    // This is "cooperative cancellation": we ask, we don't force.
    job.cancellationRequested = true;
    job.history.push({ status: job.status, note: "Cancellation requested while job was processing" });
    await job.save();

    publishEvent("JOB_CANCELLATION_REQUESTED", { jobId: job._id });
    return res.status(202).json({
      message: "Cancellation requested — job is currently processing and will be cancelled once the current attempt finishes",
      job,
    });
  }

  return res.status(409).json({ error: `Cannot cancel a job in status ${job.status}` });
}
