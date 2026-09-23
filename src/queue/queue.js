import { redisClient } from "../config/redis.js";

const QUEUE_KEY = "jobs:queue";
const DELAYED_QUEUE_KEY = "jobs:delayed";

// Lower score = processed first.
// We multiply priority by a large constant so it always dominates the
// timestamp component — priority ordering wins, timestamp just breaks
// ties within the same priority (oldest first).
const PRIORITY_WEIGHT = 1_000_000_000_000; // 1e12, bigger than any real timestamp offset we care about

export function computeScore(priority, createdAtMs = Date.now()) {
  return priority * PRIORITY_WEIGHT + createdAtMs;
}

// Push a job reference onto the queue. We only store the jobId —
// the queue is a pointer system, not a data store. Full job data lives in Mongo.
export async function pushJobToQueue(jobId, priority) {
  const score = computeScore(priority);
  await redisClient.zadd(QUEUE_KEY, score, jobId.toString());
}

// Push a job onto the DELAYED queue, scored purely by "ready-at" timestamp
// (not priority — this is a separate structure precisely so it doesn't
// collide with the main queue's priority-encoded scoring).
// Used for both retry backoff AND user-requested scheduled/delayed jobs.
export async function pushToDelayedQueue(jobId, readyAtMs) {
  await redisClient.zadd(DELAYED_QUEUE_KEY, readyAtMs, jobId.toString());
}

// Peek at queue length — useful for health/monitoring later.
export async function getQueueLength() {
  return redisClient.zcard(QUEUE_KEY);
}

// Remove a specific job from the main queue (used for cancellation) OR
// the delayed queue (a scheduled/retrying job that gets cancelled before
// its time comes). ZREM returns 1 if it actually removed something, 0 if
// the member wasn't there — the caller uses that to know whether this
// really was still QUEUED (removable) or had already been claimed by a
// worker (nothing left to remove, so cancellation must be handled
// differently — the cooperative flag path).
export async function removeJobFromQueue(jobId) {
  return redisClient.zrem(QUEUE_KEY, jobId.toString());
}

export async function removeJobFromDelayedQueue(jobId) {
  return redisClient.zrem(DELAYED_QUEUE_KEY, jobId.toString());
}

export { QUEUE_KEY, DELAYED_QUEUE_KEY };
