import { redisClient } from "../config/redis.js";
import { QUEUE_KEY, DELAYED_QUEUE_KEY } from "./queue.js";

// IMPORTANT: BZPOPMIN is a BLOCKING command — Redis will not respond to
// ANY other command sent on the same TCP connection until this one either
// finds a job or times out. If we ran this on the same shared `redisClient`
// used by heartbeat/promote/recovery/rate-limiter, all of THOSE commands
// would silently queue up behind it for the whole block duration — the
// exact "operations coupled to an unrelated blocking wait" bug we already
// found and fixed once for the worker's own loop timing, reappearing one
// layer down at the connection level. The fix is the same idea: give
// blocking calls their own dedicated connection.
export function createBlockingClient() {
  return redisClient.duplicate();
}

// timeoutSeconds: how long to block before returning null and looping again
// (returning periodically lets the worker check for shutdown signals etc.)
//
// blockingClient: a DEDICATED connection (see createBlockingClient above),
// not the shared redisClient — so this call's blocking wait never delays
// heartbeat, delayed-job promotion, stale-job recovery, or rate limiting.
//
// Returns: jobId (string) or null if the timeout elapsed with no job.
export async function claimNextJob(blockingClient, timeoutSeconds = 5) {
  const result = await blockingClient.bzpopmin(QUEUE_KEY, timeoutSeconds);
  // ioredis returns [key, member, score] on success, or null on timeout
  if (!result) {
    return null;
  }
  const [, jobId] = result;
  return jobId;
}

// Finds jobs in the delayed queue whose "ready-at" time has passed, and
// removes them from it. Returns the list of jobIds that THIS call actually
// promoted (not ones another worker already promoted).
//
// Why ZREM's return value matters: if multiple worker processes run this
// check concurrently, they might all fetch the same "due" jobId via
// ZRANGEBYSCORE before any of them removes it. ZREM is atomic — only the
// first caller to actually remove a given member gets a truthy result back.
// Everyone else gets 0, and we skip re-promoting it. This is the same
// "atomic remove, not just atomic read" principle as BZPOPMIN, applied here
// by hand since there's no single Redis command that does "pop everything
// below a score" atomically as one op.
export async function promoteDueDelayedJobs() {
  const now = Date.now();
  const dueJobIds = await redisClient.zrangebyscore(DELAYED_QUEUE_KEY, "-inf", now);

  const promoted = [];
  for (const jobId of dueJobIds) {
    const removed = await redisClient.zrem(DELAYED_QUEUE_KEY, jobId);
    if (removed) {
      promoted.push(jobId);
    }
  }
  return promoted;
}
