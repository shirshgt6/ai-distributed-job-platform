import { redisClient } from "../config/redis.js";

const HEARTBEAT_PREFIX = "worker:heartbeat:";

// A worker "proves" it's alive by refreshing this key before it expires.
// We don't detect crashes directly — we detect the ABSENCE of a fresh
// heartbeat, which Redis's TTL gives us for free (no cleanup code needed:
// if nobody refreshes the key, Redis deletes it on its own).
export async function writeHeartbeat(workerId, ttlSeconds = 10) {
  await redisClient.set(`${HEARTBEAT_PREFIX}${workerId}`, Date.now().toString(), "EX", ttlSeconds);
}

// Starts an independent interval that keeps refreshing this worker's
// heartbeat, decoupled from whatever the main claim/process loop is doing.
export function startHeartbeatLoop(workerId, intervalMs = 3000, ttlSeconds = 10) {
  writeHeartbeat(workerId, ttlSeconds).catch((err) =>
    console.error(`[worker ${workerId}] initial heartbeat failed:`, err.message)
  );

  return setInterval(() => {
    writeHeartbeat(workerId, ttlSeconds).catch((err) =>
      console.error(`[worker ${workerId}] heartbeat write failed:`, err.message)
    );
  }, intervalMs);
}

export async function isWorkerAlive(workerId) {
  const val = await redisClient.get(`${HEARTBEAT_PREFIX}${workerId}`);
  return val !== null;
}
