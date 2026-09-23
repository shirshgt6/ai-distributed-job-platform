import { redisClient } from "../config/redis.js";

// Fixed-window rate limiter, backed by Redis.
//
// Key: "ratelimit:<identifier>" where identifier is the authenticated
// user's id if available (req.user.id, once JWT auth is wired in), else
// falls back to IP — so even unauthenticated routes get some protection.
//
// windowSeconds: how long one counting window lasts (e.g. 60 = per minute)
// maxRequests: how many requests allowed inside that window
export function rateLimiter({ windowSeconds = 60, maxRequests = 10 } = {}) {
  return async function rateLimitMiddleware(req, res, next) {
    const identifier = req.user?.id || req.ip;
    const key = `ratelimit:${identifier}`;

    try {
      // INCR is atomic — even if many requests from the same user hit this
      // at the exact same instant, Redis serializes the increments, so the
      // count is always correct (no two requests can "read the same value
      // and both think they're under the limit" — the same class of race
      // we solved with BZPOPMIN/ZREM in the queue).
      const count = await redisClient.incr(key);

      if (count === 1) {
        // First request in a fresh window — start the TTL clock now.
        // Only the request that actually created the key sets the expiry,
        // so later requests in the same window don't keep pushing it out.
        await redisClient.expire(key, windowSeconds);
      }

      if (count > maxRequests) {
        const ttl = await redisClient.ttl(key);
        return res.status(429).json({
          error: "Too many requests, please slow down",
          retryAfterSeconds: ttl > 0 ? ttl : windowSeconds,
        });
      }

      next();
    } catch (err) {
      // FAIL OPEN: if Redis itself is unreachable, we let the request
      // through rather than blocking every user in the system because our
      // own rate-limit check couldn't run. Same per-dependency reasoning
      // we used for Redis at server startup — this endpoint's core function
      // (submitting a job) doesn't have to die just because a secondary
      // protection mechanism is temporarily down.
      console.error("[rateLimiter] Redis error, failing open:", err.message);
      next();
    }
  };
}
