import { Kafka } from "kafkajs";
import { env } from "../config/env.js";
import { logger } from "../utils/logger.js";

const kafka = new Kafka({
  clientId: "job-queue-system",
  brokers: [env.kafkaBroker],
  retry: { retries: 3 },
});

const producer = kafka.producer();
let isConnected = false;

export const JOB_EVENTS_TOPIC = "job-events";

async function ensureConnected() {
  if (!isConnected) {
    await producer.connect();
    isConnected = true;
    logger.info("kafka producer connected");
  }
}

// Real health check — actually talks to the broker via a short-lived admin
// connection, rather than just trusting our own `isConnected` flag (which
// only tells us "we connected successfully at some point in the past," not
// "the broker is reachable right now"). Same accuracy-vs-cost trade-off as
// isRedisHealthy() (real PING) vs isMongoHealthy() (cheap readyState check)
// — here we deliberately pick the accurate-but-slower option because Kafka
// is the dependency most likely to silently go stale (broker restarts,
// network partitions) without our long-lived producer connection noticing.
export async function isKafkaHealthy() {
  const admin = kafka.admin();
  try {
    await admin.connect();
    await admin.listTopics();
    return true;
  } catch (err) {
    logger.warn("kafka health check failed", { error: err.message });
    return false;
  } finally {
    await admin.disconnect().catch(() => {});
  }
}

// publishEvent is intentionally "fire and forget, but logged" — a Kafka
// outage must NEVER fail a job or an API request. Event streaming is a
// side channel for external consumers (analytics, notifications), not a
// dependency the core pipeline needs to function. This mirrors the same
// per-dependency reasoning we used for Redis at server startup: not every
// dependency deserves the same failure response.
export async function publishEvent(eventType, payload) {
  try {
    await ensureConnected();
    await producer.send({
      topic: JOB_EVENTS_TOPIC,
      messages: [
        {
          key: payload.jobId ? payload.jobId.toString() : undefined, // keys by jobId so Kafka preserves per-job ordering
          value: JSON.stringify({
            eventType,
            ...payload,
            emittedAt: new Date().toISOString(),
          }),
        },
      ],
    });
  } catch (err) {
    logger.error("kafka publish failed", { eventType, jobId: payload.jobId, error: err.message });
    // deliberately swallowed — see comment above
  }
}
