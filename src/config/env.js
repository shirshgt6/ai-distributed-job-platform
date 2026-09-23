import dotenv from "dotenv";
dotenv.config();

const required = ["MONGO_URI", "REDIS_HOST", "REDIS_PORT"];

for (const key of required) {
  if (!process.env[key]) {
    throw new Error(`Missing required env var: ${key}`);
  }
}

export const env = {
  port: process.env.PORT || 4000,
  nodeEnv: process.env.NODE_ENV || "development",
  mongoUri: process.env.MONGO_URI,
  redisHost: process.env.REDIS_HOST,
  redisPort: Number(process.env.REDIS_PORT),
  jwtAccessSecret: process.env.JWT_ACCESS_SECRET,
  jwtRefreshSecret: process.env.JWT_REFRESH_SECRET,
  kafkaBroker: process.env.KAFKA_BROKER || "localhost:9092",
  // How many jobs ONE worker process handles in parallel. Safe to raise for
  // I/O-heavy handlers (network calls, DB queries) since Node's event loop
  // stays free during the "waiting" part of I/O — CPU-heavy handlers won't
  // benefit the same way since they actually block the event loop.
  workerConcurrency: Number(process.env.WORKER_CONCURRENCY || 1),

  // AI settings. Deliberately NOT in the `required` list above: the API and
  // non-AI job types must keep working without a key. Only AI jobs fail
  // (non-retryable, straight to DLQ) when it's missing.
  anthropicApiKey: process.env.ANTHROPIC_API_KEY || null,
  // Ordered fallback chain — first entry is tried first. Comma-separated
  // so the order can be changed per environment without a code change.
  aiModels: (process.env.AI_MODELS || "claude-sonnet-5,claude-haiku-4-5-20251001")
    .split(",")
    .map((m) => m.trim())
    .filter(Boolean),
  // Per-HTTP-call timeout. The WHOLE fallback chain (models × this value)
  // has to fit inside the job's timeoutMs, otherwise the job-level
  // Promise.race timeout fires first and the fallback model never gets its
  // turn. That's why AI jobs get a bigger default timeoutMs (60s, see
  // jobs.controller.js) than ordinary jobs (30s): 2 models × 25s = 50s.
  aiRequestTimeoutMs: Number(process.env.AI_REQUEST_TIMEOUT_MS || 25000),
};
