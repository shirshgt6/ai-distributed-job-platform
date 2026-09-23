import express from "express";
import swaggerUi from "swagger-ui-express";
import { isMongoHealthy } from "./config/db.js";
import { isRedisHealthy } from "./config/redis.js";
import { isKafkaHealthy } from "./kafka/producer.js";
import { swaggerSpec } from "./config/swagger.js";
import jobsRoutes from "./modules/jobs/jobs.routes.js";
import authRoutes from "./modules/auth/auth.routes.js";

export const app = express();

app.use(express.json());

// Interactive API docs at /api-docs — the spec itself is generated from the
// @openapi JSDoc comments sitting directly above each route (see
// auth.routes.js / jobs.routes.js), not hand-maintained separately.
app.use("/api-docs", swaggerUi.serve, swaggerUi.setup(swaggerSpec));

app.use("/api/auth", authRoutes);
app.use("/api/jobs", jobsRoutes);

// Combined check — what a load balancer / orchestrator (k8s liveness probe,
// e.g.) hits to decide "is this instance okay to receive traffic." Mongo and
// Redis are hard dependencies for the API's core job (submitting/reading
// jobs), so either being down means "degraded" here.
app.get("/health", async (req, res) => {
  const mongoOk = isMongoHealthy();
  const redisOk = await isRedisHealthy();
  const healthy = mongoOk && redisOk;

  res.status(healthy ? 200 : 503).json({
    status: healthy ? "ok" : "degraded",
    dependencies: {
      mongo: mongoOk ? "up" : "down",
      redis: redisOk ? "up" : "down",
    },
  });
});

// Per-dependency checks — useful when debugging WHICH dependency is down,
// without guessing from the combined check alone (an on-call engineer, or
// a monitoring dashboard, can poll these individually).
app.get("/health/mongodb", (req, res) => {
  const ok = isMongoHealthy();
  res.status(ok ? 200 : 503).json({ status: ok ? "up" : "down" });
});

app.get("/health/redis", async (req, res) => {
  const ok = await isRedisHealthy();
  res.status(ok ? 200 : 503).json({ status: ok ? "up" : "down" });
});

app.get("/health/kafka", async (req, res) => {
  const ok = await isKafkaHealthy();
  res.status(ok ? 200 : 503).json({ status: ok ? "up" : "down" });
});
