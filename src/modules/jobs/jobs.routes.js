import { Router } from "express";
import { submitJob, getJobStatus, cancelJob } from "./jobs.controller.js";
import { rateLimiter } from "../../middlewares/rateLimiter.js";
import { authenticate } from "../../middlewares/authenticate.js";

const router = Router();

// authenticate runs FIRST — it must set req.user before rateLimiter runs,
// since rateLimiter prefers req.user.id over req.ip when available.
// Order matters here: authenticate -> rateLimiter -> controller.

/**
 * @openapi
 * /api/jobs:
 *   post:
 *     tags: [Jobs]
 *     summary: Submit a new job to the queue
 *     description: >
 *       Pushes the job into Redis for a worker to pick up. If `scheduledFor`
 *       is a future timestamp, the job is held in the delayed queue instead
 *       and only becomes eligible once that time arrives.
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [type, idempotencyKey]
 *             properties:
 *               type:
 *                 type: string
 *                 example: ai-summarize
 *                 description: "send-email | resize-image | generate-report | ai-summarize | ai-classify | ai-extract (plus test types: always-fail, slow-job, crash-test)"
 *               payload:
 *                 type: object
 *                 description: "ai-summarize: {text, maxWords?} · ai-classify: {text, labels[]} · ai-extract: {text, fields[]}"
 *                 example: { text: "Long article text here...", maxWords: 50 }
 *               priority: { type: number, example: 5, description: "Lower number = higher priority" }
 *               idempotencyKey: { type: string, description: "Unique key — resubmitting the same key returns the existing job instead of creating a duplicate" }
 *               maxAttempts: { type: number, example: 3 }
 *               timeoutMs: { type: number, example: 30000, description: "Defaults to 30000, or 60000 for ai-* jobs" }
 *               scheduledFor: { type: string, format: date-time, description: "Optional future time; omit to run as soon as a worker is free" }
 *     responses:
 *       201:
 *         description: Job submitted (or scheduled)
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 message: { type: string }
 *                 jobId: { type: string }
 *                 status: { type: string }
 *                 scheduledFor: { type: string, nullable: true }
 *       200: { description: "Job with this idempotencyKey already exists — returned as-is, nothing new created" }
 *       400: { description: Missing type or idempotencyKey }
 *       401: { description: Missing/invalid access token }
 *       429: { description: Rate limit exceeded (10 submissions/60s per user) }
 *       503: { description: Redis unavailable — job queueing is a hard dependency, so this route fails closed }
 */
router.post("/", authenticate, rateLimiter({ windowSeconds: 60, maxRequests: 10 }), submitJob);

/**
 * @openapi
 * /api/jobs/{id}:
 *   get:
 *     tags: [Jobs]
 *     summary: Get a job's current status and history
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200:
 *         description: Job found
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 job: { $ref: '#/components/schemas/Job' }
 *       401: { description: Missing/invalid access token }
 *       403: { description: "Job belongs to a different user (IDOR protection)" }
 *       404: { description: Job not found }
 */
router.get("/:id", authenticate, getJobStatus);

/**
 * @openapi
 * /api/jobs/{id}/cancel:
 *   post:
 *     tags: [Jobs]
 *     summary: Cancel a job
 *     description: >
 *       QUEUED/RETRYING jobs are cancelled immediately (removed from Redis).
 *       A PROCESSING job can't be forcibly stopped mid-execution — cancellation
 *       is only *requested*, and honored cooperatively once the worker's
 *       current handler call finishes (202 response). Terminal-state jobs
 *       (COMPLETED/FAILED/DEAD_LETTER/CANCELLED) can't be cancelled (409).
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema: { type: string }
 *     responses:
 *       200: { description: "Cancelled immediately (was QUEUED or RETRYING)" }
 *       202: { description: "Cancellation requested — job was PROCESSING, will be honored once the current attempt finishes" }
 *       401: { description: Missing/invalid access token }
 *       403: { description: "Job belongs to a different user" }
 *       404: { description: Job not found }
 *       409: { description: "Job is already in a terminal state and can't be cancelled" }
 */
router.post("/:id/cancel", authenticate, cancelJob);

export default router;
