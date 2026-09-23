import mongoose from "mongoose";

const historyEntrySchema = new mongoose.Schema(
  {
    status: { type: String, required: true },
    timestamp: { type: Date, default: Date.now },
    workerId: { type: String, default: null },
    note: { type: String, default: null },
  },
  { _id: false }
);

const jobSchema = new mongoose.Schema(
  {
    userId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User",
      required: true,
    },
    type: {
      type: String,
      required: true, // e.g. "send-email", "resize-image", "generate-report"
    },
    payload: {
      type: mongoose.Schema.Types.Mixed,
      default: {},
    },
    status: {
      type: String,
      enum: [
        "QUEUED",
        "PROCESSING",
        "COMPLETED",
        "FAILED",
        "RETRYING",
        "DEAD_LETTER",
        "CANCELLED",
      ],
      default: "QUEUED",
      index: true,
    },
    priority: {
      type: Number,
      default: 5, // lower number = higher priority
    },
    attempts: {
      type: Number,
      default: 0,
    },
    maxAttempts: {
      type: Number,
      default: 3,
    },
    // Per-JOB timeout (distinct from the worker's lock TTL, which exists
    // to detect a dead/hung WORKER via another worker's recovery scan —
    // slower, and worker-scoped). This is a faster, job-scoped mechanism:
    // the SAME worker processing this job gives up on waiting for it after
    // this many ms, without needing another worker to notice anything.
    timeoutMs: {
      type: Number,
      default: 30000,
    },
    idempotencyKey: {
      type: String,
      required: true,
      unique: true,
    },
    lockedBy: {
      type: String,
      default: null,
    },
    lockExpiresAt: {
      type: Date,
      default: null,
    },
    scheduledFor: {
      type: Date,
      default: null,
    },
    // Cooperative cancellation flag. If a job is PROCESSING when cancel is
    // requested, we can't forcibly kill the running handler mid-execution —
    // JavaScript doesn't let us interrupt an in-flight async function from
    // outside it. So we just mark intent here; the worker checks this flag
    // at its next natural checkpoint (after the handler finishes) and skips
    // saving a COMPLETED result if cancellation was requested meanwhile.
    cancellationRequested: {
      type: Boolean,
      default: false,
    },
    result: {
      type: mongoose.Schema.Types.Mixed,
      default: null,
    },
    error: {
      type: String,
      default: null,
    },
    startedAt: { type: Date, default: null },
    completedAt: { type: Date, default: null },
    history: {
      type: [historyEntrySchema],
      default: [],
    },
  },
  { timestamps: true } // adds createdAt / updatedAt automatically
);

export const Job = mongoose.model("Job", jobSchema);
