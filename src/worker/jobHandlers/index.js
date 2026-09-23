import { aiHandlers } from "./aiHandlers.js";

// Each handler simulates real work. In a real system these would call
// an email provider, an image-processing library, a report generator, etc.
// For this project we're simulating with delays — the queue/worker/locking
// mechanics are the point, not the actual "work" being done.

async function sendEmailHandler(payload) {
  await new Promise((resolve) => setTimeout(resolve, 1000));
  return { sent: true, to: payload.to };
}

async function resizeImageHandler(payload) {
  await new Promise((resolve) => setTimeout(resolve, 1500));
  return { resized: true, url: payload.url };
}

async function generateReportHandler(payload) {
  await new Promise((resolve) => setTimeout(resolve, 2000));
  return { reportGenerated: true, reportId: `report-${Date.now()}` };
}

// Deliberately fails every time — exists purely so we can observe the
// retry -> backoff -> DLQ flow happening for real. Not a real job type.
async function alwaysFailHandler(payload) {
  await new Promise((resolve) => setTimeout(resolve, 300));
  throw new Error("Simulated permanent failure for testing retry/DLQ flow");
}

// Deliberately takes LONGER than the worker's LOCK_TTL_MS (30s). Exists to
// demonstrate the recovery scan firing on a job that's slow but NOT
// actually dead — a real trade-off of TTL-based locks worth seeing happen.
async function slowJobHandler(payload) {
  await new Promise((resolve) => setTimeout(resolve, 35000));
  return { completed: true, note: "finished despite taking longer than the lock TTL" };
}

// Gives a comfortable ~8s window to manually Ctrl+C the worker mid-processing,
// to simulate a real crash for testing recovery.
async function crashTestHandler(payload) {
  await new Promise((resolve) => setTimeout(resolve, 8000));
  return { completed: true };
}

export const jobHandlers = {
  "send-email": sendEmailHandler,
  "resize-image": resizeImageHandler,
  "generate-report": generateReportHandler,
  "always-fail": alwaysFailHandler,
  "slow-job": slowJobHandler,
  "crash-test": crashTestHandler,
  // Real work, not simulated: calls Claude (see aiHandlers.js).
  ...aiHandlers,
};

export function getHandler(type) {
  const handler = jobHandlers[type];
  if (!handler) {
    const err = new Error(`No handler registered for job type: ${type}`);
    // No handler now means no handler on the next attempt either —
    // non-retryable, so the worker DLQs it immediately.
    err.retryable = false;
    throw err;
  }
  return handler;
}
