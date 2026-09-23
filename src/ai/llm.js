// The one function AI job handlers call. Handlers don't know (or care)
// which model answered — this layer walks the fallback chain from
// env.aiModels until one succeeds.
//
// Two separate layers of "try again", each with its own job:
//   - FALLBACK (here, within seconds): model A is overloaded -> try model B
//     immediately, inside the same job attempt.
//   - RETRY (worker.js, job-level, with backoff): EVERY model failed ->
//     give the whole job back to the queue and try again later.
//
// Adding another provider later (OpenAI, Gemini) means one more client file
// and letting the chain hold {provider, model} pairs — handlers don't change.

import { env } from "../config/env.js";
import { logger } from "../utils/logger.js";
import { callClaude, AIError } from "./anthropicClient.js";

export async function generate({ system, prompt, maxTokens = 1024 }) {
  const failures = [];

  for (const model of env.aiModels) {
    try {
      const result = await callClaude({
        apiKey: env.anthropicApiKey,
        model,
        system,
        prompt,
        maxTokens,
        timeoutMs: env.aiRequestTimeoutMs,
      });

      return {
        ...result,
        fallbackUsed: model !== env.aiModels[0],
        failedModels: failures.map(({ model, error }) => ({ model, error })),
      };
    } catch (err) {
      failures.push({ model, error: err.message, retryable: err.retryable });

      // Non-retryable errors are about the REQUEST, not the model: a bad
      // prompt (400) or a bad API key (401) fails identically on every
      // model, so walking the chain would just waste calls. Stop now.
      //
      // One exception: 404 means "this model name doesn't exist" (typo, or
      // retired model). That IS model-specific — the next model in the
      // chain may well exist — so it keeps falling through.
      if (err.retryable === false && err.status !== 404) {
        throw err;
      }

      logger.warn("AI model failed, falling back to next model", {
        model,
        status: err.status,
        error: err.message,
      });
    }
  }

  // Every model failed. Whether the JOB should be retried later depends on
  // WHY: if at least one failure was temporary (overload, rate limit), a
  // later retry can succeed. If they were all permanent (every model 404),
  // retrying later changes nothing.
  const anyRetryable = failures.some((f) => f.retryable);
  throw new AIError(
    `All ${failures.length} models failed: ${failures.map((f) => `${f.model} -> ${f.error}`).join(" | ")}`,
    { retryable: anyRetryable }
  );
}
