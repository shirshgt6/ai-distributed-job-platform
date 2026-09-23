// Thin wrapper around Anthropic's Messages API using plain fetch.
//
// WHY NOT THE OFFICIAL SDK: the SDK retries failed calls on its own (2 by
// default). Our job queue ALREADY retries with backoff at the job level, so
// stacking both gives 2 SDK retries × 3 job attempts = up to 9 paid calls
// for one failing job, with nobody able to see it from the job history.
// One retry policy, owned by the queue, is easier to reason about.
//
// This file does exactly two things: make ONE call, and turn every possible
// failure into an AIError that says whether retrying could possibly help.

const API_URL = "https://api.anthropic.com/v1/messages";
const API_VERSION = "2023-06-01";

export class AIError extends Error {
  constructor(message, { status = null, retryable }) {
    super(message);
    this.name = "AIError";
    this.status = status;
    // The worker's handleFailure() reads this flag. retryable === false
    // means "the same request will fail the same way every time" — so the
    // job goes straight to DEAD_LETTER instead of burning retries (and money).
    this.retryable = retryable;
  }
}

// Temporary problems (retry / fallback might work):
//   408 request timeout, 429 rate limited, 5xx server errors,
//   529 overloaded (Anthropic-specific — "too busy right now").
// Permanent problems (retrying is pointless):
//   400 bad request (bad prompt/params), 401 bad API key, 403 forbidden,
//   404 unknown model, 413 request too large.
function isRetryableStatus(status) {
  return status === 408 || status === 429 || status >= 500;
}

export async function callClaude({ apiKey, model, system, prompt, maxTokens = 1024, timeoutMs = 25000 }) {
  if (!apiKey) {
    // A missing key won't fix itself between retries — non-retryable.
    throw new AIError("ANTHROPIC_API_KEY is not configured", { retryable: false });
  }

  // REAL cancellation, unlike the generic Promise.race job timeout: aborting
  // the signal actually closes the HTTP connection. No zombie request keeps
  // running (and billing) in the background after we've given up on it.
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  let response;
  try {
    response = await fetch(API_URL, {
      method: "POST",
      headers: {
        "x-api-key": apiKey,
        "anthropic-version": API_VERSION,
        "content-type": "application/json",
      },
      body: JSON.stringify({
        model,
        max_tokens: maxTokens,
        system,
        messages: [{ role: "user", content: prompt }],
      }),
      signal: controller.signal,
    });
  } catch (err) {
    // fetch() itself only throws for network-level failures (DNS, connection
    // refused, reset) or our own abort. Both are temporary by nature.
    const reason = err.name === "AbortError" ? `timed out after ${timeoutMs}ms` : `network error: ${err.message}`;
    throw new AIError(`Claude call (${model}) ${reason}`, { retryable: true });
  } finally {
    clearTimeout(timer);
  }

  if (!response.ok) {
    // Error body looks like { type: "error", error: { type, message } }.
    // Parsing it is best-effort — a 502 from a proxy may not be JSON at all.
    let detail = "";
    try {
      const body = await response.json();
      detail = body?.error?.message || "";
    } catch {
      // ignore — status code alone is enough to classify
    }
    throw new AIError(`Claude API ${response.status} (${model})${detail ? `: ${detail}` : ""}`, {
      status: response.status,
      retryable: isRetryableStatus(response.status),
    });
  }

  const data = await response.json();

  // `content` is an array of blocks; with plain text prompts it's text blocks.
  const text = (data.content || [])
    .filter((block) => block.type === "text")
    .map((block) => block.text)
    .join("");

  return {
    text,
    model: data.model,
    // "max_tokens" means the answer was cut off mid-way. Callers that need
    // complete output (e.g. JSON extraction) should check this.
    stopReason: data.stop_reason,
    usage: {
      inputTokens: data.usage?.input_tokens ?? 0,
      outputTokens: data.usage?.output_tokens ?? 0,
    },
  };
}
