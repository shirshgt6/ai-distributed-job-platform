// A minimal structured logger — not winston/pino, deliberately. This
// project doesn't need log rotation, multiple transports, or the extra
// dependency weight; it needs ONE thing: every log line as a single-line
// JSON object with consistent fields, so a real log aggregator (or even
// `grep` + `jq` on raw output) can filter and query it reliably.
//
// Plain console.log("[worker X] did Y") is fine to read by eye in a
// terminal, but "find every ERROR log for jobId=abc123 in the last hour"
// is trivial against JSON lines and painful against free-form text.

function log(level, message, meta = {}) {
  const entry = {
    timestamp: new Date().toISOString(),
    level,
    message,
    ...meta,
  };
  // console.log/error still used underneath — we're not replacing HOW logs
  // are emitted (stdout/stderr, which is standard for containerized apps;
  // Docker/Kubernetes capture stdout automatically), just their SHAPE.
  const output = JSON.stringify(entry);
  if (level === "error") {
    console.error(output);
  } else {
    console.log(output);
  }
}

export const logger = {
  info: (message, meta) => log("info", message, meta),
  warn: (message, meta) => log("warn", message, meta),
  error: (message, meta) => log("error", message, meta),
};
