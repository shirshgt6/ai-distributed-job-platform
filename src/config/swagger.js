import swaggerJSDoc from "swagger-jsdoc";

// WHY swagger-jsdoc instead of hand-writing one giant JSON/YAML spec file:
// the spec lives as JSDoc comments directly above each route (see
// jobs.routes.js / auth.routes.js). When someone changes a route, the docs
// are sitting right there to update too — a separate spec file almost always
// drifts out of sync with the actual code because nothing forces you to
// touch both at once.
const swaggerDefinition = {
  openapi: "3.0.0",
  info: {
    title: "Distributed Job Queue System API",
    version: "1.0.0",
    description:
      "A distributed job/task processing system: submit jobs, they queue in Redis, workers process them with retries, backoff, dead-lettering, and cooperative cancellation. Auth is JWT-based (access + refresh tokens).",
  },
  servers: [{ url: "http://localhost:4000", description: "Local dev" }],
  components: {
    securitySchemes: {
      bearerAuth: {
        type: "http",
        scheme: "bearer",
        bearerFormat: "JWT",
        description: "Access token returned from /api/auth/login or /api/auth/register",
      },
    },
    schemas: {
      Job: {
        type: "object",
        properties: {
          _id: { type: "string" },
          userId: { type: "string" },
          type: { type: "string", example: "send-email" },
          payload: { type: "object" },
          status: {
            type: "string",
            enum: ["QUEUED", "PROCESSING", "COMPLETED", "FAILED", "RETRYING", "DEAD_LETTER", "CANCELLED"],
          },
          priority: { type: "number", example: 5 },
          attempts: { type: "number" },
          maxAttempts: { type: "number" },
          timeoutMs: { type: "number" },
          idempotencyKey: { type: "string" },
          scheduledFor: { type: "string", format: "date-time", nullable: true },
          result: { type: "object", nullable: true },
          error: { type: "string", nullable: true },
          history: { type: "array", items: { type: "object" } },
          createdAt: { type: "string", format: "date-time" },
          updatedAt: { type: "string", format: "date-time" },
        },
      },
      Error: {
        type: "object",
        properties: {
          error: { type: "string" },
        },
      },
    },
  },
  // Every route protected by `authenticate` needs a token — set globally
  // here so we don't repeat `security:` on every single route below.
  security: [{ bearerAuth: [] }],
};

export const swaggerSpec = swaggerJSDoc({
  swaggerDefinition,
  // Files swagger-jsdoc scans for @openapi JSDoc blocks.
  apis: ["./src/modules/**/*.routes.js"],
});
