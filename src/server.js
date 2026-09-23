import { app } from "./app.js";
import { env } from "./config/env.js";
import { connectMongo } from "./config/db.js";
import { logger } from "./utils/logger.js";

async function start() {
  await connectMongo();

  app.listen(env.port, () => {
    logger.info("API running", { port: env.port });
  });
}

start().catch((err) => {
  logger.error("server failed to start", { error: err.message, stack: err.stack });
  process.exit(1);
});
