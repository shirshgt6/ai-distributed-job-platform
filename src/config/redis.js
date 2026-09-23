import Redis from "ioredis";
import { env } from "./env.js";

export const redisClient = new Redis({
  host: env.redisHost,
  port: env.redisPort,
  maxRetriesPerRequest: 3,
});

redisClient.on("connect", () => console.log("[redis] connected"));
redisClient.on("error", (err) => console.error("[redis] error:", err.message));

export async function isRedisHealthy() {
  try {
    const pong = await redisClient.ping();
    return pong === "PONG";
  } catch {
    return false;
  }
}
