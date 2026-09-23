import mongoose from "mongoose";
import { env } from "./env.js";

export async function connectMongo() {
  await mongoose.connect(env.mongoUri);
  console.log("[mongo] connected");
}

export function isMongoHealthy() {
  // 1 = connected
  return mongoose.connection.readyState === 1;
}
