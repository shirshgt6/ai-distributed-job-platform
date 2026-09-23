import bcrypt from "bcrypt";
import jwt from "jsonwebtoken";
import { env } from "../../config/env.js";

const SALT_ROUNDS = 10; // cost factor for bcrypt — higher = slower to compute (good against brute force) but slower for real logins too. 10 is a standard balanced default.

const ACCESS_TOKEN_EXPIRY = "15m";
const REFRESH_TOKEN_EXPIRY = "7d";

export async function hashPassword(plainPassword) {
  return bcrypt.hash(plainPassword, SALT_ROUNDS);
}

export async function comparePassword(plainPassword, hash) {
  return bcrypt.compare(plainPassword, hash);
}

export function generateAccessToken(user) {
  return jwt.sign({ id: user._id, email: user.email }, env.jwtAccessSecret, {
    expiresIn: ACCESS_TOKEN_EXPIRY,
  });
}

export function generateRefreshToken(user) {
  return jwt.sign({ id: user._id }, env.jwtRefreshSecret, {
    expiresIn: REFRESH_TOKEN_EXPIRY,
  });
}

export function verifyAccessToken(token) {
  return jwt.verify(token, env.jwtAccessSecret); // throws if invalid/expired — caller must catch
}

export function verifyRefreshToken(token) {
  return jwt.verify(token, env.jwtRefreshSecret); // throws if invalid/expired — caller must catch
}
