import { User } from "../../models/user.model.js";
import {
  hashPassword,
  comparePassword,
  generateAccessToken,
  generateRefreshToken,
  verifyRefreshToken,
} from "./auth.utils.js";

// POST /api/auth/register
export async function register(req, res) {
  const { email, password } = req.body;

  if (!email || !password) {
    return res.status(400).json({ error: "email and password are required" });
  }

  const existing = await User.findOne({ email: email.toLowerCase() });
  if (existing) {
    return res.status(409).json({ error: "An account with this email already exists" });
  }

  const passwordHash = await hashPassword(password);
  const user = await User.create({ email, passwordHash });

  const accessToken = generateAccessToken(user);
  const refreshToken = generateRefreshToken(user);

  // Store a HASH of the refresh token, not the raw token itself — same
  // principle as password storage. If our database were ever leaked, raw
  // refresh tokens would let an attacker impersonate every user directly.
  user.refreshTokenHash = await hashPassword(refreshToken);
  await user.save();

  return res.status(201).json({
    message: "Registered successfully",
    accessToken,
    refreshToken,
  });
}

// POST /api/auth/login
export async function login(req, res) {
  const { email, password } = req.body;

  if (!email || !password) {
    return res.status(400).json({ error: "email and password are required" });
  }

  const user = await User.findOne({ email: email.toLowerCase() });

  // Deliberately the SAME error message whether the email doesn't exist or
  // the password is wrong. If we said "email not found" vs "wrong password"
  // separately, an attacker could use that to discover which emails are
  // registered — a real information-leak vulnerability.
  if (!user) {
    return res.status(401).json({ error: "Invalid email or password" });
  }

  const passwordMatches = await comparePassword(password, user.passwordHash);
  if (!passwordMatches) {
    return res.status(401).json({ error: "Invalid email or password" });
  }

  const accessToken = generateAccessToken(user);
  const refreshToken = generateRefreshToken(user);

  user.refreshTokenHash = await hashPassword(refreshToken);
  await user.save();

  return res.status(200).json({
    message: "Logged in successfully",
    accessToken,
    refreshToken,
  });
}

// POST /api/auth/refresh
export async function refresh(req, res) {
  const { refreshToken } = req.body;

  if (!refreshToken) {
    return res.status(400).json({ error: "refreshToken is required" });
  }

  let payload;
  try {
    payload = verifyRefreshToken(refreshToken);
  } catch {
    return res.status(401).json({ error: "Invalid or expired refresh token" });
  }

  const user = await User.findById(payload.id);
  if (!user || !user.refreshTokenHash) {
    return res.status(401).json({ error: "Invalid refresh token" });
  }

  // Not just "is this JWT valid" — also "is this THE token we currently
  // have on file for this user." If the user logged out (refreshTokenHash
  // cleared) or logged in elsewhere (refreshTokenHash overwritten), an
  // old-but-still-cryptographically-valid refresh token must NOT keep working.
  // This is the revocability that pure stateless JWTs can't give you.
  const matches = await comparePassword(refreshToken, user.refreshTokenHash);
  if (!matches) {
    return res.status(401).json({ error: "Refresh token has been revoked" });
  }

  const newAccessToken = generateAccessToken(user);
  // Rotate the refresh token too, not just the access token — limits how
  // long a stolen (but not yet used) refresh token stays valid.
  const newRefreshToken = generateRefreshToken(user);
  user.refreshTokenHash = await hashPassword(newRefreshToken);
  await user.save();

  return res.status(200).json({
    accessToken: newAccessToken,
    refreshToken: newRefreshToken,
  });
}

// POST /api/auth/logout
export async function logout(req, res) {
  const { refreshToken } = req.body;
  if (!refreshToken) {
    return res.status(400).json({ error: "refreshToken is required" });
  }

  try {
    const payload = verifyRefreshToken(refreshToken);
    await User.findByIdAndUpdate(payload.id, { refreshTokenHash: null });
  } catch {
    // Token already invalid/expired — logout's goal (no valid session) is
    // already achieved either way, so we don't treat this as an error.
  }

  return res.status(200).json({ message: "Logged out successfully" });
}
