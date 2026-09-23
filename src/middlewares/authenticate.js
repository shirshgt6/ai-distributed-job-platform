import { verifyAccessToken } from "../modules/auth/auth.utils.js";

// Protects a route: requires a valid access token in the Authorization
// header. On success, attaches { id, email } to req.user so downstream
// controllers know WHO is making the request — this is what lets
// jobs.controller.js stop trusting a client-supplied userId and use the
// verified identity instead.
export function authenticate(req, res, next) {
  const authHeader = req.headers.authorization; // expected format: "Bearer <token>"

  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    return res.status(401).json({ error: "Missing or malformed Authorization header" });
  }

  const token = authHeader.split(" ")[1];

  try {
    const payload = verifyAccessToken(token); // throws if invalid, expired, or tampered with
    req.user = { id: payload.id, email: payload.email };
    next();
  } catch (err) {
    return res.status(401).json({ error: "Invalid or expired access token" });
  }
}
