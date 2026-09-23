import { Router } from "express";
import { register, login, refresh, logout } from "./auth.controller.js";
import { rateLimiter } from "../../middlewares/rateLimiter.js";

const router = Router();

// Auth endpoints get their OWN, stricter rate limit — login/register are
// prime brute-force targets (someone guessing passwords repeatedly), so
// they deserve tighter limits than normal job submission.
const authLimiter = rateLimiter({ windowSeconds: 60, maxRequests: 5 });

/**
 * @openapi
 * /api/auth/register:
 *   post:
 *     tags: [Auth]
 *     summary: Create a new account
 *     security: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [email, password]
 *             properties:
 *               email: { type: string, example: user@example.com }
 *               password: { type: string, format: password, example: hunter2 }
 *     responses:
 *       201:
 *         description: Account created, tokens issued
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 accessToken: { type: string }
 *                 refreshToken: { type: string }
 *       400: { description: Missing/invalid fields, content: { application/json: { schema: { $ref: '#/components/schemas/Error' } } } }
 *       409: { description: Email already registered }
 */
router.post("/register", authLimiter, register);

/**
 * @openapi
 * /api/auth/login:
 *   post:
 *     tags: [Auth]
 *     summary: Log in and receive an access + refresh token pair
 *     security: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [email, password]
 *             properties:
 *               email: { type: string }
 *               password: { type: string, format: password }
 *     responses:
 *       200:
 *         description: Login successful
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 accessToken: { type: string }
 *                 refreshToken: { type: string }
 *       401:
 *         description: Invalid email or password (deliberately generic — prevents user enumeration)
 *         content: { application/json: { schema: { $ref: '#/components/schemas/Error' } } }
 */
router.post("/login", authLimiter, login);

/**
 * @openapi
 * /api/auth/refresh:
 *   post:
 *     tags: [Auth]
 *     summary: Exchange a valid refresh token for a new access + refresh token pair (rotation)
 *     security: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [refreshToken]
 *             properties:
 *               refreshToken: { type: string }
 *     responses:
 *       200:
 *         description: New token pair issued; the old refresh token is now invalid
 *         content:
 *           application/json:
 *             schema:
 *               type: object
 *               properties:
 *                 accessToken: { type: string }
 *                 refreshToken: { type: string }
 *       401:
 *         description: Refresh token invalid, expired, or already rotated/revoked
 *         content: { application/json: { schema: { $ref: '#/components/schemas/Error' } } }
 */
router.post("/refresh", authLimiter, refresh);

/**
 * @openapi
 * /api/auth/logout:
 *   post:
 *     tags: [Auth]
 *     summary: Invalidate the stored refresh token (server-side revocation)
 *     security: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             required: [refreshToken]
 *             properties:
 *               refreshToken: { type: string }
 *     responses:
 *       200:
 *         description: Always succeeds (idempotent) — the goal (no valid session) is already true even if the token was already invalid
 */
router.post("/logout", logout);

export default router;
