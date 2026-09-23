import mongoose from "mongoose";

const userSchema = new mongoose.Schema(
  {
    email: {
      type: String,
      required: true,
      unique: true, // Mongo-level guarantee: no two users can register the same email,
      lowercase: true, // normalize so "A@x.com" and "a@x.com" are treated as the same email
      trim: true,
    },
    passwordHash: {
      type: String,
      required: true, // we NEVER store the raw password, only its bcrypt hash
    },
    // Hashed refresh token currently valid for this user. Storing it lets
    // us REVOKE access (logout, or if we suspect compromise) by just
    // clearing this field — something a purely stateless token can't do.
    // Single field = one active session per user; a real multi-device
    // system would use an array instead (documented as a known simplification).
    refreshTokenHash: {
      type: String,
      default: null,
    },
  },
  { timestamps: true }
);

export const User = mongoose.model("User", userSchema);
