# Base image: Node 20 on a slim Debian base — small image size, but still
# has what native npm packages (like bcrypt, which compiles C++ code) need.
FROM node:20-slim

WORKDIR /app

# Copy ONLY package files first, not the whole codebase. Docker caches each
# instruction as a "layer" — if package.json hasn't changed, this `npm install`
# layer is reused from cache on the next build, even if our source code changed.
# If we copied everything first, ANY code change would invalidate the cache
# and force a full npm install every single build — much slower iteration.
COPY package*.json ./

RUN npm install --omit=dev

# Now copy the actual source code.
COPY . .

# Documents which port this container listens on. Doesn't actually publish
# it — that still happens via docker-compose's `ports:` mapping. This is
# metadata for humans/tools reading the Dockerfile.
EXPOSE 4000

# Default command if none is overridden — runs the API server.
# docker-compose overrides this for the worker service (see below).
CMD ["node", "src/server.js"]
