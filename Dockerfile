# syntax=docker/dockerfile:1

# ─── Build stage ────────────────────────────────────────────────────────────
FROM node:20-slim AS build
WORKDIR /app

# Install all deps (incl. dev: typescript, tsx) for the build
COPY package.json package-lock.json ./
RUN npm ci

# Compile TypeScript → dist/ (tsc emits dist/src/index.js per tsconfig)
COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# ─── Runtime stage ──────────────────────────────────────────────────────────
FROM node:20-slim AS runtime
WORKDIR /app
ENV NODE_ENV=production

# Production deps only
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force

# Compiled output from the build stage
COPY --from=build /app/dist ./dist

# App reads PORT from env (defaults to 3000 in src/index.ts).
# Make sure the platform's service port matches this.
EXPOSE 3000

# Runs `node dist/src/index.js` (no .env file — env vars come from the platform)
CMD ["node", "dist/src/index.js"]
