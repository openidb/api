# --- Stage 1: Install dependencies ---
FROM oven/bun:1-debian AS deps
WORKDIR /app

# Native build tools for better-sqlite3
RUN apt-get update && apt-get install -y python3 build-essential && rm -rf /var/lib/apt/lists/*

COPY package.json bun.lock* ./
RUN bun install --frozen-lockfile --production

# --- Stage 2: Build/prepare ---
FROM oven/bun:1-debian AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y python3 build-essential && rm -rf /var/lib/apt/lists/*

COPY package.json bun.lock* ./
RUN bun install --frozen-lockfile
COPY . .
RUN bunx prisma generate

# --- Stage 3: Production runner ---
FROM oven/bun:1-slim AS runner
WORKDIR /app

ENV NODE_ENV=production

COPY --from=deps /app/node_modules ./node_modules
COPY --from=builder /app/node_modules/.prisma ./node_modules/.prisma
COPY --from=builder /app/src ./src
COPY --from=builder /app/prisma ./prisma
COPY --from=builder /app/package.json ./

EXPOSE 4000

CMD ["bun", "src/index.ts"]
