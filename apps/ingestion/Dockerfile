# Build context is apps/ingestion (package.json, package-lock.json, tsconfig.json and src/ live there).
# NOTE: node:22-slim is a floating tag. Pin it by digest (node:22-slim@sha256:<digest>) for
# reproducible builds; the digest is NOT VERIFIED here (it cannot be resolved offline and
# must not be guessed) - look it up with `docker buildx imagetools inspect node:22-slim`.
FROM node:22-slim AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src ./src
RUN npx tsc -p tsconfig.json

# Same floating tag / unverified digest note as above.
FROM node:22-slim
WORKDIR /app
ENV NODE_ENV=production
COPY package*.json ./
RUN npm ci --omit=dev
COPY --from=build /app/dist ./dist

# Run as the unprivileged user that ships with the node image. The app only reads /app
# and writes to S3 (local ./out output is for development runs outside the container).
USER node

# default command (override via ECS containerOverrides for full loads)
CMD ["node", "dist/index.js", "--mode", "incremental", "--page-size", "1000", "--concurrency", "5"]
