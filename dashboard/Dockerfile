# Dashboard image. Build context is the repository root (Railway uploads the
# repo), so all paths are prefixed with dashboard/. NEXT_PUBLIC_API_URL is a
# build-time arg because Next.js inlines NEXT_PUBLIC_* values at build time.
FROM node:20-slim AS build
WORKDIR /app
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci
COPY dashboard/ ./
ARG NEXT_PUBLIC_API_URL
ENV NEXT_PUBLIC_API_URL=$NEXT_PUBLIC_API_URL
ENV NEXT_TELEMETRY_DISABLED=1
RUN npm run build

FROM node:20-slim AS run
WORKDIR /app
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
COPY --from=build /app/package.json /app/package-lock.json ./
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/.next ./.next
COPY --from=build /app/next.config.mjs ./next.config.mjs
EXPOSE 3000
# Railway injects PORT; next start honours it.
CMD ["sh", "-c", "npm run start -- -p ${PORT:-3000}"]
