# Duraflow Dashboard

Operations dashboard for the Duraflow durable workflow engine. A standalone
Next.js (App Router, TypeScript) app that talks to the Duraflow HTTP API for
monitoring runs, inspecting task/event history, starting new workflow runs,
and replaying dead-lettered tasks.

## Stack

- Next.js 14 (App Router) + React 18 + TypeScript
- Plain CSS (`app/globals.css`) — no UI kit or CSS framework
- Client-side data fetching with polling (no server-rendered API calls, no
  extra data-fetching libraries)

Dependencies are intentionally minimal: `next`, `react`, `react-dom`, and
TypeScript types only.

## Pages

- `/` — Overview: live stats, a form to start a new workflow run, and a
  recent-runs table.
- `/runs/[id]` — Run detail: summary, per-step task list, and an event
  history timeline. Includes a cancel button while the run is active.
- `/dlq` — Dead-letter queue: tasks that exhausted retries, with a per-row
  replay action.

All pages poll the API every 3 seconds via `setInterval` so the dashboard
stays live without a websocket dependency.

## Configuration

Copy `.env.example` to `.env.local` and point it at your Duraflow API:

```bash
cp .env.example .env.local
```

```
NEXT_PUBLIC_API_URL=http://localhost:8080
```

If unset, the app falls back to `http://localhost:8080`. The API client
lives in `lib/api.ts`.

## Local development

```bash
npm install
npm run dev
```

Visit http://localhost:3000. The dashboard will show a friendly error state
if the Duraflow API is unreachable, and will keep retrying on the polling
interval.

## Build

```bash
npm run build
npm start
```

## Deploying to Vercel

1. Push this directory (or the whole repo) to a Git provider Vercel can
   read from.
2. In Vercel, import the project and set the **root directory** to
   `dashboard/` if the repo contains other (non-Next.js) code alongside it.
3. Set the environment variable `NEXT_PUBLIC_API_URL` to your deployed
   Duraflow API URL (e.g. `https://duraflow.up.railway.app`) in the Vercel
   project settings.
4. Deploy. No additional build configuration is required — Vercel
   auto-detects Next.js.

## Project structure

```
dashboard/
  app/
    layout.tsx          Root layout, nav bar
    nav-bar.tsx          Top navigation (Overview | DLQ)
    globals.css          Global dark-theme styling
    page.tsx              Overview page
    runs/[id]/page.tsx    Run detail page
    dlq/page.tsx           DLQ page
  lib/
    api.ts                API client + types
    format.ts             Formatting helpers (dates, ids, status pills)
  package.json
  tsconfig.json
  next.config.mjs
  .env.example
```
