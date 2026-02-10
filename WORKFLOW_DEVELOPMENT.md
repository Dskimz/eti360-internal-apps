# Workflow development (local → Render)

This repo hosts multiple internal workflows behind a single FastAPI service (`api/`).

## Goals

- Build new workflows locally fast.
- Keep all UIs visually consistent (shared CSS).
- Make it easy to add new workflow pages without inventing new UI patterns.

## UI styling (required)

- All FastAPI HTML UIs should use the shared shell in `api/app/main.py` and load:
  - `GET /static/eti360.css` (vendored from `@eti360/design-system`).
- Use `body class="eti-app"` for internal tools (light gray page background).
- Add or update the project entry in `api/app/static/projects.json` so it appears in `/apps`.

## Adding a new workflow (suggested steps)

1. Add the workflow code under `api/app/<workflow_key>/...` (or `api/app/weather/...` for weather-related work).
2. Add endpoints in `api/app/main.py`:
   - `GET /<workflow_key>/ui` (human UI)
   - `POST /<workflow_key>/run` (write endpoint; protect with `X-API-Key` or auth)
   - `GET /<workflow_key>/status` (optional)
3. Build the UI using the shared shell:
   - `body_html` should use existing CSS classes: `card`, `btn`, `tablewrap`, `pill`, `statusbox`.
4. Persist shared state in Render Postgres (via `DATABASE_URL`):
   - Prefer one schema per workflow (e.g. `weather.*`, `directory.*`, etc.)
5. Deploy to Render and verify:
   - UI renders correctly
   - DB health (`GET /health/db`)
   - Admin/init endpoints only accessible when intended

## Local dev notes

- Keep `AUTH_MODE=disabled` during rapid iteration if needed, but do not leave admin/write endpoints unprotected on a public URL.
- For long-running tasks, prefer a job table + background worker instead of holding a single request open.
