# ETI360 Internal Apps — Tech Stack (Vision + Current State)

Last updated: 2026-02-04

## Vision

Build multiple small internal tools (“apps”) that all run inside **one** backend service (“internal API”), sharing:

- A single Postgres database (`eti-db`) with clear per-app schemas.
- Shared auth, logging, and usage/cost tracking.
- Shared storage for generated artifacts (charts, exports) in S3.
- A simple home page (`/apps`) that links to each tool’s UI and operational pages.

## What we have today

### Hosting / runtime

- **Render Web Service (Python)** running **FastAPI + Uvicorn**
  - Source code: `eti360-internal-apps/api`
  - Start command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
  - Docs: `GET /docs`

### Data / persistence

- **Render Postgres**: `eti-db`
  - App uses `DATABASE_URL`
  - Data is organized by **schema** (namespace) rather than one giant set of tables:
    - Workflow data: `weather.*` (locations, datasets, monthly normals, assets)
    - Shared LLM tracking (for all apps): `ops.*` by default (`USAGE_SCHEMA` can override)
      - `ops.llm_runs`: one row per workflow run (workflow/kind/counts/time)
      - `ops.llm_usage`: one or more rows per run (provider/model/tokens/cost)
    - Shared auth + user management (for browser UIs): `ops.*` by default (`AUTH_SCHEMA` can override)
      - `ops.users`: users with roles (Admin / Account Manager), optional email
      - `ops.sessions`: cookie sessions for persistent login
  - Weather schema tables are created via `POST /admin/schema/init`.
  - Shared LLM tables are created on-demand when usage logging endpoints run.
  - Auth tables are created on-demand when login/admin endpoints run.

### Storage for generated assets

- **AWS S3** for generated PNGs
  - Bucket: `S3_BUCKET` (e.g. `eti360-assets-prod`)
  - Prefix: `S3_PREFIX` (optional; lets each app write into its own folder)
  - Images are uploaded via `boto3` and viewed via **pre-signed GET URLs** (time-limited).

### External APIs used (current)

- **Google Maps Places** (Text Search) to resolve a city string to:
  - `place_id`
  - canonical name + formatted address (used for display + slug)
  - `lat` / `lng`
- **Google Time Zone** API to resolve `timezone_id` from `lat/lng` (for daylight chart accuracy).
- **Perplexity API** to fetch monthly climate normals (high/low/precip) when the dataset doesn’t exist yet.

### UIs we serve

All UIs are served by the FastAPI service as simple HTML pages:

- `GET /apps` (also `GET /`) — “home page” linking to internal tools and operational pages.
- `GET /weather/ui` — Weather + Sunlight workflow UI (batch city input + table of generated links).
- `GET /usage/ui` — API usage/cost log UI (shared across all apps).
- `GET /db/ui` — DB schema browser (tables + fields).
- `GET /admin/users/ui` — user management (roles + emails).

## Code structure (repo)

Repository top-level contains both a static directory site and the Python API:

- `eti360-internal-apps/api` — FastAPI service (the “internal API”)
  - `eti360-internal-apps/api/app/main.py` — FastAPI app + routes
  - `eti360-internal-apps/api/app/weather/` — weather + sunlight pipeline modules
- `eti360-internal-apps/index.html`, `apps.json`, `ui_style.css`, `build.mjs` — static directory site (optional; separate from the internal API)

## Environment variables (core)

Required:

- `DATABASE_URL`

Recommended (auth):

- `ETI360_API_KEY` (required if auth is enabled)
- `AUTH_DISABLED=true` to disable `X-API-Key` checks (only if you accept public access risk)

S3 (for charts/assets):

- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `S3_BUCKET`
- `S3_PREFIX` (optional)

Weather tooling:

- `GOOGLE_MAPS_API_KEY`
- `PERPLEXITY_API_KEY`
- `PERPLEXITY_MODEL` (default used by code if unset)

Shared LLM usage schema:

- `USAGE_SCHEMA` (optional; default: `ops`)

Auth / sessions:

- `AUTH_MODE` (optional): set to `disabled` to bypass login checks during rapid iteration
- `AUTH_SCHEMA` (optional; default: `ops`)
- `SESSION_TTL_DAYS` (optional; default: `30`)
- `SESSION_COOKIE_NAME` (optional; default: `eti360_session`)

Cost estimation (optional; enables non-zero cost columns):

- `PERPLEXITY_PROMPT_COST_PER_1M_USD`
- `PERPLEXITY_COMPLETION_COST_PER_1M_USD`
- `OPENAI_PROMPT_COST_PER_1M_USD`
- `OPENAI_COMPLETION_COST_PER_1M_USD`
- `OPENAI_API_KEY` (optional; enables OpenAI title/subtitle generation)
- `OPENAI_MODEL` (optional; default: `gpt-5-mini`) model for OpenAI title/subtitle prompts

## Database approach (how to scale as apps grow)

Current pattern:

- Use one schema for a workflow (`weather.*`).
- Keep shared cross-cutting logging in one schema (`ops.*` by default).
- Keep shared cross-cutting auth/session tables in one schema (`ops.*` by default).
- Store “atomic” rows (e.g., one row per month) to make chart generation reproducible.
- Store generated assets in DB (`assets`) plus their S3 location (`s3_bucket`, `s3_key`).

Scaling guidance as you add more apps:

- Prefer **one schema per app** (e.g., `weather`, `flights`, `directory`, `etl`, etc.), or a clear table prefix if you keep one schema.
- Add indexes early for “lookup” fields and foreign keys (`*_id`, slugs, timestamps).

## “Will our current tech stack hinder building?”

Mostly no — the current stack is a solid foundation for adding more apps and tables. The main constraints to watch (and how to fix them) are:

1. **DB migrations are not managed like real migrations yet**
   - Risk: once tables exist, changing columns/types safely becomes error-prone.
   - Fix: add Alembic migrations (recommended) or a simple versioned migration runner table.

2. **Long-running workflows run inside a single request**
   - Risk: bigger batch jobs can time out or tie up the only worker.
   - Fix: introduce a “job” table + background worker and make UIs poll job status.

3. **Auth is currently optional**
   - Risk: if `AUTH_DISABLED=true` on a public URL, anyone can run jobs and incur API/S3 costs.
   - Fix: enable `X-API-Key`, add allowlisting/basic auth, or put the service behind an access gateway.

4. **Pre-signed URLs expire**
   - Risk: “permanent” links will eventually break.
   - Fix: store objects behind a controlled public path, or regenerate/presign on demand behind auth.

5. **Monolith can become messy without structure**
   - Risk: `main.py` grows and becomes hard to reason about.
   - Fix: move each app to its own router module (FastAPI `APIRouter`), keep shared utilities in `app/common/`.
