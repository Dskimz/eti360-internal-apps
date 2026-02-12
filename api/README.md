# ETI360 Internal API

Minimal FastAPI service intended to run on Render (no Docker).

UI styling is shared via `/static/eti360.css` (vendored from `@eti360/design-system`).

## Render settings

- **Runtime:** Python
- **Root Directory:** `api`
- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `uvicorn app.main:app --host 0.0.0.0 --port $PORT`

## Local dev (single env file)

Set up once:

1. Copy env template:
   - `cp .env.example .env.local`
2. Fill values in `.env.local`.
3. Run:
   - `./scripts/run_local.sh`

Notes:
- The launcher auto-loads `.env.local` (fallback `.env`) and starts Uvicorn on `http://127.0.0.1:8765`.
- `.env*` files are git-ignored (except templates), so secrets stay local.

## Environment variables

Required:

- `DATABASE_URL` (Render Postgres internal URL)
- `ETI360_API_KEY` (shared secret for admin/write endpoints; sent as `X-API-Key`)
- `OPENAI_API_KEY` (for icon classification + image generation)
- `AWS_REGION` (for icon S3 writes/download links)

Auth (recommended for browser UIs):

- `AUTH_MODE`:
  - `disabled` to bypass auth checks while building quickly
  - otherwise, session login is enabled for protected endpoints
- `AUTH_SCHEMA` (optional; default: `ops`) where users/sessions tables live
- `SESSION_TTL_DAYS` (optional; default: `30`) how long browser sessions last
- `SESSION_COOKIE_NAME` (optional; default: `eti360_session`)

Optional / for assets:

- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
- `S3_BUCKET`, `S3_PREFIX`
- `ICONS_S3_BUCKET` (default: `eti360-icons`)
- `ICON_CLASSIFIER_MODEL` (default: `gpt-4.1-mini`)
- `ICON_RENDERER_MODEL` (default: `gpt-image-1`)
- `ICON_CLASSIFIER_INPUT_USD_PER_1M` (default: `0.4`)
- `ICON_CLASSIFIER_OUTPUT_USD_PER_1M` (default: `1.6`)
- `ICON_RENDERER_USD_PER_IMAGE` (default: `0.04`)
- `ICON_RENDERER_INPUT_USD_PER_1M` (default: `0`)
- `ICON_RENDERER_OUTPUT_USD_PER_1M` (default: `0`)

Optional / for trip providers:

- `DIRECTORY_SCHEMA` (optional; default: `directory`) schema for trip provider tables
- `TRIP_PROVIDERS_EVIDENCE_S3_PREFIX` (optional; default: `trip_providers/evidence/`) S3 prefix for provider evidence markdown

Optional / for weather helpers:

- `GOOGLE_MAPS_API_KEY` (for Places lookup)
- `PERPLEXITY_API_KEY` (for auto-fetching monthly normals)
- `PERPLEXITY_MODEL` (default: `sonar-pro`)
- `PERPLEXITY_PROMPT_COST_PER_1M_USD`, `PERPLEXITY_COMPLETION_COST_PER_1M_USD` (optional; enables cost estimates)
- `OPENAI_API_KEY` (for chart title/subtitle generation)
- `OPENAI_PROMPT_COST_PER_1M_USD`, `OPENAI_COMPLETION_COST_PER_1M_USD` (optional; enables cost estimates)
  - Model-specific override supported: `OPENAI_<MODEL>_PROMPT_COST_PER_1M_USD` and `OPENAI_<MODEL>_COMPLETION_COST_PER_1M_USD`
  - Example for `gpt-5-mini`: `OPENAI_GPT_5_MINI_PROMPT_COST_PER_1M_USD` and `OPENAI_GPT_5_MINI_COMPLETION_COST_PER_1M_USD`
- `OPENAI_MODEL` (optional; default: `gpt-5-mini`) model used for OpenAI title/subtitle prompts
- `USAGE_SCHEMA` (optional; default: `ops`) shared schema for LLM run/usage logging across all apps

## Endpoints

- `GET /health`
- `GET /health/db`
- DB schema browser: `GET /db/ui` (and JSON helpers `GET /db/schemas`, `GET /db/tables`, `GET /db/columns`)
- Auth: `GET /login`, `POST /login`, `GET /logout` (cookie sessions)
- Admin users: `GET /admin/users/ui` (UI), `GET /admin/users`, `POST /admin/users` (requires admin access)
- Prompts: `GET /prompts/ui`, `GET /prompts/edit?prompt_key=...`, `GET /prompts/log/ui`, `GET /prompts`, `GET /prompts/item/{prompt_key}`, `POST /prompts/item/{prompt_key}`, `POST /prompts/seed`, `GET /prompts/required`
  - Prompts UI is read-only by default and grouped by `app_key` + `workflow`. It also shows cumulative token/cost stats per prompt key.
- `POST /admin/schema/init` (one-time DB schema init; requires `X-API-Key`)
- Weather UI: `GET /weather/ui`
- Weather automation: `POST /weather/auto_batch`
- Locations: `GET /weather/locations`
- Token/cost tracker: `GET /weather/usage`
- API usage log: `GET /usage/ui`, `GET /usage/log` (usage rows are attributed by `prompt_key`)
- Documents: `GET /documents/ui`, `GET /documents/list`, `POST /documents/upload`, `GET /documents/download/{doc_id}`, `POST /documents/delete/{doc_id}`
- Trip providers (research): `GET /trip_providers_research`, `GET /trip_providers_research/{provider_key}`, `GET /trip_providers_research/{provider_key}/evidence`
- `POST /icons/form/validate` (validate user input fields with governance constraints)
- `POST /icons/spec/validate` (fail-fast validation for LLM #1 strict icon intent JSON)
- `POST /icons/prompt/build` (deterministic prompt compilation + stable hashes)
- `GET /icons/models` (configured models + pricing estimate settings)
- `POST /icons/create` (run full pipeline: classify -> prompt -> render -> store)
- `POST /icons/create-batch` (parse multiline input and run per-line icon workflow)
- `GET /icons/list` (list icon assets with model usage and estimated costs)
- `POST /icons/{id}/recreate` (render again from stored prompt; new version)
- `DELETE /icons/{id}` (soft delete icon record)
- `GET /icons/{id}/download` (presigned S3 download redirect)
- `GET /icons/{id}/download-url` (presigned S3 URL as JSON for UI actions)
- `GET /icons/ui` (UI with entry form and icon table/actions)
