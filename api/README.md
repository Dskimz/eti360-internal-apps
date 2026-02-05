# ETI360 Internal API

Minimal FastAPI service intended to run on Render (no Docker).

## Render settings

- **Runtime:** Python
- **Root Directory:** `api`
- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `uvicorn app.main:app --host 0.0.0.0 --port $PORT`

## Environment variables

Required:

- `DATABASE_URL` (Render Postgres internal URL)
- `ETI360_API_KEY` (shared secret for admin/write endpoints; sent as `X-API-Key`)

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
