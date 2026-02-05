# API Contract (current)

This is the current contract used by the internal apps UI.

## Auth

- **Dev mode:** set `AUTH_MODE=disabled` to bypass auth checks while building.
- **Session auth:** when enabled, browser login uses a cookie session.
- **API key fallback:** `X-API-Key: <ETI360_API_KEY>` can be used for admin bootstrap and programmatic calls.

Role model (most common):

- `viewer`: read-only (locations list, usage log, prompt inventory)
- `editor`: can upload/delete documents and perform other “content” writes
- `account_manager`: can manage workflows depending on policy
- `admin`: can manage users and view DB schema

## Health

- `GET /health`
  - returns `{ "ok": true }`
- `GET /health/db`
  - returns `{ "ok": true }` or `{ "detail": "..." }`

## Apps home

- `GET /apps` (also `/`)
  - directory page that links to internal tools

## Weather + Sunlight

- `GET /weather/ui`
  - internal UI
- `POST /weather/auto_batch`
  - body: `{ "locations": ["Lima, Peru", "Nagasaki, Japan"], "force_refresh": false }`
  - returns run id, results, usage rows, and costs
- `GET /weather/locations?limit=500&order=alpha`
  - returns saved locations and the latest weather/daylight links

## API usage (LLM tokens/cost)

- `GET /usage/ui` (UI)
- `GET /usage/log?limit=500` (JSON)
- `GET /weather/usage` (JSON summary)

## Prompts + audit log

- `GET /prompts/ui` (UI inventory table; read-only “used prompts” + per-prompt usage stats)
- `GET /prompts/edit?prompt_key=...` (UI details; read-only by default)
- `GET /prompts/log/ui` (UI audit log)
- `POST /prompts/seed` (reconciles/creates required prompts; idempotent)
- `GET /prompts` (JSON list)
- `GET /prompts/item/{prompt_key}` (JSON)
- `POST /prompts/item/{prompt_key}` (JSON upsert; logs a revision)

Notes:

- Prompt edits are intentionally not “day-to-day” via UI. If you need to change prompts, prefer `POST /prompts/item/{prompt_key}` (audit logged).
- To allow editing via the UI, set `PROMPTS_UI_EDITING=enabled` (otherwise `/prompts/edit` is view-only).

Prompts are grouped by:

- `app_key` (e.g. `weather`, `directory`, `flights`)
- `workflow` (e.g. `weather`, `sunlight`)

## Usage log

- `GET /usage/ui` (UI token/cost log)
- `GET /usage/log` (JSON)

Each usage row includes:

- `run_id`, `workflow`, `kind`, `created_at`
- `prompt_key` (which prompt produced the usage row)
- `provider`, `model`
- `prompt_tokens` (in), `completion_tokens` (out), `total_tokens`, `cost_usd`

## Documents

- `GET /documents/ui` (UI upload + browse)
- `GET /documents/list` (JSON list)
- `POST /documents/upload` (multipart upload; stored in S3, metadata in Postgres)
- `GET /documents/view/{doc_id}` (preview; inline when possible)
- `GET /documents/download/{doc_id}` (download)
- `POST /documents/delete/{doc_id}` (delete)

## DB schema browser (admin)

- `GET /db/ui` (UI)
- `GET /db/schemas`, `GET /db/tables`, `GET /db/columns` (JSON)

## Admin (users + schema)

- `GET /admin/users/ui` (UI)
- `GET /admin/users` (JSON list)
- `POST /admin/users` (JSON create)
- `POST /admin/schema/init` (creates `weather.*` schema tables)
