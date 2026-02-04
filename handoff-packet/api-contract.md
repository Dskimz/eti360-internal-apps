# API Contract (current)

This is the current contract used by the internal apps UI.

## Auth

- **Dev mode:** set `AUTH_MODE=disabled` to bypass auth checks while building.
- **Session auth:** when enabled, browser login uses a cookie session.
- **API key fallback:** `X-API-Key: <ETI360_API_KEY>` can be used for admin bootstrap and programmatic calls.

Role model (most common):

- `viewer`: read-only (locations list, usage log, prompt inventory)
- `account_manager`: can edit prompts/users depending on policy
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

- `GET /prompts/ui` (UI inventory table)
- `GET /prompts/edit?prompt_key=...` (UI details/editor)
- `GET /prompts/log/ui` (UI audit log)
- `POST /prompts/seed` (creates default prompts)
- `GET /prompts` (JSON list)
- `GET /prompts/item/{prompt_key}` (JSON)
- `POST /prompts/item/{prompt_key}` (JSON upsert; logs a revision)

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

## DB schema browser (admin)

- `GET /db/ui` (UI)
- `GET /db/schemas`, `GET /db/tables`, `GET /db/columns` (JSON)

## Admin (users + schema)

- `GET /admin/users/ui` (UI)
- `GET /admin/users` (JSON list)
- `POST /admin/users` (JSON create)
- `POST /admin/schema/init` (creates `weather.*` schema tables)
