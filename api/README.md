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

Optional / for assets:

- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`
- `S3_BUCKET`, `S3_PREFIX`

Optional / for weather helpers:

- `GOOGLE_MAPS_API_KEY` (for Places lookup)
- `PERPLEXITY_API_KEY` (for auto-fetching monthly normals)
- `PERPLEXITY_MODEL` (default: `sonar-pro`)
- `PERPLEXITY_PROMPT_COST_PER_1M_USD`, `PERPLEXITY_COMPLETION_COST_PER_1M_USD` (optional; enables cost estimates)
- `OPENAI_PROMPT_COST_PER_1M_USD`, `OPENAI_COMPLETION_COST_PER_1M_USD` (optional; enables cost estimates)
- `OPENAI_MODEL` (optional; displayed in tracker; this app currently doesn't call OpenAI)

## Endpoints

- `GET /health`
- `GET /health/db`
- DB schema browser: `GET /db/ui` (and JSON helpers `GET /db/schemas`, `GET /db/tables`, `GET /db/columns`)
- `POST /admin/schema/init` (one-time DB schema init; requires `X-API-Key`)
- Weather UI: `GET /weather/ui`
- Weather automation: `POST /weather/auto_batch`
- Locations: `GET /weather/locations`
- Token/cost tracker: `GET /weather/usage`
