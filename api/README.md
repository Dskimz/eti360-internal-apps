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

## Endpoints

- `GET /health`
- `GET /health/db`
- `POST /admin/schema/init` (one-time DB schema init; requires `X-API-Key`)
