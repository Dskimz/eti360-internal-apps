# Runbooks

## 1) One-time DB init

Preferred: run `POST /admin/schema/init` (requires `X-API-Key`) once to create the `weather.*` tables.

The shared `ops.*` tables for auth/usage/prompts are created automatically the first time you use those features.

If you must do it from a machine:

- Use the Render Postgres “External Database URL” temporarily
- Apply `schema.sql`
- Re-lock access / rotate credentials if needed

## 2) Create a location

Two patterns:

- Simple: use the Weather UI (`/weather/ui`) and enter “City, Country”
- Assisted: system resolves Google Places + timezone and stores the canonical location in DB

## 3) Generate weather/daylight assets

1) UI calls `POST /weather/auto_batch` (one city per line)
2) API generates weather + daylight PNGs, uploads to S3, writes asset rows
3) UI refreshes `/weather/locations` and shows links

## 3b) Upload a document

- Open `/documents/ui`
- Choose folder + status (optional) and upload a markdown/file
- Use Download links to retrieve stored files

## 4) Debug a failed run

Check:

- API logs (Render) for validation errors
- `/usage/ui` for the run’s token/cost rows
- `/prompts/ui` for per-prompt token/cost totals (helps compare prompts)
- `/db/ui` to confirm tables exist and data is written

Common causes:

- missing env vars (`DATABASE_URL`, S3 vars, API keys)
- auth mismatch (`AUTH_MODE`, cookie sessions, `ETI360_API_KEY`)
- Matplotlib font/config issues (ensure `MPLCONFIGDIR=/tmp/matplotlib`)

## 5) Rotate secrets

### Rotate Postgres credentials

In Render Postgres:

- Reset/regenerate credentials
- Update `DATABASE_URL` env var on the API service
- Restart/redeploy the service

### Rotate AWS access keys

In AWS IAM:

- Create a new access key for the uploader user
- Update Render env vars on the API service
- Restart/redeploy the service
- Delete the old key

## 6) S3 access policy sanity check

Ensure the IAM policy scope matches:

- bucket: `eti360-assets-prod`
- prefix: whatever you set in `S3_PREFIX` (or lock to `*/weather/*` and `*/daylight/*`)

Avoid granting `s3:*` or access to all buckets.
