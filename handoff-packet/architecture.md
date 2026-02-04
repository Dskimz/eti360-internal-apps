# Architecture

## Goal

Provide a server-only system for an internal team to:

- Manage locations (Google Place ID, lat/lng, timezone)
- Store per-location weather inputs (12 monthly normals + provenance)
- Generate ETI360-branded chart assets (weather/daylight/map)
- Track LLM usage/cost and prompt edits over time
- Serve a UI via an API (no local scripts required)

## Components

### Render Web Service (FastAPI)

Responsibilities:

- Serves the internal UIs (`/apps`, `/weather/ui`, `/usage/ui`, `/prompts/ui`, `/db/ui`)
- Auth:
  - Cookie sessions + roles (`viewer`, `account_manager`, `admin`) when enabled
  - `X-API-Key` fallback for programmatic/admin bootstrap
  - Dev-mode bypass via `AUTH_MODE=disabled`
- CRUD for:
  - `weather.*` tables (locations, datasets, monthly normals, assets)
  - `ops.*` shared tables (LLM usage, users/sessions, prompts, prompt revisions)
- Generates charts synchronously (Matplotlib + Astral), uploads to S3, writes DB rows
- Generates pre-signed S3 URLs for viewing private assets (recommended)

### Render Postgres

Responsibilities:

- Source of truth for structured data
- Stores the source-of-truth inputs, asset metadata, prompt inventory, and audit logs

### AWS S3

Responsibilities:

- Stores generated PNG assets (not stored in Postgres)
- Example prefix: `s3://eti360-assets-prod/weather_graphs/...`

Recommended key format (stable + predictable):

`{prefix}/{location_slug}/{kind}/{year}.png`

Where:
- `prefix` is `S3_PREFIX` (optional; can be blank)
- `kind` is `weather|daylight|map`
- `year` is generated at run-time from the current year (UTC)

## Data flow (happy path)

1) User visits `/weather/ui`, pastes one city per line
2) UI calls `POST /weather/auto_batch`
3) API:
   - resolves Google Places (place_id + lat/lng)
   - fetches climate normals via Perplexity if not already in DB
   - optionally generates titles/subtitles via OpenAI (if configured)
   - renders PNGs (weather + daylight), uploads to S3, writes asset rows
   - logs LLM token usage/cost into `ops.llm_runs` + `ops.llm_usage`
4) UI refreshes `/weather/locations` and shows links to the new PNGs

## Prompt management + audit

- `/prompts/ui` lists prompts (DB source of truth), grouped by `app_key` + `workflow`, and links to details pages.
- `/prompts/edit?prompt_key=...` edits a prompt. Every save creates a row in `ops.prompt_revisions` with date/time/user/note and a before/after snapshot.
- `/prompts/log/ui` is the audit log UI.
