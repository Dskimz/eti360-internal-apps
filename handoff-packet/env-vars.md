# Environment Variables

Set these on the Render **API service** unless noted.

## Core

- `DATABASE_URL` — Render Postgres connection string
- `ETI360_API_KEY` — shared secret used as `X-API-Key` for programmatic/admin bootstrap

Auth (recommended)

- `AUTH_MODE` — set to `disabled` during rapid iteration (bypasses login/role checks)
- `AUTH_SCHEMA` (optional, default: `ops`) — schema for `users` + `sessions`
- `SESSION_TTL_DAYS` (optional, default: `30`) — browser session TTL
- `SESSION_COOKIE_NAME` (optional, default: `eti360_session`)

## AWS S3 (assets)

- `AWS_REGION=ap-southeast-2`
- `AWS_ACCESS_KEY_ID` — IAM user access key (uploader)
- `AWS_SECRET_ACCESS_KEY` — IAM user secret (uploader)
- `S3_BUCKET=eti360-assets-prod`
- `S3_PREFIX=` (optional) — prefix for objects; recommended to keep per-app folders (e.g. `internal/`)

Optional:
- `S3_PREFIX` can be blank if you want paths like `{location_slug}/weather/{year}.png`

## External data providers (optional)

- `GOOGLE_MAPS_API_KEY` — required if the system will resolve Place IDs / timezone via Google
- `PERPLEXITY_API_KEY` — required if you want auto-fetching climate normals
- `PERPLEXITY_MODEL` (optional, default: `sonar-pro`)
- `OPENAI_API_KEY` — required if you want title/subtitle generation
- `OPENAI_MODEL` (optional, default: `gpt-5-mini`)

## Runtime defaults (rendering)

- `MPLCONFIGDIR=/tmp/matplotlib` — avoids Matplotlib writing to home dir

## Shared schemas (optional)

- `USAGE_SCHEMA` (optional, default: `ops`) — LLM token/cost logging tables
- `PROMPTS_SCHEMA` (optional, default: `ops`) — prompts + prompt revisions tables

## Cost configuration (optional)

If unset, costs will show as `$0.000000`.

Generic pricing (per 1M tokens):

- `PERPLEXITY_PROMPT_COST_PER_1M_USD`
- `PERPLEXITY_COMPLETION_COST_PER_1M_USD`
- `OPENAI_PROMPT_COST_PER_1M_USD`
- `OPENAI_COMPLETION_COST_PER_1M_USD`

Model-specific overrides (recommended when you use multiple models):

- `OPENAI_<MODEL>_PROMPT_COST_PER_1M_USD`
- `OPENAI_<MODEL>_COMPLETION_COST_PER_1M_USD`

Example for `gpt-5-mini`:

- `OPENAI_GPT_5_MINI_PROMPT_COST_PER_1M_USD`
- `OPENAI_GPT_5_MINI_COMPLETION_COST_PER_1M_USD`

## Documents (optional)

- `DOCS_MAX_UPLOAD_BYTES` (optional; default: `10485760`) — max upload size for `/documents/upload` (stored in Postgres)
