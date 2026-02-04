# Decisions & Best Practices

## Locked decisions (current)

- Render-first: everything runs on Render (no Docker required).
- Sanity is used only for marketing website content (separate domain).
- S3 stores PNG **assets**; Postgres stores metadata + S3 keys.
- DB uses schemas for grouping:
  - `weather.*` for workflow data
  - `ops.*` for shared cross-cutting concerns (auth, prompts, usage logging)
- Prompt edits are audited (history tables exist by design).
  - `ops.prompts` stores the current prompt text + metadata (grouped by `app_key` + `workflow`)
  - `ops.prompt_revisions` stores an immutable audit trail (UTC timestamp + user + note + before/after)

## Best practices to keep

- One system of record per domain (avoid Sanity/Postgres overlap).
- Keep write endpoints protected:
  - dev mode bypass: `AUTH_MODE=disabled`
  - otherwise: cookie sessions + roles
  - API key fallback for bootstrap/programmatic calls
- Use idempotency:
  - Upsert location by `location_id` and/or `place_id`
  - For assets, allow multiple runs but include `generated_at` and (optionally) a unique key for “latest”
- Prefer private S3 bucket + pre-signed URLs for viewing.
- Store stable keys in S3 (deterministic naming) so you can re-generate without orphaning assets.
- Log “runs” via `ops.llm_runs` + `ops.llm_usage` (tokens/costs) and timestamps; don’t rely on file existence.
- Keep prompt changes in `ops.prompt_revisions` (who/when/what changed).

## Open decisions (pick when ready)

- Background jobs:
  - Today generation is synchronous inside the API request.
  - Add a worker + job queue only if batches start timing out.
- “Latest only” assets vs keep all historical assets:
  - If latest only: overwrite the same S3 key (simple)
  - If history: include timestamp in the S3 key and keep multiple rows in `assets`
- Pricing updates:
  - Prefer env var pricing and manual updates.
  - A scheduled checker/alert can be added (don’t auto-scrape and write prices).
