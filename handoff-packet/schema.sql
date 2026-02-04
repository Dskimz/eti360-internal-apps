-- ETI360 Internal Apps (Weather + Sunlight) — Baseline Postgres Schema
-- Run once against the Render Postgres instance (eti-db).
--
-- Notes:
-- - The running service can also create some tables automatically ("create-if-missing").
-- - This file is a baseline snapshot for onboarding and disaster recovery.
-- - Schemas:
--   - weather.* : workflow data + assets
--   - ops.*     : shared cross-cutting tables (LLM usage, auth, prompts)

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------- weather schema ----------

CREATE SCHEMA IF NOT EXISTS weather;

CREATE TABLE IF NOT EXISTS weather.locations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  location_slug TEXT NOT NULL UNIQUE,
  place_id TEXT NOT NULL UNIQUE,
  city TEXT NOT NULL DEFAULT '',
  country TEXT NOT NULL DEFAULT '',
  lat DOUBLE PRECISION NOT NULL,
  lng DOUBLE PRECISION NOT NULL,
  timezone_id TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS weather.weather_sources (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  label TEXT NOT NULL DEFAULT '',
  url TEXT NOT NULL DEFAULT '',
  accessed_utc TIMESTAMPTZ,
  notes TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS weather.weather_datasets (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  location_id UUID NOT NULL REFERENCES weather.locations(id) ON DELETE CASCADE,
  source_id UUID REFERENCES weather.weather_sources(id) ON DELETE SET NULL,
  title TEXT NOT NULL DEFAULT '',
  subtitle TEXT NOT NULL DEFAULT '',
  weather_overview TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS weather.weather_monthly_normals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  dataset_id UUID NOT NULL REFERENCES weather.weather_datasets(id) ON DELETE CASCADE,
  month SMALLINT NOT NULL,
  high_c NUMERIC(6,2) NOT NULL,
  low_c  NUMERIC(6,2) NOT NULL,
  precip_cm NUMERIC(8,3) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT wmn_month_range_chk CHECK (month >= 1 AND month <= 12),
  CONSTRAINT wmn_high_ge_low_chk CHECK (high_c >= low_c),
  CONSTRAINT wmn_dataset_month_uniq UNIQUE (dataset_id, month)
);

CREATE TABLE IF NOT EXISTS weather.assets (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  location_id UUID NOT NULL REFERENCES weather.locations(id) ON DELETE CASCADE,
  kind TEXT NOT NULL,
  year INTEGER,
  s3_bucket TEXT NOT NULL DEFAULT '',
  s3_key TEXT NOT NULL DEFAULT '',
  content_type TEXT NOT NULL DEFAULT 'image/png',
  bytes BIGINT,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT assets_kind_chk CHECK (kind IN ('weather','daylight','map'))
);

CREATE INDEX IF NOT EXISTS assets_location_kind_idx ON weather.assets(location_id, kind);
CREATE INDEX IF NOT EXISTS assets_generated_at_idx ON weather.assets(generated_at DESC);

-- ---------- ops schema ----------

CREATE SCHEMA IF NOT EXISTS ops;

-- Shared LLM usage tracking across apps.
CREATE TABLE IF NOT EXISTS ops.llm_runs (
  id UUID PRIMARY KEY,
  workflow TEXT NOT NULL DEFAULT '',
  kind TEXT NOT NULL DEFAULT '',
  locations_count INTEGER NOT NULL DEFAULT 0,
  ok_count INTEGER NOT NULL DEFAULT 0,
  fail_count INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.llm_usage (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID NOT NULL REFERENCES ops.llm_runs(id) ON DELETE CASCADE,
  provider TEXT NOT NULL,
  model TEXT NOT NULL DEFAULT '',
  prompt_tokens INTEGER NOT NULL DEFAULT 0,
  completion_tokens INTEGER NOT NULL DEFAULT 0,
  total_tokens INTEGER NOT NULL DEFAULT 0,
  cost_usd NUMERIC(12,6) NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS llm_usage_run_id_idx ON ops.llm_usage(run_id);
CREATE INDEX IF NOT EXISTS llm_usage_created_at_idx ON ops.llm_usage(created_at DESC);

-- Auth (cookie sessions).
CREATE TABLE IF NOT EXISTS ops.users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  username TEXT NOT NULL UNIQUE,
  email TEXT NOT NULL DEFAULT '',
  display_name TEXT NOT NULL DEFAULT '',
  role TEXT NOT NULL DEFAULT 'viewer',
  password_hash TEXT NOT NULL,
  is_disabled BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT users_role_chk CHECK (role IN ('viewer','account_manager','editor','admin'))
);

CREATE UNIQUE INDEX IF NOT EXISTS users_email_uniq_idx ON ops.users (lower(email)) WHERE email <> '';

CREATE TABLE IF NOT EXISTS ops.sessions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES ops.users(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ NOT NULL,
  user_agent TEXT NOT NULL DEFAULT '',
  ip TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS sessions_user_id_idx ON ops.sessions(user_id);
CREATE INDEX IF NOT EXISTS sessions_expires_at_idx ON ops.sessions(expires_at DESC);

-- Prompts + audited revisions.
CREATE TABLE IF NOT EXISTS ops.prompts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  prompt_key TEXT NOT NULL UNIQUE,
  app_key TEXT NOT NULL DEFAULT '',
  workflow TEXT NOT NULL DEFAULT '',
  name TEXT NOT NULL DEFAULT '',
  natural_name TEXT NOT NULL DEFAULT '',
  description TEXT NOT NULL DEFAULT '',
  provider TEXT NOT NULL DEFAULT '',
  model TEXT NOT NULL DEFAULT '',
  prompt_text TEXT NOT NULL DEFAULT '',
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS prompts_updated_at_idx ON ops.prompts(updated_at DESC);
CREATE INDEX IF NOT EXISTS prompts_app_workflow_idx ON ops.prompts(app_key, workflow, prompt_key);

CREATE TABLE IF NOT EXISTS ops.prompt_revisions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  prompt_id UUID NOT NULL REFERENCES ops.prompts(id) ON DELETE CASCADE,
  prompt_key TEXT NOT NULL DEFAULT '',
  edited_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  edited_by_user_id UUID,
  edited_by_username TEXT NOT NULL DEFAULT '',
  edited_by_role TEXT NOT NULL DEFAULT '',
  change_note TEXT NOT NULL DEFAULT '',
  before_text TEXT NOT NULL DEFAULT '',
  after_text TEXT NOT NULL DEFAULT '',
  before_app_key TEXT NOT NULL DEFAULT '',
  after_app_key TEXT NOT NULL DEFAULT '',
  before_workflow TEXT NOT NULL DEFAULT '',
  after_workflow TEXT NOT NULL DEFAULT '',
  before_provider TEXT NOT NULL DEFAULT '',
  after_provider TEXT NOT NULL DEFAULT '',
  before_model TEXT NOT NULL DEFAULT '',
  after_model TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS prompt_revisions_key_idx ON ops.prompt_revisions(prompt_key, edited_at DESC);
CREATE INDEX IF NOT EXISTS prompt_revisions_edited_at_idx ON ops.prompt_revisions(edited_at DESC);

COMMIT;
