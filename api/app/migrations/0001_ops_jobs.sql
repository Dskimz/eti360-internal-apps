-- 0001_ops_jobs.sql
-- Creates a minimal migrations table + jobs table for background workflows.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS "__OPS_SCHEMA__";

CREATE TABLE IF NOT EXISTS "__OPS_SCHEMA__".schema_migrations (
  version TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS "__OPS_SCHEMA__".jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  kind TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued', -- queued | running | ok | error | cancelled
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  result JSONB NOT NULL DEFAULT '{}'::jsonb,
  error TEXT NOT NULL DEFAULT '',
  log TEXT NOT NULL DEFAULT '',
  created_by TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  heartbeat_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS jobs_status_created_at_idx
  ON "__OPS_SCHEMA__".jobs(status, created_at DESC);

