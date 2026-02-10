-- 0002_arp_schema.sql
-- ARP tables: activities, sources, raw docs, chunks, and reports.

CREATE SCHEMA IF NOT EXISTS "__ARP_SCHEMA__";

CREATE TABLE IF NOT EXISTS "__ARP_SCHEMA__".activities (
  activity_id INTEGER PRIMARY KEY,
  activity_slug TEXT NOT NULL UNIQUE,
  activity_name TEXT NOT NULL,
  activity_category TEXT NOT NULL DEFAULT '',
  scope_notes TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS "__ARP_SCHEMA__".sources (
  source_id TEXT PRIMARY KEY,
  activity_id INTEGER NOT NULL REFERENCES "__ARP_SCHEMA__".activities(activity_id) ON DELETE CASCADE,
  activity_name TEXT NOT NULL DEFAULT '',
  title TEXT NOT NULL DEFAULT '',
  organization TEXT NOT NULL DEFAULT '',
  jurisdiction TEXT NOT NULL DEFAULT '',
  url TEXT NOT NULL,
  source_type TEXT NOT NULL DEFAULT '',
  activities_covered_raw TEXT NOT NULL DEFAULT '',
  brief_focus TEXT NOT NULL DEFAULT '',
  authority_class TEXT NOT NULL DEFAULT '',
  publication_date TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS arp_sources_activity_id_idx
  ON "__ARP_SCHEMA__".sources(activity_id);

CREATE TABLE IF NOT EXISTS "__ARP_SCHEMA__".documents (
  source_id TEXT PRIMARY KEY REFERENCES "__ARP_SCHEMA__".sources(source_id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'missing', -- missing | fetched | error
  content_type TEXT NOT NULL DEFAULT '',
  fetched_at TIMESTAMPTZ,
  sha256 TEXT NOT NULL DEFAULT '',
  bytes_size BIGINT NOT NULL DEFAULT 0,
  s3_bucket TEXT NOT NULL DEFAULT '',
  s3_key TEXT NOT NULL DEFAULT '',
  error TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS "__ARP_SCHEMA__".chunks (
  chunk_id TEXT PRIMARY KEY,
  activity_id INTEGER NOT NULL REFERENCES "__ARP_SCHEMA__".activities(activity_id) ON DELETE CASCADE,
  source_id TEXT NOT NULL REFERENCES "__ARP_SCHEMA__".sources(source_id) ON DELETE CASCADE,
  heading TEXT NOT NULL DEFAULT '',
  text TEXT NOT NULL,
  jurisdiction TEXT NOT NULL DEFAULT '',
  authority_class TEXT NOT NULL DEFAULT '',
  publication_date TEXT NOT NULL DEFAULT '',
  loc TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS arp_chunks_activity_id_idx
  ON "__ARP_SCHEMA__".chunks(activity_id);

CREATE INDEX IF NOT EXISTS arp_chunks_source_id_idx
  ON "__ARP_SCHEMA__".chunks(source_id);

CREATE TABLE IF NOT EXISTS "__ARP_SCHEMA__".reports (
  activity_id INTEGER PRIMARY KEY REFERENCES "__ARP_SCHEMA__".activities(activity_id) ON DELETE CASCADE,
  activity_slug TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL DEFAULT 'draft', -- draft | reviewed
  report_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  report_md TEXT NOT NULL DEFAULT '',
  model TEXT NOT NULL DEFAULT '',
  error TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

