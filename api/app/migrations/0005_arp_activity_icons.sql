-- 0005_arp_activity_icons.sql
-- Store deterministic icon specs + rendered SVGs for ARP activities.

CREATE SCHEMA IF NOT EXISTS "__ARP_SCHEMA__";

CREATE TABLE IF NOT EXISTS "__ARP_SCHEMA__".activity_icons (
  activity_id INTEGER PRIMARY KEY REFERENCES "__ARP_SCHEMA__".activities(activity_id) ON DELETE CASCADE,
  activity_slug TEXT NOT NULL UNIQUE,
  input_hash TEXT NOT NULL DEFAULT '',
  renderer_version TEXT NOT NULL DEFAULT '',
  spec_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  svg TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS arp_activity_icons_updated_at_idx
  ON "__ARP_SCHEMA__".activity_icons(updated_at DESC);

