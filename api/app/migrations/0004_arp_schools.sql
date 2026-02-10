-- 0004_arp_schools.sql
-- Schools research UI tables (imported from the Schools Research pipeline).

CREATE TABLE IF NOT EXISTS "__ARP_SCHEMA__".schools (
  school_key TEXT PRIMARY KEY,
  name TEXT NOT NULL DEFAULT '',
  homepage_url TEXT NOT NULL DEFAULT '',
  primary_domain TEXT NOT NULL DEFAULT '',
  last_crawled_at TIMESTAMPTZ,
  tier TEXT NOT NULL DEFAULT '',
  health_score INTEGER NOT NULL DEFAULT 0,
  logo_url TEXT NOT NULL DEFAULT '',
  emails JSONB NOT NULL DEFAULT '{}'::jsonb,
  social_links JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS arp_schools_tier_idx
  ON "__ARP_SCHEMA__".schools(tier);

CREATE INDEX IF NOT EXISTS arp_schools_health_score_idx
  ON "__ARP_SCHEMA__".schools(health_score);

CREATE TABLE IF NOT EXISTS "__ARP_SCHEMA__".school_evidence (
  school_key TEXT PRIMARY KEY REFERENCES "__ARP_SCHEMA__".schools(school_key) ON DELETE CASCADE,
  evidence_markdown TEXT NOT NULL DEFAULT '',
  evidence_generated_at TIMESTAMPTZ,
  evidence_sources JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE TABLE IF NOT EXISTS "__ARP_SCHEMA__".school_overviews (
  school_key TEXT PRIMARY KEY REFERENCES "__ARP_SCHEMA__".schools(school_key) ON DELETE CASCADE,
  overview_75w TEXT NOT NULL DEFAULT '',
  narrative TEXT NOT NULL DEFAULT '',
  model TEXT NOT NULL DEFAULT '',
  run_id TEXT NOT NULL DEFAULT '',
  tokens_in INTEGER NOT NULL DEFAULT 0,
  tokens_out INTEGER NOT NULL DEFAULT 0,
  tokens_total INTEGER NOT NULL DEFAULT 0,
  cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  extracted_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS "__ARP_SCHEMA__".school_trip_programs (
  school_key TEXT NOT NULL REFERENCES "__ARP_SCHEMA__".schools(school_key) ON DELETE CASCADE,
  program_name TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'llm', -- llm | manual | regex
  extracted_at TIMESTAMPTZ,
  PRIMARY KEY (school_key, program_name)
);

CREATE INDEX IF NOT EXISTS arp_school_trip_programs_school_key_idx
  ON "__ARP_SCHEMA__".school_trip_programs(school_key);

