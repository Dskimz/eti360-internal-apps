-- 0005_arp_schools_llm_json.sql
-- Persist raw per-school LLM JSON output for UI drill-down.

ALTER TABLE "__ARP_SCHEMA__".school_overviews
  ADD COLUMN IF NOT EXISTS llm_json JSONB NOT NULL DEFAULT '{}'::jsonb;

