#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from hashlib import sha256
from pathlib import Path


def stable_slug(*parts: str, max_len: int = 90) -> str:
    raw = " ".join(p.strip() for p in parts if p and p.strip())
    raw = re.sub(r"\s+", " ", raw).strip()
    base = re.sub(r"[^a-zA-Z0-9]+", "-", raw).strip("-").lower()
    if not base:
        base = "source"
    if len(base) <= max_len:
        return base
    digest = sha256(raw.encode("utf-8")).hexdigest()[:12]
    return f"{base[: max_len - 13]}-{digest}"


def sql_str(s: str) -> str:
    s = (s or "").replace("\r\n", "\n").replace("\r", "\n")
    return "'" + s.replace("'", "''") + "'"


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate ARP sources replacement migration from research.csv")
    ap.add_argument(
        "--research",
        default="api/app/static/arp_data/research.csv",
        help="Path to canonical research.csv (repo-relative)",
    )
    ap.add_argument(
        "--out",
        default="api/app/migrations/0006_arp_replace_sources_v2.sql",
        help="Output migration path (repo-relative)",
    )
    args = ap.parse_args()

    research_path = Path(args.research)
    out_path = Path(args.out)
    if not research_path.exists():
        raise SystemExit(f"Missing research CSV: {research_path}")

    rows: list[dict[str, str]] = []
    with research_path.open("r", encoding="utf-8", newline="") as f:
        dr = csv.DictReader(f)
        if not dr.fieldnames:
            raise SystemExit("research.csv has no headers")
        for r in dr:
            rows.append({k: (v or "").strip() for k, v in r.items()})

    values_lines: list[str] = []
    for r in rows:
        activity_name = (r.get("Activity") or r.get("Activity Name") or "").strip()
        if not activity_name:
            continue

        title = (r.get("Title") or "").strip()
        organization = (r.get("Organization / Publisher") or r.get("Organization") or r.get("Publisher") or "").strip()
        jurisdiction = (r.get("Country / Jurisdiction") or r.get("Jurisdiction") or r.get("Country") or "").strip()
        url = (r.get("URL") or r.get("Url") or r.get("Link") or "").strip()
        source_type = (r.get("Source type") or r.get("Source Type") or r.get("Type") or "").strip()
        activities_covered_raw = (r.get("Activities covered") or r.get("Activities Covered") or "").strip()
        brief_focus = (r.get("Brief focus (1–2 lines)") or r.get("Brief focus (1-2 lines)") or r.get("Brief focus") or "").strip()
        authority_class = (r.get("Authority class (A/B/C)") or r.get("Authority class") or r.get("Authority Class") or "").strip()
        publication_date = (r.get("Publication date (YYYY-MM-DD or YYYY)") or r.get("Publication date") or r.get("Publication Date") or "").strip()

        if not url:
            continue

        source_id = stable_slug(activity_name, organization, title, url, max_len=90)
        values_lines.append(
            "  ("
            + ", ".join(
                [
                    sql_str(source_id),
                    sql_str(activity_name),
                    sql_str(title),
                    sql_str(organization),
                    sql_str(jurisdiction),
                    sql_str(url),
                    sql_str(source_type),
                    sql_str(activities_covered_raw),
                    sql_str(brief_focus),
                    sql_str(authority_class),
                    sql_str(publication_date),
                ]
            )
            + ")"
        )

    if not values_lines:
        raise SystemExit("No valid research rows found to generate migration.")

    sql = "\n".join(
        [
            "-- 0006_arp_replace_sources_v2.sql",
            "-- Replace ARP sources from the canonical research.csv export (correct column mapping).",
            "-- One-off corrective migration: deletes sources+evidence for activities present in the CSV, then re-inserts.",
            "-- Activity IDs are mapped by activity_name to avoid ID mismatches across environments.",
            "",
            'CREATE SCHEMA IF NOT EXISTS "__ARP_SCHEMA__";',
            "",
            "WITH data(source_id, activity_name, title, organization, jurisdiction, url, source_type, activities_covered_raw, brief_focus, authority_class, publication_date) AS (",
            "  VALUES",
            ",\n".join(values_lines),
            "), dedup AS (",
            "  SELECT DISTINCT ON (source_id) * FROM data ORDER BY source_id",
            "), mapped AS (",
            '  SELECT d.source_id, a.activity_id, d.activity_name, d.title, d.organization, d.jurisdiction, d.url, d.source_type, d.activities_covered_raw, d.brief_focus, d.authority_class, d.publication_date',
            "  FROM dedup d",
            '  JOIN "__ARP_SCHEMA__".activities a ON lower(a.activity_name) = lower(d.activity_name)',
            "), affected AS (",
            "  SELECT DISTINCT activity_id FROM mapped",
            "), del_reports AS (",
            '  DELETE FROM "__ARP_SCHEMA__".reports',
            "  WHERE activity_id IN (SELECT activity_id FROM affected)",
            "  RETURNING activity_id",
            "), del_sources AS (",
            '  DELETE FROM "__ARP_SCHEMA__".sources',
            "  WHERE activity_id IN (SELECT activity_id FROM affected)",
            "  RETURNING source_id",
            "), upserted AS (",
            '  INSERT INTO "__ARP_SCHEMA__".sources (source_id, activity_id, activity_name, title, organization, jurisdiction, url, source_type, activities_covered_raw, brief_focus, authority_class, publication_date)',
            "  SELECT source_id, activity_id, activity_name, title, organization, jurisdiction, url, source_type, activities_covered_raw, brief_focus, authority_class, publication_date",
            "  FROM mapped",
            "  ON CONFLICT (source_id) DO UPDATE SET",
            "    activity_id=EXCLUDED.activity_id,",
            "    activity_name=EXCLUDED.activity_name,",
            "    title=EXCLUDED.title,",
            "    organization=EXCLUDED.organization,",
            "    jurisdiction=EXCLUDED.jurisdiction,",
            "    url=EXCLUDED.url,",
            "    source_type=EXCLUDED.source_type,",
            "    activities_covered_raw=EXCLUDED.activities_covered_raw,",
            "    brief_focus=EXCLUDED.brief_focus,",
            "    authority_class=EXCLUDED.authority_class,",
            "    publication_date=EXCLUDED.publication_date",
            "  RETURNING source_id",
            ")",
            "-- Ensure documents rows exist for all inserted/updated sources",
            'INSERT INTO "__ARP_SCHEMA__".documents (source_id)',
            "SELECT DISTINCT source_id FROM upserted",
            "ON CONFLICT (source_id) DO NOTHING;",
            "",
        ]
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(sql, encoding="utf-8")
    print(f"Wrote {out_path} ({len(values_lines)} sources)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
