#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg

from app.weather.s3 import get_s3_config, put_bytes


def safe_key(name: str) -> str:
    return (name or "").strip().lower().replace(" ", "_")


def parse_iso8601(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1]
        after_t = text.split("T", 1)[1] if "T" in text else text
        if "+" not in after_t and "-" not in after_t:
            text += "+00:00"
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def require_env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise SystemExit(f"Missing env var: {name}")
    return v


def connect() -> psycopg.Connection:
    return psycopg.connect(require_env("DATABASE_URL"))


_SAFE_IDENT_RE = re.compile(r"^[a-zA-Z0-9_]+$")


def directory_schema() -> str:
    raw = (os.environ.get("DIRECTORY_SCHEMA", "directory").strip() or "directory").strip()
    if not _SAFE_IDENT_RE.match(raw):
        raise SystemExit("Invalid DIRECTORY_SCHEMA (use a-z, A-Z, 0-9, _)")
    return raw


def _schema(sql: str) -> str:
    return sql.replace("__SCHEMA__", directory_schema())


DIRECTORY_SCHEMA_STATEMENTS: list[str] = [
    "CREATE EXTENSION IF NOT EXISTS pgcrypto;",
    _schema('CREATE SCHEMA IF NOT EXISTS "__SCHEMA__";'),
    _schema(
        """
        CREATE TABLE IF NOT EXISTS "__SCHEMA__".providers (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          provider_key TEXT NOT NULL UNIQUE,
          provider_name TEXT NOT NULL DEFAULT '',
          website_url TEXT NOT NULL DEFAULT '',
          status TEXT NOT NULL DEFAULT 'active',
          last_reviewed_at TIMESTAMPTZ,
          review_interval_days INTEGER NOT NULL DEFAULT 365,
          profile_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT providers_status_chk CHECK (status IN ('active','excluded'))
        );
        """
    ).strip(),
    _schema('CREATE INDEX IF NOT EXISTS providers_name_idx ON "__SCHEMA__".providers(provider_name);'),
    _schema('CREATE INDEX IF NOT EXISTS providers_status_idx ON "__SCHEMA__".providers(status);'),
    _schema(
        """
        CREATE TABLE IF NOT EXISTS "__SCHEMA__".provider_classifications (
          provider_id UUID PRIMARY KEY REFERENCES "__SCHEMA__".providers(id) ON DELETE CASCADE,
          market_orientation TEXT NOT NULL DEFAULT '',
          client_profile_indicators TEXT NOT NULL DEFAULT '',
          educational_market_orientation TEXT NOT NULL DEFAULT '',
          commercial_posture_signal TEXT NOT NULL DEFAULT '',
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    ).strip(),
    _schema('CREATE INDEX IF NOT EXISTS provider_classifications_market_idx ON "__SCHEMA__".provider_classifications(market_orientation);'),
    _schema(
        """
        CREATE TABLE IF NOT EXISTS "__SCHEMA__".provider_social_links (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          provider_id UUID NOT NULL REFERENCES "__SCHEMA__".providers(id) ON DELETE CASCADE,
          kind TEXT NOT NULL DEFAULT 'other',
          url TEXT NOT NULL DEFAULT '',
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT provider_social_kind_chk CHECK (kind IN ('facebook','linkedin','instagram','twitter','youtube','tiktok','other')),
          CONSTRAINT provider_social_provider_kind_uniq UNIQUE (provider_id, kind)
        );
        """
    ).strip(),
    _schema('CREATE INDEX IF NOT EXISTS provider_social_provider_idx ON "__SCHEMA__".provider_social_links(provider_id);'),
    _schema(
        """
        CREATE TABLE IF NOT EXISTS "__SCHEMA__".provider_analysis_runs (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          provider_id UUID NOT NULL REFERENCES "__SCHEMA__".providers(id) ON DELETE CASCADE,
          analytical_prompt_version TEXT NOT NULL DEFAULT '',
          raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          generated_at TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT provider_analysis_provider_version_uniq UNIQUE (provider_id, analytical_prompt_version)
        );
        """
    ).strip(),
    _schema('CREATE INDEX IF NOT EXISTS provider_analysis_provider_idx ON "__SCHEMA__".provider_analysis_runs(provider_id);'),
    _schema(
        """
        CREATE TABLE IF NOT EXISTS "__SCHEMA__".provider_evidence (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          provider_id UUID NOT NULL REFERENCES "__SCHEMA__".providers(id) ON DELETE CASCADE,
          kind TEXT NOT NULL DEFAULT 'markdown',
          s3_bucket TEXT NOT NULL DEFAULT '',
          s3_key TEXT NOT NULL DEFAULT '',
          content_type TEXT NOT NULL DEFAULT 'text/markdown',
          bytes BIGINT,
          uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT provider_evidence_provider_kind_uniq UNIQUE (provider_id, kind)
        );
        """
    ).strip(),
    _schema('CREATE INDEX IF NOT EXISTS provider_evidence_provider_idx ON "__SCHEMA__".provider_evidence(provider_id);'),
]


def evidence_s3_prefix() -> str:
    raw = os.environ.get("TRIP_PROVIDERS_EVIDENCE_S3_PREFIX", "trip_providers/evidence/").strip()
    if not raw:
        raw = "trip_providers/evidence/"
    if not raw.endswith("/"):
        raw += "/"
    if raw.startswith("/"):
        raw = raw.lstrip("/")
    return raw


def ensure_schema() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            for stmt in DIRECTORY_SCHEMA_STATEMENTS:
                cur.execute(stmt)
        conn.commit()


def load_aggregated(aggregated_json: Path) -> dict[str, dict[str, Any]]:
    payload = json.loads(aggregated_json.read_text(encoding="utf-8"))
    providers = payload.get("providers") or []
    out: dict[str, dict[str, Any]] = {}
    for p in providers:
        if not isinstance(p, dict):
            continue
        name_obj = p.get("provider_name")
        name = name_obj.get("value") if isinstance(name_obj, dict) else None
        if not name:
            continue
        out[safe_key(str(name))] = p
    return out


def extract_signals(analysis_payload: dict[str, Any]) -> dict[str, str]:
    analysis = analysis_payload.get("analysis") or {}
    if not isinstance(analysis, dict):
        return {}
    interpretive = analysis.get("interpretive_signals")
    if isinstance(interpretive, dict):
        return {
            "market_orientation": str(interpretive.get("market_orientation") or "").strip(),
            "client_profile_indicators": str(interpretive.get("client_profile_indicators") or "").strip(),
            "educational_market_orientation": str(interpretive.get("educational_market_orientation") or "").strip(),
            "commercial_posture_signal": str(interpretive.get("commercial_posture_signal") or "").strip(),
        }
    market = analysis.get("market_orientation")
    if isinstance(market, dict) and market.get("signal") is not None:
        return {"market_orientation": str(market.get("signal") or "").strip()}
    if isinstance(market, str):
        return {"market_orientation": market.strip()}
    return {}


def upsert_provider(
    *,
    cur: psycopg.Cursor,
    provider_key: str,
    provider_name: str,
    website_url: str,
    status: str,
    last_reviewed_at: datetime | None,
    profile_json: dict[str, Any],
) -> str:
    cur.execute(
        _schema(
            """
            INSERT INTO "__SCHEMA__".providers (provider_key, provider_name, website_url, status, last_reviewed_at, profile_json)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (provider_key) DO UPDATE SET
              provider_name = EXCLUDED.provider_name,
              website_url = EXCLUDED.website_url,
              status = EXCLUDED.status,
              last_reviewed_at = COALESCE(EXCLUDED.last_reviewed_at, "__SCHEMA__".providers.last_reviewed_at),
              profile_json = EXCLUDED.profile_json,
              updated_at = now()
            RETURNING id;
            """
        ).strip(),
        (provider_key, provider_name, website_url, status, last_reviewed_at, json.dumps(profile_json, ensure_ascii=False)),
    )
    return str(cur.fetchone()[0])


def upsert_classifications(*, cur: psycopg.Cursor, provider_id: str, signals: dict[str, str]) -> None:
    cur.execute(
        _schema(
            """
            INSERT INTO "__SCHEMA__".provider_classifications (
              provider_id, market_orientation, client_profile_indicators, educational_market_orientation, commercial_posture_signal
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (provider_id) DO UPDATE SET
              market_orientation = EXCLUDED.market_orientation,
              client_profile_indicators = EXCLUDED.client_profile_indicators,
              educational_market_orientation = EXCLUDED.educational_market_orientation,
              commercial_posture_signal = EXCLUDED.commercial_posture_signal,
              updated_at = now();
            """
        ).strip(),
        (
            provider_id,
            signals.get("market_orientation", ""),
            signals.get("client_profile_indicators", ""),
            signals.get("educational_market_orientation", ""),
            signals.get("commercial_posture_signal", ""),
        ),
    )


def upsert_analysis_run(
    *,
    cur: psycopg.Cursor,
    provider_id: str,
    analytical_prompt_version: str,
    raw_json: dict[str, Any],
    generated_at: datetime | None,
) -> None:
    cur.execute(
        _schema(
            """
            INSERT INTO "__SCHEMA__".provider_analysis_runs (provider_id, analytical_prompt_version, raw_json, generated_at)
            VALUES (%s, %s, %s::jsonb, %s)
            ON CONFLICT (provider_id, analytical_prompt_version) DO UPDATE SET
              raw_json = EXCLUDED.raw_json,
              generated_at = EXCLUDED.generated_at,
              created_at = now();
            """
        ).strip(),
        (provider_id, analytical_prompt_version, json.dumps(raw_json, ensure_ascii=False), generated_at),
    )


def upsert_social_links(*, cur: psycopg.Cursor, provider_id: str, social_links: dict[str, Any]) -> None:
    if not isinstance(social_links, dict):
        return
    allowed = {"facebook", "linkedin", "instagram", "twitter", "youtube", "tiktok"}
    for kind, url in social_links.items():
        k = str(kind or "").strip().lower()
        if not k:
            continue
        if k not in allowed:
            k = "other"
        u = str(url or "").strip()
        if not u:
            continue
        cur.execute(
            _schema(
                """
                INSERT INTO "__SCHEMA__".provider_social_links (provider_id, kind, url)
                VALUES (%s, %s, %s)
                ON CONFLICT (provider_id, kind) DO UPDATE SET url = EXCLUDED.url;
                """
            ).strip(),
            (provider_id, k, u),
        )


def upsert_evidence_markdown(
    *,
    cur: psycopg.Cursor,
    provider_id: str,
    provider_key: str,
    evidence_path: Path,
    dry_run: bool,
) -> None:
    if not evidence_path.exists():
        return
    body = evidence_path.read_bytes()
    if not body:
        return

    cfg = get_s3_config()
    key = f"{cfg.prefix}{evidence_s3_prefix()}{provider_key}/evidence.md"
    if not dry_run:
        put_bytes(region=cfg.region, bucket=cfg.bucket, key=key, body=body, content_type="text/markdown")

    cur.execute(
        _schema(
            """
            INSERT INTO "__SCHEMA__".provider_evidence (provider_id, kind, s3_bucket, s3_key, content_type, bytes)
            VALUES (%s, 'markdown', %s, %s, 'text/markdown', %s)
            ON CONFLICT (provider_id, kind) DO UPDATE SET
              s3_bucket = EXCLUDED.s3_bucket,
              s3_key = EXCLUDED.s3_key,
              content_type = EXCLUDED.content_type,
              bytes = EXCLUDED.bytes,
              uploaded_at = now();
            """
        ).strip(),
        (provider_id, cfg.bucket, key, len(body)),
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Import trip provider research data into Postgres + S3.")
    ap.add_argument("--aggregated-json", type=Path, required=True, help="Path to part_a_aggregated.json")
    ap.add_argument("--analysis-dir", type=Path, required=True, help="Directory of analytical_prompt_v1/*.json")
    ap.add_argument("--evidence-dir", type=Path, required=False, help="Directory of evidence_markdown/*.md")
    ap.add_argument("--social-links-dir", type=Path, required=False, help="Directory of social link jsons")
    ap.add_argument("--providers-csv", type=Path, required=False, help="Optional providers.csv (to set last_reviewed_at)")
    ap.add_argument("--dry-run", action="store_true", help="No S3 uploads; still writes DB rows.")
    args = ap.parse_args()

    ensure_schema()

    aggregated = load_aggregated(args.aggregated_json)

    reviewed_by_key: dict[str, datetime] = {}
    if args.providers_csv and args.providers_csv.exists():
        import csv

        with args.providers_csv.open(newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                name = (row.get("provider_name") or "").strip()
                completed = parse_iso8601(row.get("completed_at"))
                if name and completed:
                    reviewed_by_key[safe_key(name)] = completed

    total = 0
    edu = 0
    with connect() as conn:
        with conn.cursor() as cur:
            for path in sorted(args.analysis_dir.glob("*.json")):
                provider_key = path.stem
                analysis_payload = json.loads(path.read_text(encoding="utf-8"))

                signals = extract_signals(analysis_payload)
                market = signals.get("market_orientation", "")
                if market == "Education-focused":
                    edu += 1

                provider_record = aggregated.get(provider_key, {})
                provider_name_obj = provider_record.get("provider_name") if isinstance(provider_record, dict) else {}
                provider_name = (
                    provider_name_obj.get("value") if isinstance(provider_name_obj, dict) else None
                ) or provider_key
                website_url = str(provider_record.get("website_url") or "").strip() if isinstance(provider_record, dict) else ""

                last_reviewed_at = reviewed_by_key.get(provider_key)
                if not last_reviewed_at:
                    last_reviewed_at = parse_iso8601(analysis_payload.get("generated_at"))
                if not last_reviewed_at:
                    last_reviewed_at = datetime.now(timezone.utc)

                provider_id = upsert_provider(
                    cur=cur,
                    provider_key=provider_key,
                    provider_name=str(provider_name),
                    website_url=website_url,
                    status="active",
                    last_reviewed_at=last_reviewed_at,
                    profile_json=provider_record if isinstance(provider_record, dict) else {},
                )

                upsert_classifications(cur=cur, provider_id=provider_id, signals=signals)

                version = (
                    str(analysis_payload.get("analytical_prompt_version") or "").strip()
                    or str((analysis_payload.get("metadata") or {}).get("version") or "").strip()
                    or "v1"
                )
                generated_at = parse_iso8601(analysis_payload.get("generated_at"))
                upsert_analysis_run(
                    cur=cur,
                    provider_id=provider_id,
                    analytical_prompt_version=version,
                    raw_json=analysis_payload,
                    generated_at=generated_at,
                )

                if args.social_links_dir:
                    social_path = args.social_links_dir / f"{provider_key}.json"
                    if social_path.exists():
                        social_links = json.loads(social_path.read_text(encoding="utf-8"))
                        upsert_social_links(cur=cur, provider_id=provider_id, social_links=social_links)

                if args.evidence_dir:
                    evidence_path = args.evidence_dir / f"{provider_key}.md"
                    upsert_evidence_markdown(
                        cur=cur,
                        provider_id=provider_id,
                        provider_key=provider_key,
                        evidence_path=evidence_path,
                        dry_run=bool(args.dry_run),
                    )

                total += 1

        conn.commit()

    print(f"Imported {total} providers. Education-focused: {edu}.")


if __name__ == "__main__":
    main()

