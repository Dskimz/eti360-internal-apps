from __future__ import annotations

import os
from typing import Any

import psycopg
from fastapi import FastAPI, Header, HTTPException

app = FastAPI(title="ETI360 Internal API", docs_url=None, redoc_url=None)

WEATHER_SCHEMA = "weather"


def _require_api_key(x_api_key: str | None) -> None:
    expected = os.environ.get("ETI360_API_KEY", "").strip()
    if not expected:
        raise HTTPException(status_code=500, detail="ETI360_API_KEY is not set")
    if not x_api_key or x_api_key.strip() != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")


def _get_database_url() -> str:
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        raise HTTPException(status_code=500, detail="DATABASE_URL is not set")
    return database_url


@app.get("/health")
def health() -> dict[str, bool]:
    return {"ok": True}


@app.get("/health/db")
def health_db() -> dict[str, bool]:
    database_url = _get_database_url()

    try:
        with psycopg.connect(database_url, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("select 1")
                cur.fetchone()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB connection failed: {e}") from e

    return {"ok": True}


def _q(name: str) -> str:
    # Safe for our internal fixed identifiers; avoids mixing schemas in f-strings.
    if not name.replace("_", "").isalnum():
        raise ValueError("Invalid identifier")
    return f'"{name}"'


_SCHEMA_STATEMENTS: list[str] = [
    # Extensions
    "CREATE EXTENSION IF NOT EXISTS pgcrypto;",
    # Namespacing: keep weather tables out of public schema to avoid collisions with other apps.
    f"CREATE SCHEMA IF NOT EXISTS {_q(WEATHER_SCHEMA)};",
    # Core tables (atomic schema; no JSON columns, no array columns)
    f"""
    CREATE TABLE IF NOT EXISTS {_q(WEATHER_SCHEMA)}.locations (
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
    )
    """.strip()
    + ";",
    f"""
    CREATE TABLE IF NOT EXISTS {_q(WEATHER_SCHEMA)}.weather_sources (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      label TEXT NOT NULL DEFAULT '',
      url TEXT NOT NULL DEFAULT '',
      accessed_utc TIMESTAMPTZ,
      notes TEXT NOT NULL DEFAULT '',
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """.strip()
    + ";",
    f"""
    CREATE TABLE IF NOT EXISTS {_q(WEATHER_SCHEMA)}.weather_datasets (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      location_id UUID NOT NULL REFERENCES {_q(WEATHER_SCHEMA)}.locations(id) ON DELETE CASCADE,
      source_id UUID REFERENCES {_q(WEATHER_SCHEMA)}.weather_sources(id) ON DELETE SET NULL,
      title TEXT NOT NULL DEFAULT '',
      subtitle TEXT NOT NULL DEFAULT '',
      weather_overview TEXT NOT NULL DEFAULT '',
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """.strip()
    + ";",
    f"""
    CREATE TABLE IF NOT EXISTS {_q(WEATHER_SCHEMA)}.weather_monthly_normals (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      dataset_id UUID NOT NULL REFERENCES {_q(WEATHER_SCHEMA)}.weather_datasets(id) ON DELETE CASCADE,
      month SMALLINT NOT NULL,
      high_c NUMERIC(6,2) NOT NULL,
      low_c  NUMERIC(6,2) NOT NULL,
      precip_cm NUMERIC(8,3) NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      CONSTRAINT wmn_month_range_chk CHECK (month >= 1 AND month <= 12),
      CONSTRAINT wmn_high_ge_low_chk CHECK (high_c >= low_c),
      CONSTRAINT wmn_dataset_month_uniq UNIQUE (dataset_id, month)
    )
    """.strip()
    + ";",
    f"""
    CREATE TABLE IF NOT EXISTS {_q(WEATHER_SCHEMA)}.assets (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      location_id UUID NOT NULL REFERENCES {_q(WEATHER_SCHEMA)}.locations(id) ON DELETE CASCADE,
      kind TEXT NOT NULL,
      year INTEGER,
      s3_bucket TEXT NOT NULL DEFAULT '',
      s3_key TEXT NOT NULL DEFAULT '',
      content_type TEXT NOT NULL DEFAULT 'image/png',
      bytes BIGINT,
      generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      CONSTRAINT assets_kind_chk CHECK (kind IN ('weather','daylight','map'))
    )
    """.strip()
    + ";",
    f"CREATE INDEX IF NOT EXISTS assets_location_kind_idx ON {_q(WEATHER_SCHEMA)}.assets(location_id, kind);",
    f"CREATE INDEX IF NOT EXISTS assets_generated_at_idx ON {_q(WEATHER_SCHEMA)}.assets(generated_at DESC);",
]


@app.post("/admin/schema/init")
def admin_schema_init(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> dict[str, Any]:
    _require_api_key(x_api_key)
    database_url = _get_database_url()

    applied: list[str] = []
    try:
        with psycopg.connect(database_url, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                for stmt in _SCHEMA_STATEMENTS:
                    cur.execute(stmt)
                    applied.append(stmt.splitlines()[0][:120])
            conn.commit()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Schema init failed: {e}") from e

    return {"ok": True, "schema": WEATHER_SCHEMA, "applied": applied}
