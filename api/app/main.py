from __future__ import annotations

import json
import os
import re
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse
from urllib.request import urlopen

import psycopg
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from app.weather.perplexity import fetch_monthly_weather_normals
from app.weather.daylight_chart import MonthlyDaylight, render_daylight_chart
from app.weather.llm_usage import estimate_cost_usd
from app.weather.s3 import get_s3_config, presign_get, put_png
from app.weather.weather_chart import MONTHS, MonthlyWeather, render_weather_chart

app = FastAPI(title="ETI360 Internal API", docs_url="/docs", redoc_url=None)

WEATHER_SCHEMA = "weather"


def _auth_disabled() -> bool:
    v = os.environ.get("AUTH_DISABLED", "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _require_api_key(x_api_key: str | None) -> None:
    if _auth_disabled():
        return
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


def _connect() -> psycopg.Connection:
    return psycopg.connect(_get_database_url())


def _schema(sql: str) -> str:
    return sql.replace("__SCHEMA__", WEATHER_SCHEMA)


def _slugify(s: str) -> str:
    s = (s or "").strip().lower()
    out: list[str] = []
    last_dash = False
    for ch in s:
        if ch.isalnum():
            out.append(ch)
            last_dash = False
        else:
            if not last_dash and out:
                out.append("-")
                last_dash = True
    return ("".join(out).strip("-") or "location")[:64]


def _extract_url(s: str) -> str:
    s = (s or "").strip()
    m = re.search(r"https?://[^\s\)\]\}\>\"']+", s)
    return m.group(0) if m else s


def _require_google_key() -> str:
    key = os.environ.get("GOOGLE_MAPS_API_KEY", "").strip()
    if not key:
        raise HTTPException(status_code=500, detail="GOOGLE_MAPS_API_KEY is not set")
    return key


def _fetch_json(url: str) -> dict[str, Any]:
    with urlopen(url, timeout=15) as resp:  # nosec - internal tool
        raw = resp.read().decode("utf-8", errors="replace")
    obj = json.loads(raw)
    if not isinstance(obj, dict):
        raise ValueError("Expected JSON object")
    return obj


def _extract_country(formatted_address: str) -> str:
    formatted_address = (formatted_address or "").strip()
    if not formatted_address:
        return ""
    parts = [p.strip() for p in formatted_address.split(",") if p.strip()]
    return parts[-1] if parts else ""


def _extract_model_float_list(obj: Any, key: str) -> list[float]:
    v = obj.get(key) if isinstance(obj, dict) else None
    if not isinstance(v, list):
        raise HTTPException(status_code=500, detail=f"Model payload missing {key} list")
    out: list[float] = []
    for i, x in enumerate(v):
        if not isinstance(x, (int, float)):
            raise HTTPException(status_code=500, detail=f"Model payload {key}[{i}] is not a number")
        out.append(float(x))
    return out


def _generate_weather_png_for_slug(*, location_slug: str, year: int) -> dict[str, Any]:
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_schema('SELECT id FROM "__SCHEMA__".locations WHERE location_slug=%s;'), (location_slug,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Unknown location_slug")
            (location_id,) = row

            cur.execute(
                _schema(
                    'SELECT d.id, d.title, d.subtitle, s.label, s.url FROM "__SCHEMA__".weather_datasets d LEFT JOIN "__SCHEMA__".weather_sources s ON s.id = d.source_id WHERE d.location_id=%s ORDER BY d.updated_at DESC LIMIT 1;'
                ),
                (location_id,),
            )
            ds = cur.fetchone()
            if not ds:
                raise HTTPException(status_code=404, detail="No dataset for location")
            dataset_id, title, subtitle, src_label, src_url = ds

            cur.execute(
                _schema(
                    'SELECT month, high_c, low_c, precip_cm FROM "__SCHEMA__".weather_monthly_normals WHERE dataset_id=%s ORDER BY month ASC;'
                ),
                (dataset_id,),
            )
            rows = cur.fetchall()
            if len(rows) != 12:
                raise HTTPException(status_code=400, detail=f"Expected 12 monthly rows; got {len(rows)}")

    monthly = [
        MonthlyWeather(month=MONTHS[int(m) - 1], high_c=float(h), low_c=float(l), precip_cm=float(p))
        for (m, h, l, p) in rows
    ]

    tmpdir = Path(tempfile.gettempdir())
    out_path = tmpdir / f"eti360-weather-{location_slug}-{year}.png"
    render_weather_chart(
        project_root=Path("."),
        monthly=monthly,
        title=str(title),
        subtitle=str(subtitle),
        source_left=_source_left(str(src_label or "Source"), str(src_url or "")),
        output_path=out_path,
    )
    png_bytes = out_path.read_bytes()

    cfg = get_s3_config()
    key = f"{cfg.prefix}{location_slug}/weather/{year}.png"
    put_png(region=cfg.region, bucket=cfg.bucket, key=key, body=png_bytes)

    generated_at = datetime.now(timezone.utc)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                _schema(
                    'INSERT INTO "__SCHEMA__".assets (location_id, kind, year, s3_bucket, s3_key, bytes, content_type, generated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;'
                ),
                (location_id, "weather", year, cfg.bucket, key, len(png_bytes), "image/png", generated_at),
            )
            (asset_id,) = cur.fetchone()  # type: ignore[misc]
        conn.commit()

    view_url = presign_get(region=cfg.region, bucket=cfg.bucket, key=key, expires_in=3600)
    return {"ok": True, "asset_id": str(asset_id), "s3_bucket": cfg.bucket, "s3_key": key, "view_url": view_url}


def _require_timezone_id(*, lat: float, lng: float) -> str:
    key = _require_google_key()
    ts = int(datetime.now(timezone.utc).timestamp())
    url = "https://maps.googleapis.com/maps/api/timezone/json?" + urlencode(
        {"location": f"{lat},{lng}", "timestamp": str(ts), "key": key}
    )
    data = _fetch_json(url)
    status = str(data.get("status") or "")
    if status != "OK":
        raise HTTPException(status_code=400, detail=f"Google Time Zone status: {status}")
    tzid = str(data.get("timeZoneId") or "").strip()
    if not tzid:
        raise HTTPException(status_code=500, detail="Google Time Zone did not return timeZoneId")
    return tzid


def _compute_monthly_daylight_hours(*, lat: float, lng: float, timezone_id: str, year: int) -> list[MonthlyDaylight]:
    from datetime import date
    from zoneinfo import ZoneInfo

    from astral import LocationInfo
    from astral.sun import sun

    tz = ZoneInfo(timezone_id)
    loc = LocationInfo(name="location", region="", timezone=timezone_id, latitude=lat, longitude=lng)
    monthly: list[MonthlyDaylight] = []
    for month_idx, month_name in enumerate(MONTHS, start=1):
        d = date(year, month_idx, 15)
        s = sun(loc.observer, date=d, tzinfo=tz)
        sunrise = s["sunrise"]
        sunset = s["sunset"]
        hours = max(0.0, float((sunset - sunrise).total_seconds() / 3600.0))
        monthly.append(MonthlyDaylight(month=month_name, daylight_hours=hours))
    return monthly


def _generate_daylight_png_for_slug(*, location_slug: str, year: int) -> dict[str, Any]:
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                _schema('SELECT id, lat, lng, timezone_id, city, country FROM "__SCHEMA__".locations WHERE location_slug=%s;'),
                (location_slug,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Unknown location_slug")
            location_id, lat, lng, timezone_id, city, country = row
            lat_f = float(lat)
            lng_f = float(lng)
            tzid = str(timezone_id or "").strip()
            if not tzid:
                tzid = _require_timezone_id(lat=lat_f, lng=lng_f)
                cur.execute(
                    _schema('UPDATE "__SCHEMA__".locations SET timezone_id=%s, updated_at=now() WHERE id=%s;'),
                    (tzid, location_id),
                )
                conn.commit()

    monthly = _compute_monthly_daylight_hours(lat=lat_f, lng=lng_f, timezone_id=tzid, year=year)

    title_city = str(city or "").strip() or location_slug
    title_country = str(country or "").strip()
    title = f"{title_city}{', ' + title_country if title_country else ''} daylight varies by season"
    subtitle = f"Estimated daylight hours by month (year {year})"
    source_left = "Source: Astral (computed from lat/lng + timezone)"

    tmpdir = Path(tempfile.gettempdir())
    out_path = tmpdir / f"eti360-daylight-{location_slug}-{year}.png"
    render_daylight_chart(monthly=monthly, title=title, subtitle=subtitle, source_left=source_left, output_path=out_path)
    png_bytes = out_path.read_bytes()

    cfg = get_s3_config()
    key = f"{cfg.prefix}{location_slug}/daylight/{year}.png"
    put_png(region=cfg.region, bucket=cfg.bucket, key=key, body=png_bytes)

    generated_at = datetime.now(timezone.utc)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                _schema(
                    'INSERT INTO "__SCHEMA__".assets (location_id, kind, year, s3_bucket, s3_key, bytes, content_type, generated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;'
                ),
                (location_id, "daylight", year, cfg.bucket, key, len(png_bytes), "image/png", generated_at),
            )
            (asset_id,) = cur.fetchone()  # type: ignore[misc]
        conn.commit()

    view_url = presign_get(region=cfg.region, bucket=cfg.bucket, key=key, expires_in=3600)
    return {"ok": True, "asset_id": str(asset_id), "s3_bucket": cfg.bucket, "s3_key": key, "view_url": view_url}


def _create_run_id() -> str:
    return str(uuid.uuid4())


def _record_llm_usage(
    *,
    run_id: str,
    kind: str,
    provider: str,
    model: str,
    prompt_tokens: int,
    completion_tokens: int,
    total_tokens: int,
) -> dict[str, Any]:
    run_uuid = uuid.UUID(str(run_id))
    cost_usd = float(estimate_cost_usd(provider=provider, prompt_tokens=prompt_tokens, completion_tokens=completion_tokens))

    with _connect() as conn:
        with conn.cursor() as cur:
            # Ensure tracker tables exist even if /admin/schema/init hasn't been run since adding them.
            cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
            cur.execute(_schema('CREATE SCHEMA IF NOT EXISTS "__SCHEMA__";'))
            cur.execute(
                _schema(
                    """
                    CREATE TABLE IF NOT EXISTS "__SCHEMA__".llm_runs (
                      id UUID PRIMARY KEY,
                      kind TEXT NOT NULL DEFAULT '',
                      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                    """
                ).strip()
            )
            cur.execute(
                _schema(
                    """
                    CREATE TABLE IF NOT EXISTS "__SCHEMA__".llm_usage (
                      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                      run_id UUID NOT NULL REFERENCES "__SCHEMA__".llm_runs(id) ON DELETE CASCADE,
                      provider TEXT NOT NULL,
                      model TEXT NOT NULL DEFAULT '',
                      prompt_tokens INTEGER NOT NULL DEFAULT 0,
                      completion_tokens INTEGER NOT NULL DEFAULT 0,
                      total_tokens INTEGER NOT NULL DEFAULT 0,
                      cost_usd NUMERIC(12,6) NOT NULL DEFAULT 0,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                    """
                ).strip()
            )
            cur.execute(_schema('CREATE INDEX IF NOT EXISTS llm_usage_run_id_idx ON "__SCHEMA__".llm_usage(run_id);'))
            cur.execute(_schema('CREATE INDEX IF NOT EXISTS llm_usage_created_at_idx ON "__SCHEMA__".llm_usage(created_at DESC);'))

            cur.execute(
                _schema('INSERT INTO "__SCHEMA__".llm_runs (id, kind) VALUES (%s,%s) ON CONFLICT (id) DO NOTHING;'),
                (run_uuid, kind),
            )
            cur.execute(
                _schema(
                    """
                    INSERT INTO "__SCHEMA__".llm_usage (run_id, provider, model, prompt_tokens, completion_tokens, total_tokens, cost_usd)
                    VALUES (%s,%s,%s,%s,%s,%s,%s);
                    """
                ),
                (run_uuid, provider, model, int(prompt_tokens), int(completion_tokens), int(total_tokens), cost_usd),
            )
        conn.commit()

    return {
        "provider": provider,
        "model": model,
        "prompt_tokens": int(prompt_tokens),
        "completion_tokens": int(completion_tokens),
        "total_tokens": int(total_tokens),
        "cost_usd": cost_usd,
    }


@app.get("/weather/usage")
def weather_usage(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_api_key(x_api_key)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
            cur.execute(_schema('CREATE SCHEMA IF NOT EXISTS "__SCHEMA__";'))
            cur.execute(
                _schema(
                    """
                    CREATE TABLE IF NOT EXISTS "__SCHEMA__".llm_runs (
                      id UUID PRIMARY KEY,
                      kind TEXT NOT NULL DEFAULT '',
                      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                    """
                ).strip()
            )
            cur.execute(
                _schema(
                    """
                    CREATE TABLE IF NOT EXISTS "__SCHEMA__".llm_usage (
                      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                      run_id UUID NOT NULL REFERENCES "__SCHEMA__".llm_runs(id) ON DELETE CASCADE,
                      provider TEXT NOT NULL,
                      model TEXT NOT NULL DEFAULT '',
                      prompt_tokens INTEGER NOT NULL DEFAULT 0,
                      completion_tokens INTEGER NOT NULL DEFAULT 0,
                      total_tokens INTEGER NOT NULL DEFAULT 0,
                      cost_usd NUMERIC(12,6) NOT NULL DEFAULT 0,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                    """
                ).strip()
            )

            cur.execute(
                _schema(
                    """
                    SELECT provider, model,
                           COALESCE(SUM(prompt_tokens),0) AS prompt_tokens,
                           COALESCE(SUM(completion_tokens),0) AS completion_tokens,
                           COALESCE(SUM(total_tokens),0) AS total_tokens,
                           COALESCE(SUM(cost_usd),0) AS cost_usd
                    FROM "__SCHEMA__".llm_usage
                    GROUP BY provider, model
                    ORDER BY provider, model;
                    """
                )
            )
            rows = cur.fetchall()

            cur.execute(_schema('SELECT id, kind, created_at FROM "__SCHEMA__".llm_runs ORDER BY created_at DESC LIMIT 1;'))
            last_run = cur.fetchone()
            last = None
            if last_run:
                run_id, kind, created_at = last_run
                cur.execute(
                    _schema(
                        """
                        SELECT provider, model, prompt_tokens, completion_tokens, total_tokens, cost_usd
                        FROM "__SCHEMA__".llm_usage
                        WHERE run_id=%s
                        ORDER BY provider, model;
                        """
                    ),
                    (run_id,),
                )
                last_usage = cur.fetchall()
                last = {
                    "run_id": str(run_id),
                    "kind": str(kind or ""),
                    "created_at": created_at.isoformat() if created_at else None,
                    "usage": [
                        {
                            "provider": str(p),
                            "model": str(m),
                            "prompt_tokens": int(pt),
                            "completion_tokens": int(ct),
                            "total_tokens": int(tt),
                            "cost_usd": float(c),
                        }
                        for (p, m, pt, ct, tt, c) in last_usage
                    ],
                }

    cumulative = [
        {
            "provider": str(p),
            "model": str(m),
            "prompt_tokens": int(pt),
            "completion_tokens": int(ct),
            "total_tokens": int(tt),
            "cost_usd": float(c),
        }
        for (p, m, pt, ct, tt, c) in rows
    ]
    total_cost = float(sum(r["cost_usd"] for r in cumulative))
    return {"ok": True, "last_run": last, "cumulative": cumulative, "cumulative_total_cost_usd": total_cost}


@app.get("/weather/locations")
def list_weather_locations(
    limit: int = Query(default=50, ge=1, le=500),
    order: str = Query(default="alpha"),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_api_key(x_api_key)

    cfg = get_s3_config()

    rows_out: list[dict[str, Any]] = []
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                _schema(
                    """
                    SELECT
                      l.id,
                      l.location_slug,
                      l.city,
                      l.country,
                      l.place_id,
                      aw.s3_bucket AS weather_bucket,
                      aw.s3_key    AS weather_key,
                      ad.s3_bucket AS daylight_bucket,
                      ad.s3_key    AS daylight_key
                    FROM "__SCHEMA__".locations l
                    LEFT JOIN LATERAL (
                      SELECT a.s3_bucket, a.s3_key
                      FROM "__SCHEMA__".assets a
                      WHERE a.location_id = l.id AND a.kind = 'weather'
                      ORDER BY a.generated_at DESC
                      LIMIT 1
                    ) aw ON true
                    LEFT JOIN LATERAL (
                      SELECT a.s3_bucket, a.s3_key
                      FROM "__SCHEMA__".assets a
                      WHERE a.location_id = l.id AND a.kind = 'daylight'
                      ORDER BY a.generated_at DESC
                      LIMIT 1
                    ) ad ON true
                    ORDER BY
                      CASE WHEN %s = 'recent' THEN l.updated_at END DESC,
                      COALESCE(NULLIF(l.city,''), l.location_slug) ASC,
                      l.country ASC,
                      l.location_slug ASC
                    LIMIT %s;
                    """
                ),
                ((order or "").strip().lower(), limit),
            )
            rows = cur.fetchall()

    for (
        _location_id,
        location_slug,
        city,
        country,
        place_id,
        weather_bucket,
        weather_key,
        daylight_bucket,
        daylight_key,
    ) in rows:
        city_s = str(city or "").strip()
        country_s = str(country or "").strip()
        label = city_s or str(location_slug)
        if city_s and country_s:
            label = f"{city_s}, {country_s}"

        weather_url = ""
        if weather_key:
            weather_url = presign_get(region=cfg.region, bucket=str(weather_bucket or cfg.bucket), key=str(weather_key), expires_in=3600)

        daylight_url = ""
        if daylight_key:
            daylight_url = presign_get(
                region=cfg.region, bucket=str(daylight_bucket or cfg.bucket), key=str(daylight_key), expires_in=3600
            )

        rows_out.append(
            {
                "location_slug": str(location_slug),
                "label": label,
                "city": city_s,
                "country": country_s,
                "place_id": str(place_id or "").strip(),
                "weather_url": weather_url,
                "daylight_url": daylight_url,
            }
        )

    return {"ok": True, "locations": rows_out}


@app.get("/", response_class=HTMLResponse)
def home() -> str:
    return """<!doctype html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>ETI360 Internal API</title>
    <style>
      body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 24px; color: #0f172a; }
      .card { max-width: 760px; border: 1px solid #e2e8f0; border-radius: 12px; padding: 16px; background: #fff; }
      h1 { margin: 0 0 8px 0; font-size: 18px; }
      ul { margin: 8px 0 0 18px; }
      code { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, \"Liberation Mono\", \"Courier New\", monospace; }
      a { color: #2563eb; text-decoration: none; }
      a:hover { text-decoration: underline; }
      .muted { color: #475569; font-size: 13px; }
    </style>
  </head>
  <body>
    <div class=\"card\">
      <h1>ETI360 Internal API</h1>
      <div class=\"muted\">This service powers internal tools. Writes can be protected by <code>X-API-Key</code>.</div>
      <ul>
        <li><a href=\"/health\">GET /health</a></li>
        <li><a href=\"/health/db\">GET /health/db</a></li>
        <li><a href=\"/docs\">Swagger UI</a></li>
        <li><a href=\"/weather/ui\">Weather UI</a></li>
      </ul>
    </div>
  </body>
</html>"""


@app.get("/health")
def health() -> dict[str, bool]:
    return {"ok": True}


@app.get("/health/db")
def health_db() -> dict[str, bool]:
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute("select 1")
                cur.fetchone()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB connection failed: {e}") from e

    return {"ok": True}


_SCHEMA_STATEMENTS: list[str] = [
    "CREATE EXTENSION IF NOT EXISTS pgcrypto;",
    _schema('CREATE SCHEMA IF NOT EXISTS "__SCHEMA__";'),
    _schema(
        """
        CREATE TABLE IF NOT EXISTS "__SCHEMA__".locations (
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
        """
    ).strip(),
    _schema(
        """
        CREATE TABLE IF NOT EXISTS "__SCHEMA__".weather_sources (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          label TEXT NOT NULL DEFAULT '',
          url TEXT NOT NULL DEFAULT '',
          accessed_utc TIMESTAMPTZ,
          notes TEXT NOT NULL DEFAULT '',
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    ).strip(),
    _schema(
        """
        CREATE TABLE IF NOT EXISTS "__SCHEMA__".weather_datasets (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          location_id UUID NOT NULL REFERENCES "__SCHEMA__".locations(id) ON DELETE CASCADE,
          source_id UUID REFERENCES "__SCHEMA__".weather_sources(id) ON DELETE SET NULL,
          title TEXT NOT NULL DEFAULT '',
          subtitle TEXT NOT NULL DEFAULT '',
          weather_overview TEXT NOT NULL DEFAULT '',
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    ).strip(),
    _schema(
        """
        CREATE TABLE IF NOT EXISTS "__SCHEMA__".weather_monthly_normals (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          dataset_id UUID NOT NULL REFERENCES "__SCHEMA__".weather_datasets(id) ON DELETE CASCADE,
          month SMALLINT NOT NULL,
          high_c NUMERIC(6,2) NOT NULL,
          low_c  NUMERIC(6,2) NOT NULL,
          precip_cm NUMERIC(8,3) NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT wmn_month_range_chk CHECK (month >= 1 AND month <= 12),
          CONSTRAINT wmn_high_ge_low_chk CHECK (high_c >= low_c),
          CONSTRAINT wmn_dataset_month_uniq UNIQUE (dataset_id, month)
        );
        """
    ).strip(),
    _schema(
        """
        CREATE TABLE IF NOT EXISTS "__SCHEMA__".assets (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          location_id UUID NOT NULL REFERENCES "__SCHEMA__".locations(id) ON DELETE CASCADE,
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
        """
    ).strip(),
    _schema('CREATE INDEX IF NOT EXISTS assets_location_kind_idx ON "__SCHEMA__".assets(location_id, kind);'),
    _schema('CREATE INDEX IF NOT EXISTS assets_generated_at_idx ON "__SCHEMA__".assets(generated_at DESC);'),
    _schema(
        """
        CREATE TABLE IF NOT EXISTS "__SCHEMA__".llm_runs (
          id UUID PRIMARY KEY,
          kind TEXT NOT NULL DEFAULT '',
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    ).strip(),
    _schema(
        """
        CREATE TABLE IF NOT EXISTS "__SCHEMA__".llm_usage (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          run_id UUID NOT NULL REFERENCES "__SCHEMA__".llm_runs(id) ON DELETE CASCADE,
          provider TEXT NOT NULL,
          model TEXT NOT NULL DEFAULT '',
          prompt_tokens INTEGER NOT NULL DEFAULT 0,
          completion_tokens INTEGER NOT NULL DEFAULT 0,
          total_tokens INTEGER NOT NULL DEFAULT 0,
          cost_usd NUMERIC(12,6) NOT NULL DEFAULT 0,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """
    ).strip(),
    _schema('CREATE INDEX IF NOT EXISTS llm_usage_run_id_idx ON "__SCHEMA__".llm_usage(run_id);'),
    _schema('CREATE INDEX IF NOT EXISTS llm_usage_created_at_idx ON "__SCHEMA__".llm_usage(created_at DESC);'),
]


@app.post("/admin/schema/init")
def admin_schema_init(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> dict[str, Any]:
    _require_api_key(x_api_key)

    applied: list[str] = []
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                for stmt in _SCHEMA_STATEMENTS:
                    cur.execute(stmt)
                    applied.append(stmt.splitlines()[0][:120])
            conn.commit()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Schema init failed: {e}") from e

    return {"ok": True, "schema": WEATHER_SCHEMA, "applied": applied}


class PlaceCandidate(BaseModel):
    name: str
    formatted_address: str = ""
    place_id: str
    lat: float | None = None
    lng: float | None = None


@app.get("/places/search")
def places_search(
    q: str = Query(..., min_length=1),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_api_key(x_api_key)
    key = _require_google_key()

    url = "https://maps.googleapis.com/maps/api/place/textsearch/json?" + urlencode({"query": q, "key": key})
    try:
        data = _fetch_json(url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Google Places request failed: {e}") from e

    status = str(data.get("status") or "")
    if status not in {"OK", "ZERO_RESULTS"}:
        raise HTTPException(status_code=400, detail=f"Google Places status: {status}")

    results = data.get("results") or []
    if not isinstance(results, list):
        results = []

    out: list[dict[str, Any]] = []
    for r in results[:5]:
        if not isinstance(r, dict):
            continue
        place_id = str(r.get("place_id") or "").strip()
        if not place_id:
            continue
        geom = (r.get("geometry") or {}).get("location") or {}
        lat = geom.get("lat")
        lng = geom.get("lng")
        out.append(
            {
                "name": str(r.get("name") or "").strip(),
                "formatted_address": str(r.get("formatted_address") or "").strip(),
                "place_id": place_id,
                "lat": float(lat) if isinstance(lat, (int, float)) else None,
                "lng": float(lng) if isinstance(lng, (int, float)) else None,
            }
        )

    return {"ok": True, "results": out}


@app.get("/places/resolve")
def places_resolve(
    q: str = Query(..., min_length=1),
    place_id: str = Query(default=""),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    """
    Convenience wrapper: run a Places search and return a single picked result.
    """
    _require_api_key(x_api_key)
    results = (places_search(q=q, x_api_key=x_api_key) or {}).get("results") or []
    if not isinstance(results, list) or not results:
        raise HTTPException(status_code=404, detail="No Google Places results")

    picked = None
    pid = (place_id or "").strip()
    if pid:
        for r in results:
            if isinstance(r, dict) and str(r.get("place_id") or "").strip() == pid:
                picked = r
                break
    if picked is None:
        picked = results[0] if isinstance(results[0], dict) else {}

    if not str(picked.get("place_id") or "").strip():
        raise HTTPException(status_code=500, detail="Picked result missing place_id")

    return {"ok": True, "picked": picked, "results": results}


class WeatherSourceIn(BaseModel):
    label: str = ""
    url: str = ""
    accessed_utc: datetime | None = None
    notes: str = ""


class WeatherPayloadIn(BaseModel):
    location_slug: str = Field(..., min_length=1)
    place_id: str = Field(..., min_length=1)
    city: str = ""
    country: str = ""
    lat: float
    lng: float
    timezone_id: str = ""

    title: str = Field(..., min_length=1)
    subtitle: str = Field(..., min_length=1)
    weather_overview: str = ""

    source: WeatherSourceIn

    high_c: list[float] = Field(..., min_length=12, max_length=12)
    low_c: list[float] = Field(..., min_length=12, max_length=12)
    precip_cm: list[float] = Field(..., min_length=12, max_length=12)


class WeatherJsonIn(BaseModel):
    # Optional: find place_id if not provided.
    location_query: str = ""
    location_slug: str = ""

    place_id: str = ""
    weather_overview: str = ""
    title: str
    subtitle: str

    source: dict[str, Any] = Field(default_factory=dict)

    months: list[str] = Field(default_factory=list)
    high_c: list[float]
    low_c: list[float]
    precip_cm: list[float]


def _save_weather_payload(payload: WeatherPayloadIn) -> dict[str, Any]:
    if len(payload.high_c) != 12 or len(payload.low_c) != 12 or len(payload.precip_cm) != 12:
        raise HTTPException(status_code=400, detail="high_c/low_c/precip_cm must have 12 values")
    for i in range(12):
        if payload.high_c[i] < payload.low_c[i]:
            raise HTTPException(status_code=400, detail=f"high_c[{i}] < low_c[{i}]")

    with _connect() as conn:
        with conn.cursor() as cur:
            # place_id is the identity; location_slug is a human-friendly handle.
            cur.execute(
                _schema('SELECT location_slug FROM "__SCHEMA__".locations WHERE place_id=%s LIMIT 1;'), (payload.place_id,)
            )
            row = cur.fetchone()
            effective_slug = str(row[0]) if row else payload.location_slug

            # If the slug is already taken by a different place, disambiguate.
            cur.execute(
                _schema('SELECT place_id FROM "__SCHEMA__".locations WHERE location_slug=%s LIMIT 1;'), (effective_slug,)
            )
            row2 = cur.fetchone()
            if row2 and str(row2[0]) != payload.place_id:
                suffix = payload.place_id[-6:].lower()
                effective_slug = _slugify(f"{effective_slug}-{suffix}")[:64]

            cur.execute(
                _schema(
                    """
                    INSERT INTO "__SCHEMA__".locations (location_slug, place_id, city, country, lat, lng, timezone_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (place_id) DO UPDATE SET
                      location_slug = EXCLUDED.location_slug,
                      city = EXCLUDED.city,
                      country = EXCLUDED.country,
                      lat = EXCLUDED.lat,
                      lng = EXCLUDED.lng,
                      timezone_id = EXCLUDED.timezone_id,
                      updated_at = now()
                    RETURNING id;
                    """
                ),
                (
                    effective_slug,
                    payload.place_id,
                    payload.city,
                    payload.country,
                    payload.lat,
                    payload.lng,
                    payload.timezone_id,
                ),
            )
            (location_id,) = cur.fetchone()  # type: ignore[misc]

            cur.execute(
                _schema(
                    'INSERT INTO "__SCHEMA__".weather_sources (label, url, accessed_utc, notes) VALUES (%s,%s,%s,%s) RETURNING id;'
                ),
                (payload.source.label, payload.source.url, payload.source.accessed_utc, payload.source.notes),
            )
            (source_id,) = cur.fetchone()  # type: ignore[misc]

            cur.execute(
                _schema(
                    """
                    INSERT INTO "__SCHEMA__".weather_datasets (location_id, source_id, title, subtitle, weather_overview)
                    VALUES (%s,%s,%s,%s,%s)
                    RETURNING id;
                    """
                ),
                (location_id, source_id, payload.title, payload.subtitle, payload.weather_overview),
            )
            (dataset_id,) = cur.fetchone()  # type: ignore[misc]

            for month in range(1, 13):
                i = month - 1
                cur.execute(
                    _schema(
                        """
                        INSERT INTO "__SCHEMA__".weather_monthly_normals (dataset_id, month, high_c, low_c, precip_cm)
                        VALUES (%s,%s,%s,%s,%s);
                        """
                    ),
                    (dataset_id, month, payload.high_c[i], payload.low_c[i], payload.precip_cm[i]),
                )

        conn.commit()

    return {"ok": True, "location_slug": effective_slug, "dataset_id": str(dataset_id)}


@app.post("/weather/payload")
def upsert_weather_payload(
    body: WeatherPayloadIn,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_api_key(x_api_key)
    return _save_weather_payload(body)


@app.post("/weather/import")
def import_weather_json(
    body: WeatherJsonIn,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_api_key(x_api_key)

    place_id = (body.place_id or "").strip()
    location_query = (body.location_query or "").strip()
    if not place_id:
        if not location_query:
            raise HTTPException(status_code=400, detail="Provide place_id or location_query")
        key = _require_google_key()
        url = "https://maps.googleapis.com/maps/api/place/textsearch/json?" + urlencode({"query": location_query, "key": key})
        data = _fetch_json(url)
        results = data.get("results") or []
        if not results:
            raise HTTPException(status_code=404, detail="No Google Places results")
        first = results[0] if isinstance(results[0], dict) else {}
        place_id = str(first.get("place_id") or "").strip()
        if not place_id:
            raise HTTPException(status_code=500, detail="Google Places did not return place_id")
        geom = (first.get("geometry") or {}).get("location") or {}
        lat = geom.get("lat")
        lng = geom.get("lng")
        lat_f = float(lat) if isinstance(lat, (int, float)) else None
        lng_f = float(lng) if isinstance(lng, (int, float)) else None
    else:
        lat_f = None
        lng_f = None

    # Lat/lng must exist for our atomic schema.
    if lat_f is None or lng_f is None:
        raise HTTPException(status_code=400, detail="lat/lng are required (use location_query so the API can resolve them)")

    location_slug = (body.location_slug or "").strip() or _slugify(location_query or body.title)

    src = body.source or {}
    source = WeatherSourceIn(
        label=str(src.get("label") or "").strip(),
        url=_extract_url(str(src.get("url") or "").strip()),
        accessed_utc=_parse_accessed_utc(str(src.get("accessed_utc") or "").strip()),
        notes=str(src.get("notes") or "").strip(),
    )

    payload = WeatherPayloadIn(
        location_slug=location_slug,
        place_id=place_id,
        city="",
        country="",
        lat=float(lat_f),
        lng=float(lng_f),
        timezone_id="",
        title=body.title.strip(),
        subtitle=body.subtitle.strip(),
        weather_overview=(body.weather_overview or "").strip(),
        source=source,
        high_c=body.high_c,
        low_c=body.low_c,
        precip_cm=body.precip_cm,
    )

    saved = _save_weather_payload(payload)
    return {"ok": True, "saved": saved, "place_id": place_id, "location_slug": location_slug, "lat": lat_f, "lng": lng_f}


def _parse_accessed_utc(s: str) -> datetime | None:
    s = (s or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _source_left(label: str, url: str) -> str:
    label = (label or "Source").strip()
    url = (url or "").strip()
    if url:
        parsed = urlparse(url)
        if parsed.scheme and parsed.netloc:
            url = f"{parsed.scheme}://{parsed.netloc}"
    return f"{label}: {url}".strip() if url else f"{label}:".strip()


class GenerateIn(BaseModel):
    location_slug: str = Field(..., min_length=1)
    year: int = 2026


@app.post("/weather/generate")
def generate_weather_png(
    body: GenerateIn,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_api_key(x_api_key)
    return _generate_weather_png_for_slug(location_slug=body.location_slug, year=body.year)


class AutoWeatherIn(BaseModel):
    location_query: str = Field(..., min_length=1)
    force_refresh: bool = False


class AutoBatchIn(BaseModel):
    locations: list[str] = Field(..., min_length=1, max_length=250)
    force_refresh: bool = False


def _auto_generate_one(
    *,
    location_query: str,
    force_refresh: bool,
) -> tuple[dict[str, Any], dict[str, int], str]:
    """
    Returns (result, perplexity_token_totals, perplexity_model).
    """
    location_query = (location_query or "").strip()
    if not location_query:
        raise HTTPException(status_code=400, detail="location_query is required")

    key = _require_google_key()
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json?" + urlencode({"query": location_query, "key": key})
    try:
        data = _fetch_json(url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Google Places request failed: {e}") from e

    status = str(data.get("status") or "")
    if status not in {"OK", "ZERO_RESULTS"}:
        raise HTTPException(status_code=400, detail=f"Google Places status: {status}")

    results = data.get("results") or []
    if not isinstance(results, list) or not results:
        raise HTTPException(status_code=404, detail="No Google Places results")
    first = results[0] if isinstance(results[0], dict) else {}

    place_id = str(first.get("place_id") or "").strip()
    if not place_id:
        raise HTTPException(status_code=500, detail="Google Places did not return place_id")
    name = str(first.get("name") or "").strip()
    formatted_address = str(first.get("formatted_address") or "").strip()
    geom = (first.get("geometry") or {}).get("location") or {}
    lat = geom.get("lat")
    lng = geom.get("lng")
    lat_f = float(lat) if isinstance(lat, (int, float)) else None
    lng_f = float(lng) if isinstance(lng, (int, float)) else None
    if lat_f is None or lng_f is None:
        raise HTTPException(status_code=500, detail="Google Places did not return lat/lng")

    picked_place = {
        "name": name,
        "formatted_address": formatted_address,
        "place_id": place_id,
        "lat": lat_f,
        "lng": lng_f,
    }

    default_slug = _slugify(location_query or name or formatted_address)

    existing_slug = ""
    existing_location_id = None
    has_dataset = False
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_schema('SELECT id, location_slug FROM "__SCHEMA__".locations WHERE place_id=%s LIMIT 1;'), (place_id,))
            r = cur.fetchone()
            if r:
                existing_location_id, existing_slug = r
                cur.execute(_schema('SELECT 1 FROM "__SCHEMA__".weather_datasets WHERE location_id=%s LIMIT 1;'), (existing_location_id,))
                has_dataset = cur.fetchone() is not None

    effective_slug = existing_slug or default_slug

    perplexity_tokens = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    perplexity_model = ""

    imported = False
    if force_refresh or not has_dataset:
        try:
            hint = f"{formatted_address} (place_id {place_id}, lat {lat_f}, lng {lng_f})".strip()
            pr = fetch_monthly_weather_normals(location_label=(name or location_query), location_hint=hint)
            payload_obj = pr.payload
            perplexity_tokens = {
                "prompt_tokens": int(pr.prompt_tokens),
                "completion_tokens": int(pr.completion_tokens),
                "total_tokens": int(pr.total_tokens),
            }
            perplexity_model = pr.model
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Perplexity fetch failed: {e}") from e

        months = payload_obj.get("months") if isinstance(payload_obj, dict) else None
        if months and months != MONTHS:
            raise HTTPException(status_code=500, detail="Model payload months must be Jan..Dec")

        src = payload_obj.get("source") if isinstance(payload_obj, dict) else None
        if not isinstance(src, dict):
            src = {}
        src_url = str(src.get("url") or "").strip()
        if not src_url and pr.citations:
            src_url = str(pr.citations[0]).strip()

        payload = WeatherPayloadIn(
            location_slug=effective_slug,
            place_id=place_id,
            city=(name or location_query).strip(),
            country=_extract_country(formatted_address),
            lat=float(lat_f),
            lng=float(lng_f),
            timezone_id="",
            title=str(payload_obj.get("title") or "").strip() or f"{(name or location_query).strip()} climate overview",
            subtitle=str(payload_obj.get("subtitle") or "").strip()
            or "Monthly average high/low temperatures and precipitation",
            weather_overview=str(payload_obj.get("weather_overview") or "").strip(),
            source=WeatherSourceIn(
                label=str(src.get("label") or "Perplexity").strip(),
                url=_extract_url(src_url),
                accessed_utc=_parse_accessed_utc(str(src.get("accessed_utc") or "").strip()) or datetime.now(timezone.utc),
                notes=str(src.get("notes") or "").strip(),
            ),
            high_c=_extract_model_float_list(payload_obj, "high_c"),
            low_c=_extract_model_float_list(payload_obj, "low_c"),
            precip_cm=_extract_model_float_list(payload_obj, "precip_cm"),
        )

        saved = _save_weather_payload(payload)
        effective_slug = str(saved.get("location_slug") or effective_slug)
        imported = True

    year = datetime.now(timezone.utc).year
    generated_weather = _generate_weather_png_for_slug(location_slug=effective_slug, year=year)
    generated_daylight = _generate_daylight_png_for_slug(location_slug=effective_slug, year=year)

    result = {
        "ok": True,
        "picked_place": picked_place,
        "location_query": location_query,
        "location_slug": effective_slug,
        "imported": imported,
        "year": year,
        "generated": {"weather": generated_weather, "daylight": generated_daylight},
    }
    return result, perplexity_tokens, perplexity_model


@app.post("/weather/auto")
def auto_weather(
    body: AutoWeatherIn,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_api_key(x_api_key)
    batch = AutoBatchIn(locations=[body.location_query], force_refresh=body.force_refresh)
    return auto_weather_batch(batch, x_api_key=x_api_key)


@app.post("/weather/auto_batch")
def auto_weather_batch(
    body: AutoBatchIn,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_api_key(x_api_key)

    locations = [str(x).strip() for x in (body.locations or []) if str(x).strip()]
    if not locations:
        raise HTTPException(status_code=400, detail="Provide at least one location")

    run_id = _create_run_id()
    results: list[dict[str, Any]] = []
    perplexity_prompt = 0
    perplexity_completion = 0
    perplexity_total = 0
    perplexity_model = ""

    for q in locations:
        try:
            res, tok, model = _auto_generate_one(location_query=q, force_refresh=body.force_refresh)
            results.append(res)
            perplexity_prompt += int(tok.get("prompt_tokens") or 0)
            perplexity_completion += int(tok.get("completion_tokens") or 0)
            perplexity_total += int(tok.get("total_tokens") or 0)
            perplexity_model = model or perplexity_model
        except Exception as e:
            results.append({"ok": False, "location_query": q, "error": str(getattr(e, "detail", e))})

    usage_rows: list[dict[str, Any]] = []
    usage_rows.append(
        _record_llm_usage(
            run_id=run_id,
            kind="weather_auto_batch",
            provider="perplexity",
            model=perplexity_model or os.environ.get("PERPLEXITY_MODEL", "").strip() or "unused",
            prompt_tokens=perplexity_prompt,
            completion_tokens=perplexity_completion,
            total_tokens=perplexity_total,
        )
    )

    # Always record OpenAI as 0 for this app (placeholder for shared tracker).
    usage_rows.append(
        _record_llm_usage(
            run_id=run_id,
            kind="weather_auto_batch",
            provider="openai",
            model=os.environ.get("OPENAI_MODEL", "").strip() or "unused",
            prompt_tokens=0,
            completion_tokens=0,
            total_tokens=0,
        )
    )

    run_cost_usd = float(sum(r.get("cost_usd", 0.0) for r in usage_rows))

    # Cumulative total cost (all runs).
    cumulative_total = 0.0
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_schema('SELECT COALESCE(SUM(cost_usd),0) FROM "__SCHEMA__".llm_usage;'))
            (cumulative_total,) = cur.fetchone()  # type: ignore[misc]

    return {
        "ok": True,
        "run_id": run_id,
        "results": results,
        "usage": usage_rows,
        "run_cost_usd": run_cost_usd,
        "cumulative_total_cost_usd": float(cumulative_total),
    }


@app.get("/weather/ui", response_class=HTMLResponse)
def weather_ui() -> str:
    return """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>ETI360 Weather</title>
    <style>
      :root { color-scheme: light; }
      body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 18px; color: #0f172a; background: #f8fafc; }
      .wrap { max-width: 1600px; margin: 0 auto; }
      header { background: white; border: 1px solid #e2e8f0; border-radius: 12px; padding: 14px; }
      h1 { margin: 0 0 6px 0; font-size: 18px; }
      .muted { color: #475569; font-size: 13px; }
      .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
      .layout { display: grid; grid-template-columns: 1fr; gap: 14px; margin-top: 14px; align-items: start; }
      @media (min-width: 1100px) { .layout { grid-template-columns: 0.9fr 1.2fr 0.9fr; } }
      .card { background: white; border: 1px solid #e2e8f0; border-radius: 12px; padding: 14px; }
      label { display: block; font-size: 12px; color: #334155; margin-bottom: 6px; }
      input[type="text"], textarea { width: 100%; box-sizing: border-box; padding: 10px 10px; border: 1px solid #cbd5e1; border-radius: 10px; font-size: 14px; outline: none; background: white; }
      textarea { min-height: 220px; resize: vertical; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
      .actions { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 10px; }
      button { padding: 10px 12px; border-radius: 10px; border: 1px solid #0f172a; background: #0f172a; color: white; cursor: pointer; font-size: 14px; }
      button.secondary { background: white; color: #0f172a; }
      button:disabled { opacity: 0.6; cursor: not-allowed; }
      .status { margin-top: 10px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; font-size: 12px; white-space: pre-wrap; }
      table { width: 100%; border-collapse: separate; border-spacing: 0; font-size: 14px; }
      th, td { text-align: left; padding: 8px 8px; border-bottom: 1px solid #e2e8f0; vertical-align: top; }
      th { font-size: 12px; letter-spacing: 0.2px; color: #475569; background: #f8fafc; }
      a { color: #2563eb; text-decoration: none; }
      a:hover { text-decoration: underline; }
      .pill { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; border: 1px solid #e2e8f0; background: #f8fafc; color: #334155; }
      .right { text-align: right; }
    </style>
  </head>
  <body>
    <div class="wrap">
      <header>
        <h1>ETI360 Weather</h1>
        <div class="muted">Paste one city per line, click Run. Column 2 shows saved cities + chart links. Column 3 tracks tokens and estimated costs.</div>
      </header>

      <div class="layout">
        <div class="card">
          <label>Location Input (one per line)</label>
          <textarea id="citiesInput" placeholder="Lima, Peru&#10;Nagasaki, Japan"></textarea>
          <div class="actions">
            <button id="btnRun" type="button">Run</button>
            <button id="btnClear" class="secondary" type="button">Clear</button>
          </div>
          <div id="status" class="status">Ready.</div>
        </div>

        <div class="card">
          <div style="display:flex; justify-content:space-between; gap:10px; align-items:baseline;">
            <h2 style="margin:0; font-size: 14px;">Cities and Links</h2>
            <span class="pill">Alphabetical</span>
          </div>
          <div style="overflow:auto; margin-top: 8px;">
            <table>
              <thead>
                <tr>
                  <th>Location</th>
                  <th>Weather</th>
                  <th>Sunlight</th>
                </tr>
              </thead>
              <tbody id="locRows"></tbody>
            </table>
          </div>
        </div>

        <div class="card">
          <h2 style="margin:0 0 10px 0; font-size: 14px;">Token Tracker</h2>
          <div class="muted">Costs use env pricing vars. If unset, costs show as $0.</div>
          <div class="divider" style="height:1px; background:#e2e8f0; margin:10px 0;"></div>
          <div id="usageBox" class="status">Loading…</div>
        </div>
      </div>
    </div>

    <script>
      const citiesEl = document.getElementById('citiesInput');
      const statusEl = document.getElementById('status');
      const locRowsEl = document.getElementById('locRows');
      const usageBoxEl = document.getElementById('usageBox');
      const btnRun = document.getElementById('btnRun');

      window.addEventListener('error', (e) => {
        statusEl.textContent = 'JS Error: ' + (e?.message || e);
      });

      function setStatus(msg) { statusEl.textContent = msg; }

      function headers() { return { 'Content-Type': 'application/json' }; }

      function saveLocal() { localStorage.setItem('eti360_weather_cities', citiesEl.value || ''); }
      function loadLocal() { citiesEl.value = localStorage.getItem('eti360_weather_cities') || ''; }
      loadLocal();
      citiesEl.addEventListener('input', saveLocal);

      function parseCities() {
        const raw = String(citiesEl.value || '');
        const lines = raw.split(/\\r?\\n/).map(s => s.trim()).filter(Boolean);
        const seen = new Set();
        const out = [];
        for (const s of lines) {
          const key = s.toLowerCase();
          if (seen.has(key)) continue;
          seen.add(key);
          out.push(s);
        }
        return out;
      }

      function renderLocations(locations) {
        locRowsEl.innerHTML = '';
        for (const r of (locations || [])) {
          const tr = document.createElement('tr');
          const locLabel = String(r.label || r.location_slug || '');
          const placeId = String(r.place_id || '');
          const weatherUrl = String(r.weather_url || '');
          const daylightUrl = String(r.daylight_url || '');
          const mapsUrl = placeId ? `https://www.google.com/maps/place/?q=place_id:${encodeURIComponent(placeId)}` : '';
          tr.innerHTML = `
            <td>${mapsUrl ? `<a href="${mapsUrl}" target="_blank" rel="noopener">${locLabel}</a>` : locLabel}</td>
            <td>${weatherUrl ? `<a href="${weatherUrl}" target="_blank" rel="noopener">Weather PNG</a>` : '<span class="muted">—</span>'}</td>
            <td>${daylightUrl ? `<a href="${daylightUrl}" target="_blank" rel="noopener">Sunlight PNG</a>` : '<span class="muted">—</span>'}</td>
          `;
          locRowsEl.appendChild(tr);
        }
        if (!locations || locations.length === 0) {
          locRowsEl.innerHTML = '<tr><td colspan="3" class="muted">No locations yet.</td></tr>';
        }
      }

      async function refreshLocations() {
        try {
          const res = await fetch('/weather/locations?limit=500&order=alpha', { headers: headers() });
          const body = await res.json().catch(() => ({}));
          if (!res.ok) throw new Error(body.detail || (res.status === 401 ? 'Auth required (set AUTH_DISABLED=true)' : `HTTP ${res.status}`));
          renderLocations(body.locations || []);
        } catch (e) {
          locRowsEl.innerHTML = `<tr><td colspan="3" class="muted">Could not load locations: ${String(e?.message || e)}</td></tr>`;
        }
      }

      function renderUsage(data) {
        if (!data || !data.ok) {
          usageBoxEl.textContent = 'Could not load usage.';
          return;
        }
        const lines = [];
        const last = data.last_run;
        if (last) {
          lines.push(`Last run: ${last.kind || ''} (${last.created_at || ''})`);
          for (const r of (last.usage || [])) {
            lines.push(`- ${r.provider} / ${r.model}: in ${r.prompt_tokens}, out ${r.completion_tokens}, total ${r.total_tokens}, cost $${Number(r.cost_usd || 0).toFixed(6)}`);
          }
          lines.push('');
        } else {
          lines.push('Last run: (none)');
          lines.push('');
        }
        lines.push('Cumulative:');
        for (const r of (data.cumulative || [])) {
          lines.push(`- ${r.provider} / ${r.model}: in ${r.prompt_tokens}, out ${r.completion_tokens}, total ${r.total_tokens}, cost $${Number(r.cost_usd || 0).toFixed(6)}`);
        }
        lines.push(``);
        lines.push(`Cumulative total: $${Number(data.cumulative_total_cost_usd || 0).toFixed(6)}`);
        usageBoxEl.textContent = lines.join('\\n');
      }

      async function refreshUsage() {
        try {
          const res = await fetch('/weather/usage', { headers: headers() });
          const body = await res.json().catch(() => ({}));
          if (!res.ok) throw new Error(body.detail || `HTTP ${res.status}`);
          renderUsage(body);
        } catch (e) {
          usageBoxEl.textContent = 'Could not load usage: ' + String(e?.message || e);
        }
      }

      async function runBatch() {
        const cities = parseCities();
        if (cities.length === 0) {
          setStatus('Enter at least one city.');
          return;
        }
        btnRun.disabled = true;
        setStatus(`Running ${cities.length}…`);
        try {
          const res = await fetch('/weather/auto_batch', {
            method: 'POST',
            headers: headers(),
            body: JSON.stringify({ locations: cities })
          });
          const body = await res.json().catch(() => ({}));
          if (!res.ok) throw new Error(body.detail || (res.status === 401 ? 'Auth required (set AUTH_DISABLED=true)' : `HTTP ${res.status}`));

          const okCount = (body.results || []).filter(r => r && r.ok).length;
          const failCount = (body.results || []).filter(r => r && !r.ok).length;
          const lines = [];
          lines.push(`Done. Run: ${body.run_id || ''}`);
          lines.push(`OK: ${okCount}, Failed: ${failCount}`);
          lines.push(`Run cost: $${Number(body.run_cost_usd || 0).toFixed(6)} | Cumulative: $${Number(body.cumulative_total_cost_usd || 0).toFixed(6)}`);
          if (failCount) {
            lines.push('');
            lines.push('Failures:');
            for (const r of (body.results || [])) {
              if (!r || r.ok) continue;
              lines.push(`- ${r.location_query}: ${r.error}`);
            }
          }
          setStatus(lines.join('\\n'));
          await refreshLocations();
          await refreshUsage();
        } catch (e) {
          setStatus('Error: ' + String(e?.message || e));
        } finally {
          btnRun.disabled = false;
        }
      }

      document.getElementById('btnRun').addEventListener('click', runBatch);
      document.getElementById('btnClear').addEventListener('click', () => { citiesEl.value = ''; saveLocal(); setStatus('Ready.'); });
      citiesEl.addEventListener('keydown', (e) => { if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') runBatch(); });

      refreshLocations();
      refreshUsage();
    </script>
  </body>
</html>"""
