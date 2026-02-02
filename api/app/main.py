from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse
from urllib.request import urlopen

import psycopg
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

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
            cur.execute(
                _schema(
                    """
                    INSERT INTO "__SCHEMA__".locations (location_slug, place_id, city, country, lat, lng, timezone_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (location_slug) DO UPDATE SET
                      place_id = EXCLUDED.place_id,
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
                    payload.location_slug,
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

    return {"ok": True, "location_slug": payload.location_slug, "dataset_id": str(dataset_id)}


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
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(_schema('SELECT id FROM "__SCHEMA__".locations WHERE location_slug=%s;'), (body.location_slug,))
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
    out_path = tmpdir / f"eti360-weather-{body.location_slug}-{body.year}.png"
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
    key = f"{cfg.prefix}{body.location_slug}/weather/{body.year}.png"
    put_png(region=cfg.region, bucket=cfg.bucket, key=key, body=png_bytes)

    generated_at = datetime.now(timezone.utc)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                _schema(
                    'INSERT INTO "__SCHEMA__".assets (location_id, kind, year, s3_bucket, s3_key, bytes, content_type, generated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;'
                ),
                (location_id, "weather", body.year, cfg.bucket, key, len(png_bytes), "image/png", generated_at),
            )
            (asset_id,) = cur.fetchone()  # type: ignore[misc]
        conn.commit()

    view_url = presign_get(region=cfg.region, bucket=cfg.bucket, key=key, expires_in=3600)
    return {"ok": True, "asset_id": str(asset_id), "s3_bucket": cfg.bucket, "s3_key": key, "view_url": view_url}


@app.get("/weather/ui", response_class=HTMLResponse)
def weather_ui() -> str:
    return """<!doctype html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>ETI360 Weather</title>
    <style>
      :root { color-scheme: light; }
      body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 18px; color: #0f172a; background: #f8fafc; }
      .wrap { max-width: 1100px; margin: 0 auto; }
      header { background: white; border: 1px solid #e2e8f0; border-radius: 12px; padding: 14px; }
      h1 { margin: 0 0 6px 0; font-size: 18px; }
      .muted { color: #475569; font-size: 13px; }
      .grid { display: grid; grid-template-columns: 1fr; gap: 14px; margin-top: 14px; }
      .row2 { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
      .card { background: white; border: 1px solid #e2e8f0; border-radius: 12px; padding: 14px; }
      label { display: block; font-size: 12px; color: #334155; margin-bottom: 6px; }
      input[type=\"text\"], input[type=\"number\"], textarea { width: 100%; box-sizing: border-box; padding: 10px 10px; border: 1px solid #cbd5e1; border-radius: 10px; font-size: 14px; outline: none; background: white; }
      textarea { min-height: 280px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, \"Liberation Mono\", \"Courier New\", monospace; }
      .actions { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 10px; }
      button { padding: 10px 12px; border-radius: 10px; border: 1px solid #0f172a; background: #0f172a; color: white; cursor: pointer; font-size: 14px; }
      button.secondary { background: white; color: #0f172a; }
      .status { margin-top: 10px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, \"Liberation Mono\", \"Courier New\", monospace; font-size: 12px; white-space: pre-wrap; }
      .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, \"Liberation Mono\", \"Courier New\", monospace; }
      table { width: 100%; border-collapse: separate; border-spacing: 0; font-size: 14px; }
      th, td { text-align: left; padding: 8px 8px; border-bottom: 1px solid #e2e8f0; vertical-align: top; }
      th { font-size: 12px; letter-spacing: 0.2px; color: #475569; background: #f8fafc; }
      a { color: #2563eb; text-decoration: none; }
      a:hover { text-decoration: underline; }
      @media (max-width: 980px) { .row2 { grid-template-columns: 1fr; } }
    </style>
  </head>
  <body>
    <div class=\"wrap\">
      <header>
        <h1>ETI360 Weather</h1>
        <div class=\"muted\">Search for a location (Google Places), paste the weather JSON format, import to atomic DB rows, then generate a PNG to S3.</div>
      </header>

      <div class=\"grid\">
        <div class=\"card\">
          <div class=\"row2\">
            <div>
              <label>API key (optional if AUTH_DISABLED=true)</label>
              <input id=\"apiKey\" type=\"text\" placeholder=\"ETI360 API key\" autocomplete=\"off\" />
            </div>
            <div>
              <label>Year (Generate)</label>
              <input id=\"year\" type=\"number\" value=\"2026\" />
            </div>
          </div>

          <div class=\"actions\">
            <button id=\"btnSearch\" class=\"secondary\" type=\"button\">Search Places</button>
            <button id=\"btnImport\" type=\"button\">Import to DB</button>
            <button id=\"btnGenerate\" type=\"button\">Generate PNG</button>
            <button id=\"btnSample\" class=\"secondary\" type=\"button\">Load sample (Bali)</button>
          </div>

          <div id=\"status\" class=\"status\">Ready.</div>
        </div>

        <div class=\"card\">
          <div class=\"row2\">
            <div>
              <label>Location search (e.g. Bali, Indonesia)</label>
              <input id=\"locationQuery\" type=\"text\" />
            </div>
            <div>
              <label>Location slug (used as ID in the system)</label>
              <input id=\"locationSlug\" type=\"text\" placeholder=\"bali\" />
              <div class=\"muted\" style=\"margin-top:6px;\">If blank, it will be auto-generated from the search.</div>
            </div>
          </div>

          <div style=\"margin-top: 12px; overflow: auto;\">
            <table>
              <thead>
                <tr>
                  <th>Pick</th>
                  <th>Name</th>
                  <th>Address</th>
                  <th class=\"mono\">Place ID</th>
                  <th class=\"mono\">Lat</th>
                  <th class=\"mono\">Lng</th>
                </tr>
              </thead>
              <tbody id=\"placeRows\"></tbody>
            </table>
          </div>

          <div class=\"muted\" style=\"margin-top: 10px;\">Selected: <span id=\"picked\" class=\"mono\">(none)</span></div>
        </div>

        <div class=\"card\">
          <label>Weather JSON (your current input format)</label>
          <textarea id=\"weatherJson\" spellcheck=\"false\"></textarea>
          <div class=\"muted\" style=\"margin-top: 8px;\">We store atomic rows in Postgres; this JSON is only an input convenience.</div>
        </div>
      </div>
    </div>

    <script>
      const apiKeyEl = document.getElementById('apiKey');
      const yearEl = document.getElementById('year');
      const locationQueryEl = document.getElementById('locationQuery');
      const locationSlugEl = document.getElementById('locationSlug');
      const weatherJsonEl = document.getElementById('weatherJson');
      const statusEl = document.getElementById('status');
      const rowsEl = document.getElementById('placeRows');
      const pickedEl = document.getElementById('picked');

      let picked = null;

      window.addEventListener('error', (e) => {
        statusEl.textContent = 'JS Error: ' + (e?.message || e);
      });

      function setStatus(msg) { statusEl.textContent = msg; }

      function headers() {
        const h = { 'Content-Type': 'application/json' };
        const k = String(apiKeyEl.value || '').trim();
        if (k) h['X-API-Key'] = k;
        return h;
      }

      function slugify(s) {
        return String(s || '').trim().toLowerCase()
          .replace(/[^a-z0-9]+/g, '-')
          .replace(/^-+|-+$/g, '')
          .slice(0, 64) || 'location';
      }

      function saveLocal() {
        localStorage.setItem('eti360_api_key', apiKeyEl.value || '');
        localStorage.setItem('eti360_weather_year', yearEl.value || '2026');
        localStorage.setItem('eti360_weather_q', locationQueryEl.value || '');
        localStorage.setItem('eti360_weather_slug', locationSlugEl.value || '');
        localStorage.setItem('eti360_weather_json', weatherJsonEl.value || '');
      }

      function loadLocal() {
        apiKeyEl.value = localStorage.getItem('eti360_api_key') || '';
        yearEl.value = localStorage.getItem('eti360_weather_year') || '2026';
        locationQueryEl.value = localStorage.getItem('eti360_weather_q') || '';
        locationSlugEl.value = localStorage.getItem('eti360_weather_slug') || '';
        weatherJsonEl.value = localStorage.getItem('eti360_weather_json') || '';
      }

      loadLocal();
      [apiKeyEl, yearEl, locationQueryEl, locationSlugEl, weatherJsonEl].forEach((el) => el.addEventListener('input', saveLocal));

      function renderPlaces(results) {
        rowsEl.innerHTML = '';
        picked = null;
        pickedEl.textContent = '(none)';

        for (const r of results) {
          const tr = document.createElement('tr');
          const id = r.place_id;
          tr.innerHTML = `
            <td><input type="radio" name="pick" value="${id}" /></td>
            <td>${String(r.name || '')}</td>
            <td>${String(r.formatted_address || '')}</td>
            <td class="mono" style="max-width: 360px; overflow:hidden; text-overflow: ellipsis;">${String(r.place_id || '')}</td>
            <td class="mono">${r.lat ?? ''}</td>
            <td class="mono">${r.lng ?? ''}</td>
          `;
          tr.querySelector('input').addEventListener('change', () => {
            picked = r;
            pickedEl.textContent = r.place_id;
            if (!locationSlugEl.value) {
              locationSlugEl.value = slugify(locationQueryEl.value || r.name || r.formatted_address);
            }
            saveLocal();
          });
          rowsEl.appendChild(tr);
        }
      }

      document.getElementById('btnSearch').addEventListener('click', async () => {
        try {
          saveLocal();
          const q = String(locationQueryEl.value || '').trim();
          if (!q) throw new Error('Enter a location search first');
          setStatus('Searching Places…');

          const res = await fetch(`/places/search?q=${encodeURIComponent(q)}`, { headers: headers() });
          const body = await res.json().catch(() => ({}));
          if (!res.ok) throw new Error(body.detail || `HTTP ${res.status}`);

          renderPlaces(body.results || []);
          setStatus(`Places found: ${(body.results || []).length}. Pick one.`);
        } catch (e) {
          setStatus('Error: ' + (e?.message || String(e)));
        }
      });

      document.getElementById('btnSample').addEventListener('click', () => {
        const sample = {
          place_id: 'ChIJoQ8Q6NNB0S0RkOYkS7EPkSQ',
          weather_overview: 'Bali has a tropical climate with consistently warm temperatures year-round and a pronounced wet season with heavy rainfall from November to March and a distinct drier period from May to October. Temperature variation is minimal throughout the year.',
          title: 'Bali’s climate has year-round warmth with a wet monsoon season',
          subtitle: 'Temperatures remain stable while rainfall peaks in the wet months and drops markedly in the dry months',
          source: {
            label: 'WeatherWonderer Bali monthly averages',
            url: 'https://weatherwonderer.com/roundups/bali/',
            accessed_utc: '2026-01-31T00:00:00Z',
            notes: 'Monthly average high, low and precipitation converted from mm to cm.'
          },
          months: ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"],
          high_c: [30, 30, 31, 31, 31, 30, 29, 29, 30, 31, 31, 30],
          low_c: [23, 23, 23, 24, 24, 23, 23, 22, 23, 24, 24, 23],
          precip_cm: [34.5, 27.4, 23.4, 10.2, 9.2, 5.1, 4.3, 2.5, 4.3, 10.7, 17.8, 28.2]
        };
        locationQueryEl.value = 'Bali, Indonesia';
        if (!locationSlugEl.value) locationSlugEl.value = 'bali';
        weatherJsonEl.value = JSON.stringify(sample, null, 2);
        saveLocal();
        setStatus('Sample loaded. Click “Search Places”, pick one, then “Import to DB”.');
      });

      document.getElementById('btnImport').addEventListener('click', async () => {
        try {
          saveLocal();
          const q = String(locationQueryEl.value || '').trim();
          if (!q) throw new Error('Enter a location search first');
          const slug = String(locationSlugEl.value || '').trim() || slugify(q);

          const raw = String(weatherJsonEl.value || '').trim();
          if (!raw) throw new Error('Paste the weather JSON first');
          const obj = JSON.parse(raw);

          // Our server endpoint accepts your JSON + our helper fields.
          const bodyIn = { ...obj, location_query: q, location_slug: slug };

          setStatus('Importing to DB…');
          const res = await fetch('/weather/import', { method: 'POST', headers: headers(), body: JSON.stringify(bodyIn) });
          const body = await res.json().catch(() => ({}));
          if (!res.ok) throw new Error(body.detail || `HTTP ${res.status}`);

          setStatus('Imported.
' + JSON.stringify(body, null, 2));
        } catch (e) {
          setStatus('Error: ' + (e?.message || String(e)));
        }
      });

      document.getElementById('btnGenerate').addEventListener('click', async () => {
        try {
          saveLocal();
          const slug = String(locationSlugEl.value || '').trim();
          if (!slug) throw new Error('Enter a location slug (or pick a place after search)');
          const year = Number(yearEl.value || 2026);

          setStatus('Generating PNG…');
          const res = await fetch('/weather/generate', { method: 'POST', headers: headers(), body: JSON.stringify({ location_slug: slug, year }) });
          const body = await res.json().catch(() => ({}));
          if (!res.ok) throw new Error(body.detail || `HTTP ${res.status}`);

          let msg = 'Generated.
' + JSON.stringify(body, null, 2);
          if (body.view_url) msg += `

Open PNG: ${body.view_url}`;
          setStatus(msg);
        } catch (e) {
          setStatus('Error: ' + (e?.message || String(e)));
        }
      });
    </script>
  </body>
</html>"""

