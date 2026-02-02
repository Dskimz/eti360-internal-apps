from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import psycopg
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from app.weather.s3 import get_s3_config, presign_get, put_png
from app.weather.weather_chart import MONTHS, MonthlyWeather, render_weather_chart

app = FastAPI(title="ETI360 Internal API", docs_url="/docs", redoc_url=None)

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


def _connect() -> psycopg.Connection:
    return psycopg.connect(_get_database_url())


def _schema(sql: str) -> str:
    return sql.replace("__SCHEMA__", WEATHER_SCHEMA)


@app.get("/", response_class=HTMLResponse)
def home() -> str:
    return """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>ETI360 Internal API</title>
    <style>
      body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 24px; color: #0f172a; }
      .card { max-width: 760px; border: 1px solid #e2e8f0; border-radius: 12px; padding: 16px; background: #fff; }
      h1 { margin: 0 0 8px 0; font-size: 18px; }
      ul { margin: 8px 0 0 18px; }
      code { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
      a { color: #2563eb; text-decoration: none; }
      a:hover { text-decoration: underline; }
      .muted { color: #475569; font-size: 13px; }
    </style>
  </head>
  <body>
    <div class="card">
      <h1>ETI360 Internal API</h1>
      <div class="muted">This service powers internal tools. Many endpoints require <code>X-API-Key</code>.</div>
      <ul>
        <li><a href="/health">GET /health</a></li>
        <li><a href="/health/db">GET /health/db</a></li>
        <li><a href="/docs">Swagger UI</a></li>
        <li><a href="/weather/ui">Weather UI</a></li>
      </ul>
    </div>
  </body>
</html>"""





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
      header { background: white; border: 1px solid #e2e8f0; border-radius: 12px; padding: 14px 14px; }
      h1 { margin: 0 0 6px 0; font-size: 18px; }
      .muted { color: #475569; font-size: 13px; }
      .grid { display: grid; grid-template-columns: 1fr; gap: 14px; margin-top: 14px; }
      .row2 { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
      .card { background: white; border: 1px solid #e2e8f0; border-radius: 12px; padding: 14px; }
      label { display: block; font-size: 12px; color: #334155; margin-bottom: 6px; }
      input[type=\"text\"], input[type=\"number\"], textarea { width: 100%; box-sizing: border-box; padding: 10px 10px; border: 1px solid #cbd5e1; border-radius: 10px; font-size: 14px; outline: none; background: white; }
      textarea { min-height: 76px; }
      table { width: 100%; border-collapse: separate; border-spacing: 0; font-size: 14px; }
      th, td { text-align: left; padding: 8px 8px; border-bottom: 1px solid #e2e8f0; vertical-align: top; }
      th { font-size: 12px; letter-spacing: 0.2px; color: #475569; background: #f8fafc; position: sticky; top: 0; }
      .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, \"Liberation Mono\", \"Courier New\", monospace; }
      .actions { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 10px; }
      button { padding: 10px 12px; border-radius: 10px; border: 1px solid #0f172a; background: #0f172a; color: white; cursor: pointer; font-size: 14px; }
      button.secondary { background: white; color: #0f172a; }
      .status { margin-top: 10px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, \"Liberation Mono\", \"Courier New\", monospace; font-size: 12px; white-space: pre-wrap; }
      a { color: #2563eb; text-decoration: none; }
      a:hover { text-decoration: underline; }
      @media (max-width: 980px) { .row2 { grid-template-columns: 1fr; } }
    </style>
  </head>
  <body>
    <div class=\"wrap\">
      <header>
        <h1>ETI360 Weather</h1>
        <div class=\"muted\">Enter atomic monthly values, save to Postgres, then generate a PNG to S3.</div>
      </header>

      <div class=\"grid\">
        <div class=\"card\">
          <div class=\"row2\">
            <div>
              <label>API key (stored locally in this browser)</label>
              <input id=\"apiKey\" type=\"text\" placeholder=\"ETI360 API key\" autocomplete=\"off\" />
            </div>
            <div>
              <label>Year (Generate)</label>
              <input id=\"year\" type=\"number\" value=\"2026\" />
            </div>
          </div>

          <div class=\"actions\">
            <button id=\"btnSave\" type=\"button\">Save to DB</button>
            <button id=\"btnGenerate\" type=\"button\">Generate PNG</button>
            <button id=\"btnSample\" class=\"secondary\" type=\"button\">Fill sample (Fukuoka)</button>
          </div>

          <div id=\"status\" class=\"status\">Ready.</div>
        </div>

        <div class=\"card\">
          <div class=\"row2\">
            <div>
              <label>Location slug (e.g. fukuoka)</label>
              <input id=\"locationSlug\" type=\"text\" />
            </div>
            <div>
              <label>Google Place ID</label>
              <input id=\"placeId\" type=\"text\" />
            </div>
          </div>

          <div class=\"row2\" style=\"margin-top: 12px;\">
            <div>
              <label>City</label>
              <input id=\"city\" type=\"text\" />
            </div>
            <div>
              <label>Country</label>
              <input id=\"country\" type=\"text\" />
            </div>
          </div>

          <div class=\"row2\" style=\"margin-top: 12px;\">
            <div>
              <label>Latitude</label>
              <input id=\"lat\" type=\"number\" step=\"0.000001\" />
            </div>
            <div>
              <label>Longitude</label>
              <input id=\"lng\" type=\"number\" step=\"0.000001\" />
            </div>
          </div>

          <div style=\"margin-top: 12px;\">
            <label>Timezone ID (e.g. Asia/Tokyo)</label>
            <input id=\"timezoneId\" type=\"text\" />
          </div>
        </div>

        <div class=\"card\">
          <div class=\"row2\">
            <div>
              <label>Title</label>
              <input id=\"title\" type=\"text\" />
            </div>
            <div>
              <label>Subtitle</label>
              <input id=\"subtitle\" type=\"text\" />
            </div>
          </div>

          <div style=\"margin-top: 12px;\">
            <label>Weather overview (optional)</label>
            <textarea id=\"weatherOverview\" placeholder=\"Short pattern summary (optional)\"></textarea>
          </div>
        </div>

        <div class=\"card\">
          <div class=\"row2\">
            <div>
              <label>Source label</label>
              <input id=\"sourceLabel\" type=\"text\" placeholder=\"e.g. Meteostat / NOAA\" />
            </div>
            <div>
              <label>Source URL</label>
              <input id=\"sourceUrl\" type=\"text\" placeholder=\"https://...\" />
            </div>
          </div>

          <div class=\"row2\" style=\"margin-top: 12px;\">
            <div>
              <label>Source accessed (UTC, ISO-8601)</label>
              <input id=\"accessedUtc\" type=\"text\" placeholder=\"2026-02-02T00:00:00Z\" />
            </div>
            <div>
              <label>Source notes (optional)</label>
              <input id=\"sourceNotes\" type=\"text\" />
            </div>
          </div>
        </div>

        <div class=\"card\">
          <div class=\"muted\" style=\"margin-bottom: 10px;\">Monthly normals (°C / cm). Enter all 12 months.</div>
          <div style=\"overflow:auto; max-height: 420px;\">
            <table>
              <thead>
                <tr>
                  <th>Month</th>
                  <th>High (°C)</th>
                  <th>Low (°C)</th>
                  <th>Precip (cm)</th>
                </tr>
              </thead>
              <tbody id=\"months\"></tbody>
            </table>
          </div>
        </div>
      </div>
    </div>

    <script>
      window.addEventListener('error', (e) => {
        const el = document.getElementById('status');
        if (el) el.textContent = 'JS Error: ' + (e?.message || e);
      });
      const MONTHS = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];

      const apiKeyEl = document.getElementById('apiKey');
      const yearEl = document.getElementById('year');
      const statusEl = document.getElementById('status');

      const locationSlugEl = document.getElementById('locationSlug');
      const placeIdEl = document.getElementById('placeId');
      const cityEl = document.getElementById('city');
      const countryEl = document.getElementById('country');
      const latEl = document.getElementById('lat');
      const lngEl = document.getElementById('lng');
      const timezoneIdEl = document.getElementById('timezoneId');

      const titleEl = document.getElementById('title');
      const subtitleEl = document.getElementById('subtitle');
      const overviewEl = document.getElementById('weatherOverview');

      const sourceLabelEl = document.getElementById('sourceLabel');
      const sourceUrlEl = document.getElementById('sourceUrl');
      const accessedUtcEl = document.getElementById('accessedUtc');
      const sourceNotesEl = document.getElementById('sourceNotes');

      const monthsTbody = document.getElementById('months');

      function setStatus(msg) { statusEl.textContent = msg; }

      function getKey() {
        const k = String(apiKeyEl.value || '').trim();
        if (!k) throw new Error('Missing API key');
        return k;
      }

      function headers() {
        return { 'Content-Type': 'application/json', 'X-API-Key': getKey() };
      }

      function saveLocal() {
        localStorage.setItem('eti360_api_key', apiKeyEl.value || '');
        localStorage.setItem('eti360_weather_year', yearEl.value || '2026');
        localStorage.setItem('eti360_weather_location_slug', locationSlugEl.value || '');
        localStorage.setItem('eti360_weather_place_id', placeIdEl.value || '');
        localStorage.setItem('eti360_weather_city', cityEl.value || '');
        localStorage.setItem('eti360_weather_country', countryEl.value || '');
        localStorage.setItem('eti360_weather_lat', latEl.value || '');
        localStorage.setItem('eti360_weather_lng', lngEl.value || '');
        localStorage.setItem('eti360_weather_tz', timezoneIdEl.value || '');
        localStorage.setItem('eti360_weather_title', titleEl.value || '');
        localStorage.setItem('eti360_weather_subtitle', subtitleEl.value || '');
        localStorage.setItem('eti360_weather_overview', overviewEl.value || '');
        localStorage.setItem('eti360_weather_source_label', sourceLabelEl.value || '');
        localStorage.setItem('eti360_weather_source_url', sourceUrlEl.value || '');
        localStorage.setItem('eti360_weather_accessed_utc', accessedUtcEl.value || '');
        localStorage.setItem('eti360_weather_source_notes', sourceNotesEl.value || '');
        for (let i = 0; i < 12; i++) {
          localStorage.setItem(`eti360_weather_high_${i}`, document.getElementById(`high_${i}`).value || '');
          localStorage.setItem(`eti360_weather_low_${i}`, document.getElementById(`low_${i}`).value || '');
          localStorage.setItem(`eti360_weather_precip_${i}`, document.getElementById(`precip_${i}`).value || '');
        }
      }

      function loadLocal() {
        apiKeyEl.value = localStorage.getItem('eti360_api_key') || '';
        yearEl.value = localStorage.getItem('eti360_weather_year') || '2026';
        locationSlugEl.value = localStorage.getItem('eti360_weather_location_slug') || '';
        placeIdEl.value = localStorage.getItem('eti360_weather_place_id') || '';
        cityEl.value = localStorage.getItem('eti360_weather_city') || '';
        countryEl.value = localStorage.getItem('eti360_weather_country') || '';
        latEl.value = localStorage.getItem('eti360_weather_lat') || '';
        lngEl.value = localStorage.getItem('eti360_weather_lng') || '';
        timezoneIdEl.value = localStorage.getItem('eti360_weather_tz') || '';
        titleEl.value = localStorage.getItem('eti360_weather_title') || '';
        subtitleEl.value = localStorage.getItem('eti360_weather_subtitle') || '';
        overviewEl.value = localStorage.getItem('eti360_weather_overview') || '';
        sourceLabelEl.value = localStorage.getItem('eti360_weather_source_label') || '';
        sourceUrlEl.value = localStorage.getItem('eti360_weather_source_url') || '';
        accessedUtcEl.value = localStorage.getItem('eti360_weather_accessed_utc') || '';
        sourceNotesEl.value = localStorage.getItem('eti360_weather_source_notes') || '';
        for (let i = 0; i < 12; i++) {
          document.getElementById(`high_${i}`).value = localStorage.getItem(`eti360_weather_high_${i}`) || '';
          document.getElementById(`low_${i}`).value = localStorage.getItem(`eti360_weather_low_${i}`) || '';
          document.getElementById(`precip_${i}`).value = localStorage.getItem(`eti360_weather_precip_${i}`) || '';
        }
      }

      // Build months table
      for (let i = 0; i < 12; i++) {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td class="mono">${MONTHS[i]}</td>
          <td><input id="high_${i}" type="number" step="0.01" /></td>
          <td><input id="low_${i}" type="number" step="0.01" /></td>
          <td><input id="precip_${i}" type="number" step="0.001" /></td>
        `;
        monthsTbody.appendChild(tr);
      }

      loadLocal();

      document.querySelectorAll('input, textarea').forEach((el) => el.addEventListener('input', saveLocal));

      function readMonthValues(prefix) {
        const out = [];
        for (let i = 0; i < 12; i++) {
          const v = String(document.getElementById(`${prefix}_${i}`).value || '').trim();
          if (!v) throw new Error(`Missing ${prefix} for ${MONTHS[i]}`);
          const n = Number(v);
          if (!Number.isFinite(n)) throw new Error(`Invalid ${prefix} for ${MONTHS[i]}`);
          out.push(n);
        }
        return out;
      }

      function buildPayload() {
        const slug = String(locationSlugEl.value || '').trim();
        const place = String(placeIdEl.value || '').trim();
        if (!slug) throw new Error('Missing location slug');
        if (!place) throw new Error('Missing place_id');

        const lat = Number(latEl.value);
        const lng = Number(lngEl.value);
        if (!Number.isFinite(lat) || !Number.isFinite(lng)) throw new Error('Missing/invalid lat/lng');

        const title = String(titleEl.value || '').trim();
        const subtitle = String(subtitleEl.value || '').trim();
        if (!title || !subtitle) throw new Error('Missing title/subtitle');

        const high_c = readMonthValues('high');
        const low_c = readMonthValues('low');
        const precip_cm = readMonthValues('precip');
        for (let i = 0; i < 12; i++) {
          if (high_c[i] < low_c[i]) throw new Error(`High < Low for ${MONTHS[i]}`);
        }

        return {
          location_slug: slug,
          place_id: place,
          city: String(cityEl.value || '').trim(),
          country: String(countryEl.value || '').trim(),
          lat,
          lng,
          timezone_id: String(timezoneIdEl.value || '').trim(),
          title,
          subtitle,
          weather_overview: String(overviewEl.value || '').trim(),
          source: {
            label: String(sourceLabelEl.value || '').trim(),
            url: String(sourceUrlEl.value || '').trim(),
            accessed_utc: String(accessedUtcEl.value || '').trim(),
            notes: String(sourceNotesEl.value || '').trim(),
          },
          high_c,
          low_c,
          precip_cm,
        };
      }

      document.getElementById('btnSample').addEventListener('click', () => {
        locationSlugEl.value = 'fukuoka';
        placeIdEl.value = 'TEST_PLACE_ID';
        cityEl.value = 'Fukuoka';
        countryEl.value = 'Japan';
        latEl.value = '33.5902';
        lngEl.value = '130.4017';
        timezoneIdEl.value = 'Asia/Tokyo';
        titleEl.value = 'Rainfall peaks Jun–Sep';
        subtitleEl.value = 'Monthly climate normals: highs/lows (°C) and precipitation (cm)';
        overviewEl.value = '';
        sourceLabelEl.value = 'Test';
        sourceUrlEl.value = 'https://example.com';
        accessedUtcEl.value = '2026-02-02T00:00:00Z';
        sourceNotesEl.value = '';
        const high = [10,11,14,19,23,26,30,31,28,23,18,12];
        const low  = [ 3, 4, 7,11,15,20,24,25,22,16,10, 5];
        const pre  = [6,5,7,8,9,20,25,18,16,9,7,6];
        for (let i=0;i<12;i++) {
          document.getElementById(`high_${i}`).value = String(high[i]);
          document.getElementById(`low_${i}`).value = String(low[i]);
          document.getElementById(`precip_${i}`).value = String(pre[i]);
        }
        saveLocal();
        setStatus('Sample filled. Replace TEST_PLACE_ID + numbers, then click “Save to DB”.');
      });

      document.getElementById('btnSave').addEventListener('click', async () => {
        try {
          saveLocal();
          setStatus('Saving to DB…');
          const payload = buildPayload();
          const res = await fetch('/weather/payload', { method: 'POST', headers: headers(), body: JSON.stringify(payload) });
          const body = await res.json().catch(() => ({}));
          if (!res.ok) throw new Error(body.detail || `HTTP ${res.status}`);
          setStatus('Saved\n' + JSON.stringify(body, null, 2));
        } catch (e) {
          setStatus('Error: ' + (e?.message || String(e)));
        }
      });

      document.getElementById('btnGenerate').addEventListener('click', async () => {
        try {
          saveLocal();
          const slug = String(locationSlugEl.value || '').trim();
          if (!slug) throw new Error('Missing location slug');
          const year = Number(yearEl.value || 2026);
          setStatus('Generating… This can take ~10–30s.');
          const res = await fetch('/weather/generate', { method: 'POST', headers: headers(), body: JSON.stringify({ location_slug: slug, year }) });
          const body = await res.json().catch(() => ({}));
          if (!res.ok) throw new Error(body.detail || `HTTP ${res.status}`);
          let msg = 'Generated\n' + JSON.stringify(body, null, 2);
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

    # Atomic monthly values (12 rows) supplied as arrays for convenience.
    high_c: list[float] = Field(..., min_length=12, max_length=12)
    low_c: list[float] = Field(..., min_length=12, max_length=12)
    precip_cm: list[float] = Field(..., min_length=12, max_length=12)


@app.post("/weather/payload")
def upsert_weather_payload(
    body: WeatherPayloadIn,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_api_key(x_api_key)

    if len(body.high_c) != 12 or len(body.low_c) != 12 or len(body.precip_cm) != 12:
        raise HTTPException(status_code=400, detail="high_c/low_c/precip_cm must have 12 values")
    for i in range(12):
        if body.high_c[i] < body.low_c[i]:
            raise HTTPException(status_code=400, detail=f"high_c[{i}] < low_c[{i}]")

    with _connect() as conn:
        with conn.cursor() as cur:
            # Location upsert (by location_slug)
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
                (body.location_slug, body.place_id, body.city, body.country, body.lat, body.lng, body.timezone_id),
            )
            (location_id,) = cur.fetchone()  # type: ignore[misc]

            # Insert source
            cur.execute(
                _schema(
                    'INSERT INTO "__SCHEMA__".weather_sources (label, url, accessed_utc, notes) VALUES (%s,%s,%s,%s) RETURNING id;'
                ),
                (body.source.label, body.source.url, body.source.accessed_utc, body.source.notes),
            )
            (source_id,) = cur.fetchone()  # type: ignore[misc]

            # New dataset per upsert
            cur.execute(
                _schema(
                    """
                    INSERT INTO "__SCHEMA__".weather_datasets (location_id, source_id, title, subtitle, weather_overview)
                    VALUES (%s,%s,%s,%s,%s)
                    RETURNING id;
                    """
                ),
                (location_id, source_id, body.title, body.subtitle, body.weather_overview),
            )
            (dataset_id,) = cur.fetchone()  # type: ignore[misc]

            # Monthly rows
            for month in range(1, 13):
                i = month - 1
                cur.execute(
                    _schema(
                        """
                        INSERT INTO "__SCHEMA__".weather_monthly_normals (dataset_id, month, high_c, low_c, precip_cm)
                        VALUES (%s,%s,%s,%s,%s);
                        """
                    ),
                    (dataset_id, month, body.high_c[i], body.low_c[i], body.precip_cm[i]),
                )

        conn.commit()

    return {"ok": True, "location_slug": body.location_slug, "dataset_id": str(dataset_id)}


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

            # Use most recent dataset
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

    # Render to /tmp
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

    # Upload to S3
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
