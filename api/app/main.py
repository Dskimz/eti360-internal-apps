from __future__ import annotations

import json
import os
import re
import tempfile
import uuid
from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import datetime, timedelta, timezone
from hashlib import pbkdf2_hmac
from pathlib import Path
from secrets import token_bytes
from typing import Any
from urllib.parse import urlencode, urlparse
from urllib.request import urlopen

import psycopg
from fastapi import FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from app.weather.perplexity import fetch_monthly_weather_normals
from app.weather.daylight_chart import DaylightInputs, compute_daylight_summary, render_daylight_chart
from app.weather.llm_usage import estimate_cost_usd
from app.weather.openai_chat import OpenAIResult, chat_text
from app.weather.s3 import get_s3_config, presign_get, put_png
from app.weather.weather_chart import MONTHS, MonthlyWeather, render_weather_chart

app = FastAPI(title="ETI360 Internal API", docs_url="/docs", redoc_url=None)

WEATHER_SCHEMA = "weather"
USAGE_SCHEMA = os.environ.get("USAGE_SCHEMA", "ops").strip() or "ops"
AUTH_SCHEMA = os.environ.get("AUTH_SCHEMA", "ops").strip() or "ops"
SESSION_COOKIE_NAME = os.environ.get("SESSION_COOKIE_NAME", "eti360_session").strip() or "eti360_session"


def _auth_disabled() -> bool:
    mode = os.environ.get("AUTH_MODE", "").strip().lower()
    if mode in {"0", "false", "no", "off", "disabled"}:
        return True
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


def _usage_schema(sql: str) -> str:
    schema = _require_safe_ident("USAGE_SCHEMA", USAGE_SCHEMA)
    return sql.replace("__SCHEMA__", schema)


def _auth_schema(sql: str) -> str:
    schema = _require_safe_ident("AUTH_SCHEMA", AUTH_SCHEMA)
    return sql.replace("__SCHEMA__", schema)


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


_SAFE_IDENT_RE = re.compile(r"^[a-zA-Z0-9_]+$")


def _require_safe_ident(label: str, value: str) -> str:
    value = (value or "").strip()
    if not value or not _SAFE_IDENT_RE.match(value):
        raise HTTPException(status_code=400, detail=f"Invalid {label}")
    return value


def _ensure_usage_tables(cur: psycopg.Cursor) -> None:
    """
    Ensure the shared usage tables exist in USAGE_SCHEMA.

    Also attempts a one-way migration from legacy weather.* tracker tables
    into the shared schema when USAGE_SCHEMA != WEATHER_SCHEMA.
    """
    cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    cur.execute(_usage_schema('CREATE SCHEMA IF NOT EXISTS "__SCHEMA__";'))

    cur.execute(
        _usage_schema(
            """
            CREATE TABLE IF NOT EXISTS "__SCHEMA__".llm_runs (
              id UUID PRIMARY KEY,
              workflow TEXT NOT NULL DEFAULT '',
              kind TEXT NOT NULL DEFAULT '',
              locations_count INTEGER NOT NULL DEFAULT 0,
              ok_count INTEGER NOT NULL DEFAULT 0,
              fail_count INTEGER NOT NULL DEFAULT 0,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        ).strip()
    )
    # For older deployments where llm_runs existed without these columns.
    cur.execute(_usage_schema('ALTER TABLE "__SCHEMA__".llm_runs ADD COLUMN IF NOT EXISTS workflow TEXT NOT NULL DEFAULT \'\';'))
    cur.execute(_usage_schema('ALTER TABLE "__SCHEMA__".llm_runs ADD COLUMN IF NOT EXISTS locations_count INTEGER NOT NULL DEFAULT 0;'))
    cur.execute(_usage_schema('ALTER TABLE "__SCHEMA__".llm_runs ADD COLUMN IF NOT EXISTS ok_count INTEGER NOT NULL DEFAULT 0;'))
    cur.execute(_usage_schema('ALTER TABLE "__SCHEMA__".llm_runs ADD COLUMN IF NOT EXISTS fail_count INTEGER NOT NULL DEFAULT 0;'))

    cur.execute(
        _usage_schema(
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
    cur.execute(_usage_schema('CREATE INDEX IF NOT EXISTS llm_usage_run_id_idx ON "__SCHEMA__".llm_usage(run_id);'))
    cur.execute(_usage_schema('CREATE INDEX IF NOT EXISTS llm_usage_created_at_idx ON "__SCHEMA__".llm_usage(created_at DESC);'))

    # Best-effort migration from legacy weather.* usage tables.
    if _require_safe_ident("USAGE_SCHEMA", USAGE_SCHEMA) != WEATHER_SCHEMA:
        cur.execute("SELECT to_regclass(%s);", (f"{WEATHER_SCHEMA}.llm_runs",))
        has_old_runs = cur.fetchone()[0] is not None  # type: ignore[index]
        cur.execute("SELECT to_regclass(%s);", (f"{WEATHER_SCHEMA}.llm_usage",))
        has_old_usage = cur.fetchone()[0] is not None  # type: ignore[index]

        if has_old_runs:
            cur.execute(_schema('ALTER TABLE "__SCHEMA__".llm_runs ADD COLUMN IF NOT EXISTS workflow TEXT NOT NULL DEFAULT \'\';'))
            cur.execute(_schema('ALTER TABLE "__SCHEMA__".llm_runs ADD COLUMN IF NOT EXISTS locations_count INTEGER NOT NULL DEFAULT 0;'))
            cur.execute(_schema('ALTER TABLE "__SCHEMA__".llm_runs ADD COLUMN IF NOT EXISTS ok_count INTEGER NOT NULL DEFAULT 0;'))
            cur.execute(_schema('ALTER TABLE "__SCHEMA__".llm_runs ADD COLUMN IF NOT EXISTS fail_count INTEGER NOT NULL DEFAULT 0;'))
            cur.execute(
                _usage_schema(
                    f"""
                    INSERT INTO "__SCHEMA__".llm_runs (id, workflow, kind, locations_count, ok_count, fail_count, created_at)
                    SELECT id, workflow, kind, locations_count, ok_count, fail_count, created_at
                    FROM {WEATHER_SCHEMA}.llm_runs
                    ON CONFLICT (id) DO NOTHING;
                    """
                ).strip()
            )

        if has_old_usage:
            cur.execute(
                _usage_schema(
                    f"""
                    INSERT INTO "__SCHEMA__".llm_usage (id, run_id, provider, model, prompt_tokens, completion_tokens, total_tokens, cost_usd, created_at)
                    SELECT id, run_id, provider, model, prompt_tokens, completion_tokens, total_tokens, cost_usd, created_at
                    FROM {WEATHER_SCHEMA}.llm_usage
                    ON CONFLICT (id) DO NOTHING;
                    """
                ).strip()
            )


_ROLE_RANK: dict[str, int] = {"viewer": 10, "account_manager": 20, "editor": 20, "admin": 30}


def _role_ge(role: str, *, required: str) -> bool:
    return _ROLE_RANK.get((role or "").strip().lower(), 0) >= _ROLE_RANK.get(required, 9999)


def _session_ttl_seconds() -> int:
    raw = os.environ.get("SESSION_TTL_DAYS", "").strip()
    try:
        days = int(raw) if raw else 30
    except Exception:
        days = 30
    days = max(1, min(days, 365))
    return days * 24 * 3600


def _hash_password(password: str) -> str:
    password = (password or "").strip()
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")
    iterations = 200_000
    salt = token_bytes(16)
    dk = pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations, dklen=32)
    return f"pbkdf2_sha256${iterations}${urlsafe_b64encode(salt).decode('ascii')}${urlsafe_b64encode(dk).decode('ascii')}"


def _verify_password(password: str, stored: str) -> bool:
    password = (password or "").strip()
    stored = (stored or "").strip()
    if not (password and stored.startswith("pbkdf2_sha256$")):
        return False
    try:
        _, iters_s, salt_b64, dk_b64 = stored.split("$", 3)
        iterations = int(iters_s)
        salt = urlsafe_b64decode(salt_b64.encode("ascii"))
        expected = urlsafe_b64decode(dk_b64.encode("ascii"))
        got = pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations, dklen=len(expected))
        return got == expected
    except Exception:
        return False


def _ensure_auth_tables(cur: psycopg.Cursor) -> None:
    """
    Create generic DB tables for users + sessions in AUTH_SCHEMA (default: ops).
    """
    cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    cur.execute(_auth_schema('CREATE SCHEMA IF NOT EXISTS "__SCHEMA__";'))
    cur.execute(
        _auth_schema(
            """
            CREATE TABLE IF NOT EXISTS "__SCHEMA__".users (
              id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
              username TEXT NOT NULL UNIQUE,
              email TEXT NOT NULL DEFAULT '',
              display_name TEXT NOT NULL DEFAULT '',
              role TEXT NOT NULL DEFAULT 'viewer',
              password_hash TEXT NOT NULL,
              is_disabled BOOLEAN NOT NULL DEFAULT FALSE,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              CONSTRAINT users_role_chk CHECK (role IN ('viewer','account_manager','editor','admin'))
            );
            """
        ).strip()
    )
    # For older deployments.
    cur.execute(_auth_schema('ALTER TABLE "__SCHEMA__".users ADD COLUMN IF NOT EXISTS email TEXT NOT NULL DEFAULT \'\';'))
    cur.execute(_auth_schema('ALTER TABLE "__SCHEMA__".users DROP CONSTRAINT IF EXISTS users_role_chk;'))
    cur.execute(
        _auth_schema(
            "ALTER TABLE \"__SCHEMA__\".users ADD CONSTRAINT users_role_chk CHECK (role IN ('viewer','account_manager','editor','admin'));"
        )
    )
    cur.execute(
        _auth_schema(
            'CREATE UNIQUE INDEX IF NOT EXISTS users_email_uniq_idx ON "__SCHEMA__".users (lower(email)) WHERE email <> \'\';'
        )
    )
    cur.execute(
        _auth_schema(
            """
            CREATE TABLE IF NOT EXISTS "__SCHEMA__".sessions (
              id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
              user_id UUID NOT NULL REFERENCES "__SCHEMA__".users(id) ON DELETE CASCADE,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              expires_at TIMESTAMPTZ NOT NULL,
              user_agent TEXT NOT NULL DEFAULT '',
              ip TEXT NOT NULL DEFAULT ''
            );
            """
        ).strip()
    )
    cur.execute(_auth_schema('CREATE INDEX IF NOT EXISTS sessions_user_id_idx ON "__SCHEMA__".sessions(user_id);'))
    cur.execute(_auth_schema('CREATE INDEX IF NOT EXISTS sessions_expires_at_idx ON "__SCHEMA__".sessions(expires_at DESC);'))


def _get_current_user(request: Request) -> dict[str, Any] | None:
    if _auth_disabled():
        return {"id": "disabled", "username": "disabled", "display_name": "Auth disabled", "role": "admin"}

    sid = (request.cookies.get(SESSION_COOKIE_NAME) or "").strip()
    if not sid:
        return None
    try:
        sid_uuid = uuid.UUID(sid)
    except Exception:
        return None

    now = datetime.now(timezone.utc)
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_auth_tables(cur)
            cur.execute(
                _auth_schema(
                    """
                    SELECT
                      u.id, u.username, u.display_name, u.role, u.is_disabled,
                      s.expires_at
                    FROM "__SCHEMA__".sessions s
                    JOIN "__SCHEMA__".users u ON u.id = s.user_id
                    WHERE s.id=%s
                    LIMIT 1;
                    """
                ),
                (sid_uuid,),
            )
            row = cur.fetchone()
            if not row:
                return None
            user_id, username, display_name, role, is_disabled, expires_at = row
            if bool(is_disabled):
                return None
            if expires_at and expires_at <= now:
                return None

            try:
                cur.execute(_auth_schema('UPDATE "__SCHEMA__".sessions SET last_seen_at=now() WHERE id=%s;'), (sid_uuid,))
                conn.commit()
            except Exception:
                conn.rollback()

            return {
                "id": str(user_id),
                "username": str(username),
                "display_name": str(display_name or ""),
                "role": str(role or "viewer"),
            }


def _require_access(
    *,
    request: Request,
    x_api_key: str | None,
    role: str = "viewer",
) -> dict[str, Any] | None:
    """
    Enforce access for sensitive endpoints.

    Priority:
    1) AUTH_MODE/AUTH_DISABLED bypass
    2) session cookie user with sufficient role
    3) X-API-Key header
    """
    if _auth_disabled():
        return {"id": "disabled", "username": "disabled", "display_name": "Auth disabled", "role": "admin"}

    user = _get_current_user(request)
    if user:
        if _role_ge(str(user.get("role") or ""), required=role):
            return user
        raise HTTPException(status_code=403, detail="Forbidden")

    _require_api_key(x_api_key)
    return {"id": "api_key", "username": "api_key", "display_name": "API key", "role": "admin"}


PROMPTS_SCHEMA = os.environ.get("PROMPTS_SCHEMA", AUTH_SCHEMA).strip() or AUTH_SCHEMA
_PROMPT_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


def _prompts_schema(sql: str) -> str:
    schema = _require_safe_ident("PROMPTS_SCHEMA", PROMPTS_SCHEMA)
    return sql.replace("__SCHEMA__", schema)


def _require_prompt_key(key: str) -> str:
    key = (key or "").strip().lower()
    if not key or not _PROMPT_KEY_RE.match(key):
        raise HTTPException(status_code=400, detail="Invalid prompt key (use a-z, 0-9, _, -, max 64 chars)")
    return key


def _ensure_prompts_tables(cur: psycopg.Cursor) -> None:
    cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    cur.execute(_prompts_schema('CREATE SCHEMA IF NOT EXISTS "__SCHEMA__";'))

    cur.execute(
        _prompts_schema(
            """
            CREATE TABLE IF NOT EXISTS "__SCHEMA__".prompts (
              id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
              prompt_key TEXT NOT NULL UNIQUE,
              name TEXT NOT NULL DEFAULT '',
              natural_name TEXT NOT NULL DEFAULT '',
              description TEXT NOT NULL DEFAULT '',
              provider TEXT NOT NULL DEFAULT '',
              model TEXT NOT NULL DEFAULT '',
              prompt_text TEXT NOT NULL DEFAULT '',
              is_active BOOLEAN NOT NULL DEFAULT TRUE,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        ).strip()
    )
    cur.execute(_prompts_schema('ALTER TABLE "__SCHEMA__".prompts ADD COLUMN IF NOT EXISTS natural_name TEXT NOT NULL DEFAULT \'\';'))
    cur.execute(_prompts_schema('CREATE INDEX IF NOT EXISTS prompts_updated_at_idx ON "__SCHEMA__".prompts(updated_at DESC);'))

    cur.execute(
        _prompts_schema(
            """
            CREATE TABLE IF NOT EXISTS "__SCHEMA__".prompt_revisions (
              id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
              prompt_id UUID NOT NULL REFERENCES "__SCHEMA__".prompts(id) ON DELETE CASCADE,
              prompt_key TEXT NOT NULL DEFAULT '',
              edited_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              edited_by_user_id UUID,
              edited_by_username TEXT NOT NULL DEFAULT '',
              edited_by_role TEXT NOT NULL DEFAULT '',
              change_note TEXT NOT NULL DEFAULT '',
              before_text TEXT NOT NULL DEFAULT '',
              after_text TEXT NOT NULL DEFAULT '',
              before_provider TEXT NOT NULL DEFAULT '',
              after_provider TEXT NOT NULL DEFAULT '',
              before_model TEXT NOT NULL DEFAULT '',
              after_model TEXT NOT NULL DEFAULT ''
            );
            """
        ).strip()
    )
    cur.execute(_prompts_schema('CREATE INDEX IF NOT EXISTS prompt_revisions_key_idx ON "__SCHEMA__".prompt_revisions(prompt_key, edited_at DESC);'))
    cur.execute(_prompts_schema('CREATE INDEX IF NOT EXISTS prompt_revisions_edited_at_idx ON "__SCHEMA__".prompt_revisions(edited_at DESC);'))


def _get_prompt_record(*, prompt_key: str) -> dict[str, str] | None:
    key = _require_prompt_key(prompt_key)
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_prompts_tables(cur)
            cur.execute(
                _prompts_schema(
                    """
                    SELECT prompt_key, provider, model, prompt_text
                    FROM "__SCHEMA__".prompts
                    WHERE prompt_key=%s AND is_active=TRUE
                    LIMIT 1;
                    """
                ),
                (key,),
            )
            row = cur.fetchone()
    if not row:
        return None
    k, provider, model, text = row
    return {
        "prompt_key": str(k),
        "provider": str(provider or "").strip(),
        "model": str(model or "").strip(),
        "prompt_text": str(text or ""),
    }


def _format_prompt_template(template: str, **kwargs: str) -> str:
    out = str(template or "")
    for k, v in kwargs.items():
        out = out.replace("{" + k + "}", str(v))
    return out


def _weather_summary(*, monthly: list[MonthlyWeather]) -> dict[str, Any]:
    highs = [float(m.high_c) for m in monthly]
    lows = [float(m.low_c) for m in monthly]
    precip = [float(m.precip_cm) for m in monthly]
    if len(highs) != 12 or len(lows) != 12 or len(precip) != 12:
        return {"note": "Expected 12 months"}

    hi_i = max(range(12), key=lambda i: highs[i])
    lo_i = min(range(12), key=lambda i: lows[i])
    wet_i = max(range(12), key=lambda i: precip[i])
    dry_i = min(range(12), key=lambda i: precip[i])

    return {
        "warmest_month": {"month": monthly[hi_i].month, "high_c": round(highs[hi_i], 1), "low_c": round(lows[hi_i], 1), "precip_cm": round(precip[hi_i], 2)},
        "coldest_month": {"month": monthly[lo_i].month, "high_c": round(highs[lo_i], 1), "low_c": round(lows[lo_i], 1), "precip_cm": round(precip[lo_i], 2)},
        "wettest_month": {"month": monthly[wet_i].month, "precip_cm": round(precip[wet_i], 2)},
        "driest_month": {"month": monthly[dry_i].month, "precip_cm": round(precip[dry_i], 2)},
        "annual_high_range_c": round(max(highs) - min(highs), 1),
        "annual_low_range_c": round(max(lows) - min(lows), 1),
        "annual_precip_total_cm": round(sum(precip), 2),
    }


def _maybe_openai_title_subtitle(
    *,
    prompt_key: str,
    display_name: str,
    summary: dict[str, Any],
) -> tuple[str, str, dict[str, int], str]:
    """
    Returns (title, subtitle, token_totals, model_used). Empty strings if OpenAI is not configured.
    """
    try:
        rec = _get_prompt_record(prompt_key=prompt_key)
        if not rec:
            return "", "", {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}, ""
        if rec.get("provider", "").lower() != "openai":
            return "", "", {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}, ""

        model = str(rec.get("model") or "").strip()
        deprecated_openai_models = {
            "gpt-4o-mini",
            "gpt-4o",
            "gpt-4.1",
            "gpt-4.1-mini",
            "gpt-4.1-nano",
            "o4-mini",
        }
        if not model or model in deprecated_openai_models:
            model = os.environ.get("OPENAI_MODEL", "").strip() or "gpt-5-mini"
        summary_json = json.dumps(summary, ensure_ascii=False)
        prompt = _format_prompt_template(rec.get("prompt_text") or "", display_name=display_name, summary_json=summary_json)
        r: OpenAIResult = chat_text(
            model=model,
            system="Follow the instructions exactly. Output only what is requested.",
            user=prompt,
            temperature=0.2,
        )

        def _norm_line(s: str) -> str:
            s = (s or "").strip()
            if not s:
                return ""
            # Common variants like "title: ..." or "- title: ..."
            s = re.sub(r"^[\\s\\-\\*]*title\\s*:\\s*", "", s, flags=re.IGNORECASE).strip()
            s = re.sub(r"^[\\s\\-\\*]*subtitle\\s*:\\s*", "", s, flags=re.IGNORECASE).strip()
            return s

        # Prefer JSON if the model still returns it, but accept the two-line format.
        title = ""
        subtitle = ""
        raw = (r.text or "").strip()
        if raw:
            try:
                obj = json.loads(raw)
                if isinstance(obj, dict):
                    title = str(obj.get("title") or "").strip()
                    subtitle = str(obj.get("subtitle") or "").strip()
            except Exception:
                pass

            if not title or not subtitle:
                lines = [_norm_line(x) for x in raw.splitlines()]
                lines = [x for x in lines if x]
                if lines:
                    title = title or lines[0]
                if len(lines) >= 2:
                    subtitle = subtitle or lines[1]

        title = title.strip()
        subtitle = subtitle.strip()
        return (
            title,
            subtitle,
            {"prompt_tokens": int(r.prompt_tokens), "completion_tokens": int(r.completion_tokens), "total_tokens": int(r.total_tokens)},
            str(r.model or model),
        )
    except Exception:
        return "", "", {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}, ""


_UI_CSS = """
:root {
  color-scheme: light;
  --primary: #002b4f;
  --accent: #ffc300;
  --text: #000814;
  --muted: rgba(0, 8, 20, 0.6);
  --bg: #F5F6F7;
  --surface: #ffffff;
  --band: #f7f7f8;
  --border: #F2F2F2;
  --tint: rgba(0, 43, 79, 0.12);
  --radius: 16px;
}

* { box-sizing: border-box; }
body {
  margin: 0;
  font-family: Roboto, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue", Arial, sans-serif;
  color: var(--text);
  background: var(--bg);
  line-height: 1.8;
}
a { color: var(--primary); text-decoration: none; }
a:hover { text-decoration: underline; }
code, .mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
}
.muted { color: var(--muted); font-size: 13px; }
.container { max-width: 1200px; margin: 0 auto; padding: 0 24px; }
.topbar { position: sticky; top: 0; z-index: 20; background: var(--surface); border-bottom: 1px solid var(--border); }
.topbar-inner { display: flex; align-items: center; justify-content: space-between; gap: 14px; padding: 14px 0; flex-wrap: wrap; }
.brand { display: flex; align-items: center; gap: 10px; font-weight: 650; letter-spacing: 0.2px; }
.brand-dot { width: 10px; height: 10px; border-radius: 999px; background: var(--accent); box-shadow: 0 0 0 4px var(--tint); }
.badge { font-size: 12px; padding: 3px 10px; border-radius: 999px; background: var(--tint); color: var(--primary); border: 1px solid var(--border); }
.nav { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }
.nav a { font-size: 13px; padding: 6px 12px; border-radius: 999px; color: var(--muted); }
.nav a.active { background: var(--tint); color: var(--primary); }
.goldline { height: 2px; background: var(--accent); }
main { padding: 22px 0 40px; }
.card { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); padding: 16px; }
.card + .card { margin-top: 16px; }
h1 { margin: 0; font-size: 20px; line-height: 1.25; }
h2 { margin: 0; font-size: 14px; letter-spacing: 0.2px; }
.section { margin-top: 16px; }
.divider { border-top: 1px solid var(--border); margin: 16px 0; }

.btnrow { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 12px; }
.btn {
  border-radius: 999px;
  padding: 10px 16px;
  font-size: 14px;
  border: 1px solid var(--border);
  background: var(--surface);
  color: var(--primary);
  cursor: pointer;
}
.btn.primary { background: var(--primary); border-color: var(--primary); color: #fff; }
.btn:hover { background: var(--band); text-decoration: none; }
.btn.primary:hover { opacity: 0.92; }
.btn:disabled { opacity: 0.6; cursor: not-allowed; }

label { display: block; font-size: 12px; color: rgba(0, 8, 20, 0.75); margin-bottom: 6px; }
input[type="text"], textarea, select {
  width: 100%;
  padding: 10px 12px;
  border: 1px solid var(--border);
  border-radius: 12px;
  font-size: 14px;
  background: var(--surface);
  outline: none;
}
input[type="text"]:focus, textarea:focus, select:focus {
  border-color: rgba(0, 43, 79, 0.35);
  box-shadow: 0 0 0 4px rgba(0, 43, 79, 0.10);
}
textarea { min-height: 220px; resize: vertical; }

table { width: 100%; border-collapse: separate; border-spacing: 0; font-size: 14px; }
th, td { text-align: left; padding: 10px 10px; border-bottom: 1px solid var(--border); vertical-align: top; }
th { font-size: 12px; letter-spacing: 0.2px; color: rgba(0, 8, 20, 0.65); background: var(--band); }
.tablewrap { overflow: auto; border: 1px solid var(--border); border-radius: 14px; background: var(--surface); }
.tablewrap table th { position: sticky; top: 0; z-index: 1; }
.right { text-align: right; }
.pill { display: inline-block; padding: 3px 10px; border-radius: 999px; font-size: 12px; border: 1px solid var(--border); background: var(--band); color: rgba(0, 8, 20, 0.75); }

.grid-2 { display: grid; grid-template-columns: 1fr; gap: 16px; align-items: start; }
@media (min-width: 1100px) { .grid-2 { grid-template-columns: 1fr 1.4fr; } }
.grid-sidebar { display: grid; grid-template-columns: 320px 1fr; gap: 16px; align-items: start; }
@media (max-width: 900px) { .grid-sidebar { grid-template-columns: 1fr; } }

.statusbox {
  margin-top: 12px;
  padding: 12px;
  border-radius: 14px;
  border: 1px solid var(--border);
  background: var(--band);
  font-size: 12px;
  white-space: pre-wrap;
}
""".strip()


def _ui_nav(*, active: str) -> str:
    items = [
        ("Apps", "/apps", "apps"),
        ("Weather", "/weather/ui", "weather"),
        ("Usage", "/usage/ui", "usage"),
        ("DB", "/db/ui", "db"),
        ("Docs", "/docs", "docs"),
        ("Health", "/health/db", "health"),
    ]
    links = []
    for label, href, key in items:
        cls = "active" if key == active else ""
        links.append(f'<a class="{cls}" href="{href}">{label}</a>')
    return "".join(links)


def _ui_shell(
    *,
    title: str,
    active: str,
    body_html: str,
    max_width_px: int = 1200,
    extra_head: str = "",
    extra_script: str = "",
    user: dict[str, Any] | None = None,
) -> str:
    right = ""
    if _auth_disabled():
        right = '<span class="muted">Auth: disabled</span>'
    elif user:
        name = (user.get("display_name") or user.get("username") or "").strip()
        role = (user.get("role") or "").strip()
        right = f'<span class="muted">Signed in as <strong>{name}</strong> · {role} · <a href="/logout">Logout</a></span>'
    else:
        right = '<a class="btn" href="/login">Login</a>'

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{title}</title>
    <style>{_UI_CSS}</style>
    {extra_head}
  </head>
  <body>
    <header class="topbar">
      <div class="container" style="max-width:{max_width_px}px;">
        <div class="topbar-inner">
          <div class="brand">
            <span class="brand-dot"></span>
            <span>ETI360</span>
            <span class="badge">Internal</span>
          </div>
          <div style="display:flex; align-items:center; gap:12px; flex-wrap:wrap; justify-content:flex-end;">
            <nav class="nav">{_ui_nav(active=active)}</nav>
            {right}
          </div>
        </div>
      </div>
      <div class="goldline"></div>
    </header>
    <main>
      <div class="container" style="max-width:{max_width_px}px;">
        {body_html}
      </div>
    </main>
    {extra_script}
  </body>
</html>"""


def _domain_only(url: str) -> str:
    url = (url or "").strip()
    if not url:
        return ""
    parsed = urlparse(url)
    if not (parsed.scheme and parsed.netloc):
        parsed = urlparse("https://" + url)
    host = (parsed.netloc or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host


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


def _generate_weather_png_for_slug(
    *, location_slug: str, year: int, title_override: str | None = None, subtitle_override: str | None = None
) -> dict[str, Any]:
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
    title_used = (title_override or "").strip() or str(title)
    subtitle_used = (subtitle_override or "").strip() or str(subtitle)

    render_weather_chart(
        project_root=Path("."),
        monthly=monthly,
        title=str(title_used),
        subtitle=str(subtitle_used),
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
    return {
        "ok": True,
        "asset_id": str(asset_id),
        "s3_bucket": cfg.bucket,
        "s3_key": key,
        "view_url": view_url,
        "title": str(title_used),
        "subtitle": str(subtitle_used),
    }


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


def _generate_daylight_png_for_slug(
    *, location_slug: str, year: int, title_override: str | None = None, subtitle_override: str | None = None
) -> dict[str, Any]:
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

    title_city = str(city or "").strip() or location_slug
    title_country = str(country or "").strip()
    display_name = f"{title_city}{', ' + title_country if title_country else ''}"
    default_title = f"{year} Sun Graph for {display_name}"
    default_subtitle = f"Rise/set times and twilight bands (nautical/civil) • {tzid}"
    title = (title_override or "").strip() or default_title
    subtitle = (subtitle_override or "").strip() or default_subtitle
    source_left = "Source: astral.readthedocs.io"

    tmpdir = Path(tempfile.gettempdir())
    out_path = tmpdir / f"eti360-daylight-{location_slug}-{year}.png"
    render_daylight_chart(
        inputs=DaylightInputs(display_name=display_name, lat=lat_f, lng=lng_f, timezone_id=tzid),
        year=year,
        output_path=out_path,
        chart_title=title,
        chart_subtitle=subtitle,
        source_left=source_left,
    )
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
    return {
        "ok": True,
        "asset_id": str(asset_id),
        "s3_bucket": cfg.bucket,
        "s3_key": key,
        "view_url": view_url,
        "title": str(title),
        "subtitle": str(subtitle),
    }


def _create_run_id() -> str:
    return str(uuid.uuid4())


def _record_llm_usage(
    *,
    run_id: str,
    workflow: str,
    kind: str,
    provider: str,
    model: str,
    prompt_tokens: int,
    completion_tokens: int,
    total_tokens: int,
    locations_count: int,
    ok_count: int,
    fail_count: int,
) -> dict[str, Any]:
    run_uuid = uuid.UUID(str(run_id))
    cost_usd = float(
        estimate_cost_usd(provider=provider, model=model, prompt_tokens=prompt_tokens, completion_tokens=completion_tokens)
    )

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_usage_tables(cur)

            cur.execute(
                _usage_schema(
                    """
                    INSERT INTO "__SCHEMA__".llm_runs (id, workflow, kind, locations_count, ok_count, fail_count)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id) DO UPDATE SET
                      workflow = EXCLUDED.workflow,
                      kind = EXCLUDED.kind,
                      locations_count = EXCLUDED.locations_count,
                      ok_count = EXCLUDED.ok_count,
                      fail_count = EXCLUDED.fail_count;
                    """
                ).strip(),
                (run_uuid, (workflow or "").strip(), kind, int(locations_count), int(ok_count), int(fail_count)),
            )
            cur.execute(
                _usage_schema(
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
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_access(request=request, x_api_key=x_api_key, role="viewer")

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_usage_tables(cur)

            cur.execute(
                _usage_schema(
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

            cur.execute(_usage_schema('SELECT id, kind, created_at FROM "__SCHEMA__".llm_runs ORDER BY created_at DESC LIMIT 1;'))
            last_run = cur.fetchone()
            last = None
            if last_run:
                run_id, kind, created_at = last_run
                cur.execute(
                    _usage_schema(
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


@app.get("/usage/log")
def usage_log(
    request: Request,
    limit: int = Query(default=200, ge=1, le=2000),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_access(request=request, x_api_key=x_api_key, role="viewer")

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_usage_tables(cur)

            cur.execute(
                _usage_schema(
                    """
                    SELECT
                      r.id,
                      r.workflow,
                      r.kind,
                      r.locations_count,
                      r.ok_count,
                      r.fail_count,
                      u.provider,
                      u.model,
                      u.prompt_tokens,
                      u.completion_tokens,
                      u.total_tokens,
                      u.cost_usd,
                      u.created_at
                    FROM "__SCHEMA__".llm_usage u
                    JOIN "__SCHEMA__".llm_runs r ON r.id = u.run_id
                    ORDER BY u.created_at DESC
                    LIMIT %s;
                    """
                ),
                (limit,),
            )
            rows = cur.fetchall()

            cur.execute(_usage_schema('SELECT COALESCE(SUM(cost_usd),0) FROM "__SCHEMA__".llm_usage;'))
            (cumulative_total,) = cur.fetchone()  # type: ignore[misc]

    items = [
        {
            "run_id": str(run_id),
            "workflow": str(workflow or ""),
            "kind": str(kind or ""),
            "locations_count": int(locations_count),
            "ok_count": int(ok_count),
            "fail_count": int(fail_count),
            "provider": str(provider),
            "model": str(model),
            "prompt_tokens": int(prompt_tokens),
            "completion_tokens": int(completion_tokens),
            "total_tokens": int(total_tokens),
            "cost_usd": float(cost_usd),
            "created_at": created_at.isoformat() if created_at else None,
        }
        for (
            run_id,
            workflow,
            kind,
            locations_count,
            ok_count,
            fail_count,
            provider,
            model,
            prompt_tokens,
            completion_tokens,
            total_tokens,
            cost_usd,
            created_at,
        ) in rows
    ]

    totals_by_provider: dict[str, dict[str, int]] = {}
    for r in items:
        p = r["provider"]
        t = totals_by_provider.setdefault(
            p, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        )
        t["prompt_tokens"] += int(r["prompt_tokens"])
        t["completion_tokens"] += int(r["completion_tokens"])
        t["total_tokens"] += int(r["total_tokens"])

    return {
        "ok": True,
        "items": items,
        "cumulative_total_cost_usd": float(cumulative_total),
        "totals_by_provider": totals_by_provider,
    }


@app.get("/usage/ui", response_class=HTMLResponse)
def usage_ui(request: Request) -> str:
    user = _get_current_user(request)
    body_html = """
      <div class="card">
        <h1>API Usage Log</h1>
        <p class="muted">LLM tokens/cost per run. Pricing comes from env vars (if unset, costs show as $0).</p>
      </div>

      <div class="card">
        <div id="summary" class="muted">Loading…</div>
        <div class="section tablewrap" style="max-height: 70vh;">
          <table>
            <thead>
              <tr>
                <th>Date (UTC)</th>
                <th>Workflow</th>
                <th>Provider</th>
                <th>Model</th>
                <th class="right">In</th>
                <th class="right">Out</th>
                <th class="right">Total</th>
                <th class="right">Cost (USD)</th>
                <th class="right">Run</th>
              </tr>
            </thead>
            <tbody id="rows"></tbody>
          </table>
        </div>
      </div>
    """.strip()

    script = """
    <script>
      const summaryEl = document.getElementById('summary');
      const rowsEl = document.getElementById('rows');

      function money(x) { return '$' + Number(x || 0).toFixed(6); }
      function safe(s) { return String(s || ''); }

      async function load() {
        try {
          const res = await fetch('/usage/log?limit=500', { headers: { 'Content-Type': 'application/json' }});
          const body = await res.json().catch(() => ({}));
          if (!res.ok) throw new Error(body.detail || `HTTP ${res.status}`);

          const totals = body.totals_by_provider || {};
          const p = totals.perplexity || { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 };
          const o = totals.openai || { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 };
          summaryEl.textContent =
            `Cumulative total: ${money(body.cumulative_total_cost_usd)} (rows: ${(body.items || []).length}) | ` +
            `Perplexity in/out: ${p.prompt_tokens}/${p.completion_tokens} | ` +
            `OpenAI in/out: ${o.prompt_tokens}/${o.completion_tokens}`;

          const items = body.items || [];
          rowsEl.innerHTML = '';
          for (const r of items) {
            const tr = document.createElement('tr');
            const date = safe(r.created_at).replace('T', ' ').replace('Z','');
            const runShort = safe(r.run_id).slice(0, 8);
            tr.innerHTML = `
              <td><code>${date}</code></td>
              <td>${safe(r.workflow || r.kind)}</td>
              <td>${safe(r.provider)}</td>
              <td><code>${safe(r.model)}</code></td>
              <td class="right"><code>${Number(r.prompt_tokens || 0)}</code></td>
              <td class="right"><code>${Number(r.completion_tokens || 0)}</code></td>
              <td class="right"><code>${Number(r.total_tokens || 0)}</code></td>
              <td class="right"><code>${money(r.cost_usd)}</code></td>
              <td class="right"><code title="${safe(r.run_id)}">${runShort}</code></td>
            `;
            rowsEl.appendChild(tr);
          }
          if (items.length === 0) {
            rowsEl.innerHTML = '<tr><td colspan="9" class="muted">No usage yet.</td></tr>';
          }
        } catch (e) {
          summaryEl.textContent = 'Error: ' + String(e?.message || e);
          rowsEl.innerHTML = '';
        }
      }

      load();
    </script>
    """.strip()

    return _ui_shell(
        title="ETI360 API Usage Log",
        active="usage",
        body_html=body_html,
        max_width_px=1400,
        extra_script=script,
        user=user,
    )


@app.get("/weather/locations")
def list_weather_locations(
    request: Request,
    limit: int = Query(default=50, ge=1, le=500),
    order: str = Query(default="alpha"),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_access(request=request, x_api_key=x_api_key, role="viewer")

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
def home(request: Request) -> str:
    return apps_home(request=request)


@app.get("/apps", response_class=HTMLResponse)
def apps_home(request: Request) -> str:
    user = _get_current_user(request)
    body_html = """
      <div class="card">
        <h1>ETI360 Internal Apps</h1>
        <p class="muted">Internal tools running inside a single Render service. Auth can be enabled via login sessions and/or <code>X-API-Key</code>. Fast iteration mode: set <code>AUTH_MODE=disabled</code>.</p>
      </div>

      <div class="card">
        <h2>Directory</h2>
        <div class="section tablewrap">
          <table>
            <thead>
              <tr>
                <th>App</th>
                <th>Description</th>
                <th>Link</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>Weather + Sunlight</td>
                <td class="muted">Batch-generate climate + daylight charts to S3.</td>
                <td><a href="/weather/ui">Open</a></td>
              </tr>
              <tr>
                <td>API Usage Log</td>
                <td class="muted">Token + cost log by workflow/provider/model.</td>
                <td><a href="/usage/ui">Open</a></td>
              </tr>
              <tr>
                <td>DB Schema</td>
                <td class="muted">Browse tables/fields (requires API key unless auth is disabled).</td>
                <td><a href="/db/ui">Open</a></td>
              </tr>
              <tr>
                <td>API Docs</td>
                <td class="muted">Interactive docs for JSON endpoints.</td>
                <td><a href="/docs">Open</a></td>
              </tr>
              <tr>
                <td>Health</td>
                <td class="muted">Service + DB connectivity checks.</td>
                <td><a href="/health">/health</a> · <a href="/health/db">/health/db</a></td>
              </tr>
              <tr>
                <td>Admin</td>
                <td class="muted">User management (roles/permissions).</td>
                <td><a href="/admin/users/ui">Open</a></td>
              </tr>
              <tr>
                <td>Prompts</td>
                <td class="muted">Review/edit prompts and audit prompt changes.</td>
                <td><a href="/prompts/ui">Open</a></td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    """.strip()

    return _ui_shell(title="ETI360 Apps", active="apps", body_html=body_html, max_width_px=1100, user=user)


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


class LoginIn(BaseModel):
    username: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request) -> str:
    user = _get_current_user(request)
    if user and not _auth_disabled():
        return _ui_shell(
            title="Login",
            active="apps",
            user=user,
            body_html="""
              <div class="card">
                <h1>Already signed in</h1>
                <p class="muted">You are already signed in. Use Logout to switch users.</p>
              </div>
            """.strip(),
            max_width_px=820,
        )

    body_html = f"""
      <div class="card">
        <h1>Login</h1>
        <p class="muted">Sign in to access admin tools (DB schema, prompt editing, etc.).</p>
        <div class="divider"></div>
        <form method="post" action="/login">
          <label>Username</label>
          <input type="text" name="username" autocomplete="username" />
          <div style="height:12px;"></div>
          <label>Password</label>
          <input type="password" name="password" autocomplete="current-password" />
          <div class="btnrow">
            <button class="btn primary" type="submit">Login</button>
            <a class="btn" href="/apps">Cancel</a>
          </div>
          <p class="muted" style="margin-top:10px;">If you’re still building, you can temporarily bypass auth by setting <code>AUTH_MODE=disabled</code>.</p>
        </form>
      </div>
    """.strip()
    return _ui_shell(title="Login", active="apps", body_html=body_html, max_width_px=820, user=None)


@app.post("/login")
async def login_submit(request: Request) -> Response:
    if _auth_disabled():
        return Response(status_code=303, headers={"Location": "/apps"})

    content_type = (request.headers.get("content-type") or "").lower()
    username = ""
    password = ""
    if "application/json" in content_type:
        payload = await request.json()
        if isinstance(payload, dict):
            username = str(payload.get("username") or "")
            password = str(payload.get("password") or "")
    else:
        form = await request.form()
        username = str(form.get("username") or "")
        password = str(form.get("password") or "")

    username = username.strip()
    password = password.strip()
    if not username or not password:
        raise HTTPException(status_code=400, detail="username and password are required")

    now = datetime.now(timezone.utc)
    ttl = _session_ttl_seconds()
    expires_at = now + timedelta(seconds=ttl)

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_auth_tables(cur)
            cur.execute(_auth_schema('SELECT id, password_hash, is_disabled FROM "__SCHEMA__".users WHERE username=%s LIMIT 1;'), (username,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=401, detail="Invalid username or password")
            user_id, password_hash, is_disabled = row
            if bool(is_disabled):
                raise HTTPException(status_code=403, detail="User is disabled")
            if not _verify_password(password, str(password_hash or "")):
                raise HTTPException(status_code=401, detail="Invalid username or password")

            cur.execute(
                _auth_schema(
                    'INSERT INTO "__SCHEMA__".sessions (user_id, expires_at, user_agent, ip) VALUES (%s,%s,%s,%s) RETURNING id;'
                ),
                (
                    user_id,
                    expires_at,
                    str(request.headers.get("user-agent") or "")[:512],
                    str(request.client.host if request.client else "")[:128],
                ),
            )
            (sid,) = cur.fetchone()  # type: ignore[misc]
        conn.commit()

    is_https = (request.headers.get("x-forwarded-proto") or "").lower() == "https" or request.url.scheme == "https"
    resp = Response(status_code=303, headers={"Location": "/apps"})
    resp.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=str(sid),
        max_age=ttl,
        httponly=True,
        samesite="lax",
        secure=is_https,
        path="/",
    )
    return resp


@app.get("/logout")
def logout(request: Request) -> Response:
    sid = (request.cookies.get(SESSION_COOKIE_NAME) or "").strip()
    try:
        sid_uuid = uuid.UUID(sid) if sid else None
    except Exception:
        sid_uuid = None

    if sid_uuid:
        try:
            with _connect() as conn:
                with conn.cursor() as cur:
                    _ensure_auth_tables(cur)
                    cur.execute(_auth_schema('DELETE FROM "__SCHEMA__".sessions WHERE id=%s;'), (sid_uuid,))
                conn.commit()
        except Exception:
            pass

    resp = Response(status_code=303, headers={"Location": "/apps"})
    resp.delete_cookie(key=SESSION_COOKIE_NAME, path="/")
    return resp


class UserCreateIn(BaseModel):
    username: str = Field(..., min_length=1)
    email: str = ""
    password: str = Field(..., min_length=8)
    display_name: str = ""
    role: str = "account_manager"


@app.get("/admin/users")
def admin_users(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_access(request=request, x_api_key=x_api_key, role="admin")

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_auth_tables(cur)
            cur.execute(
                _auth_schema(
                    """
                    SELECT id, username, display_name, role, is_disabled, created_at, updated_at
                    FROM "__SCHEMA__".users
                    ORDER BY username ASC;
                    """
                )
            )
            rows = cur.fetchall()

    users = [
        {
            "id": str(uid),
            "username": str(u),
            "display_name": str(dn or ""),
            "role": str(r or ""),
            "is_disabled": bool(dis),
            "created_at": ca.isoformat() if ca else None,
            "updated_at": ua.isoformat() if ua else None,
        }
        for (uid, u, dn, r, dis, ca, ua) in rows
    ]
    return {"ok": True, "schema": _require_safe_ident("AUTH_SCHEMA", AUTH_SCHEMA), "users": users}


@app.post("/admin/users")
def admin_users_create(
    body: UserCreateIn,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    # Admin-only, but allow bootstrap via X-API-Key even if no users exist yet.
    _require_access(request=request, x_api_key=x_api_key, role="admin")

    username = body.username.strip()
    role = (body.role or "account_manager").strip().lower()
    if role not in _ROLE_RANK:
        raise HTTPException(status_code=400, detail="Invalid role (viewer/editor/admin)")

    password_hash = _hash_password(body.password)

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_auth_tables(cur)
            cur.execute(
                _auth_schema(
                    'INSERT INTO "__SCHEMA__".users (username, email, display_name, role, password_hash) VALUES (%s,%s,%s,%s,%s) RETURNING id;'
                ),
                (username, (body.email or "").strip().lower(), (body.display_name or "").strip(), role, password_hash),
            )
            (user_id,) = cur.fetchone()  # type: ignore[misc]
        conn.commit()

    return {"ok": True, "user_id": str(user_id), "username": username, "role": role}


@app.get("/admin/users/ui", response_class=HTMLResponse)
def admin_users_ui(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="admin")

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_auth_tables(cur)
            cur.execute(
                _auth_schema(
                    """
                    SELECT username, email, display_name, role, is_disabled, created_at
                    FROM "__SCHEMA__".users
                    ORDER BY username ASC;
                    """
                )
            )
            rows = cur.fetchall()

    def _esc(s: Any) -> str:
        return (
            str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )

    users_rows = ""
    for (username, email, display_name, role, is_disabled, created_at) in rows:
        users_rows += (
            "<tr>"
            f"<td><code>{_esc(username)}</code></td>"
            f"<td class=\"muted\">{_esc(email or '')}</td>"
            f"<td>{_esc(display_name or '')}</td>"
            f"<td><span class=\"pill\">{_esc(role)}</span></td>"
            f"<td>{'YES' if bool(is_disabled) else 'NO'}</td>"
            f"<td class=\"muted\"><code>{_esc(created_at.isoformat() if created_at else '')}</code></td>"
            "</tr>"
        )
    if not users_rows:
        users_rows = '<tr><td colspan="6" class="muted">No users yet. Create the first one below.</td></tr>'

    body_html = f"""
      <div class="card">
        <h1>User Administration</h1>
        <p class="muted">Add users and assign roles. While you’re building, set <code>AUTH_MODE=disabled</code> to skip login. When enabled, sessions persist for <code>SESSION_TTL_DAYS</code>.</p>
      </div>

      <div class="card">
        <h2>Users</h2>
        <div class="section tablewrap">
          <table>
            <thead>
              <tr>
                <th>Username</th>
                <th>Email</th>
                <th>Display</th>
                <th>Role</th>
                <th>Disabled</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {users_rows}
            </tbody>
          </table>
        </div>
        <div class="muted" style="margin-top:10px;">JSON: <a href="/admin/users">/admin/users</a></div>
      </div>

      <div class="card">
        <h2>Add user</h2>
        <form id="createForm">
          <label>Username</label>
          <input type="text" name="username" placeholder="e.g. dan" />
          <div style="height:12px;"></div>

          <label>Email (optional)</label>
          <input type="text" name="email" placeholder="name@domain.com" />
          <div style="height:12px;"></div>

          <label>Display name</label>
          <input type="text" name="display_name" placeholder="e.g. Dan" />
          <div style="height:12px;"></div>

          <label>Role</label>
          <select name="role">
            <option value="account_manager" selected>Account Manager</option>
            <option value="admin">Admin</option>
          </select>
          <div style="height:12px;"></div>

          <label>Password</label>
          <input type="password" name="password" placeholder="min 8 characters" />
          <div class="btnrow">
            <button class="btn primary" type="submit">Create user</button>
            <a class="btn" href="/apps">Back</a>
          </div>
          <div id="status" class="statusbox mono" style="display:none;"></div>
        </form>
      </div>
    """.strip()

    script = """
    <script>
      const form = document.getElementById('createForm');
      const status = document.getElementById('status');
      function show(msg) { status.style.display = 'block'; status.textContent = msg; }

      form.addEventListener('submit', async (e) => {
        e.preventDefault();
        const fd = new FormData(form);
        const payload = {
          username: String(fd.get('username') || '').trim(),
          email: String(fd.get('email') || '').trim(),
          display_name: String(fd.get('display_name') || '').trim(),
          role: String(fd.get('role') || 'account_manager').trim(),
          password: String(fd.get('password') || '').trim(),
        };
        if (!payload.username || !payload.password) {
          show('Username and password are required.');
          return;
        }
        try {
          const res = await fetch('/admin/users', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
          });
          const body = await res.json().catch(() => ({}));
          if (!res.ok) throw new Error(body.detail || `HTTP ${res.status}`);
          show('Created: ' + payload.username + ' (' + payload.role + '). Refreshing…');
          setTimeout(() => window.location.reload(), 800);
        } catch (err) {
          show('Error: ' + String(err?.message || err));
        }
      });
    </script>
    """.strip()

    return _ui_shell(title="ETI360 Admin Users", active="apps", body_html=body_html, max_width_px=1100, user=user, extra_script=script)


class PromptUpsertIn(BaseModel):
    prompt_key: str = Field(..., min_length=1, max_length=64)
    name: str = ""
    natural_name: str = ""
    description: str = ""
    provider: str = ""
    model: str = ""
    prompt_text: str = ""
    change_note: str = ""


@app.get("/prompts")
def prompts_list(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_access(request=request, x_api_key=x_api_key, role="viewer")
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_prompts_tables(cur)
            cur.execute(
                _prompts_schema(
                    """
                    SELECT prompt_key, name, natural_name, description, provider, model, is_active, updated_at
                    FROM "__SCHEMA__".prompts
                    ORDER BY prompt_key ASC;
                    """
                )
            )
            rows = cur.fetchall()

    prompts = [
        {
            "prompt_key": str(k),
            "name": str(n or ""),
            "natural_name": str(nn or ""),
            "description": str(d or ""),
            "provider": str(p or ""),
            "model": str(m or ""),
            "is_active": bool(a),
            "updated_at": ua.isoformat() if ua else None,
        }
        for (k, n, nn, d, p, m, a, ua) in rows
    ]
    return {"ok": True, "schema": _require_safe_ident("PROMPTS_SCHEMA", PROMPTS_SCHEMA), "prompts": prompts}


def _default_weather_normals_prompt_template(*, accessed_utc: str, location_label: str, location_hint: str) -> str:
    hint = f"\n\nLocation hint: {location_hint.strip()}" if location_hint.strip() else ""
    return (
        f"""
Return ONLY a single JSON object (no markdown) with exactly these keys:

- title (string, <= 120 chars): headline describing the climate pattern
- subtitle (string, <= 140 chars): supporting statement
- weather_overview (string, <= 40 words)
- source (object):
  - label (string)
  - url (string, must be a real public URL)
  - accessed_utc (string, ISO8601 UTC, use "{accessed_utc}")
  - notes (string)
- months (array): ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
- high_c (array of 12 numbers): monthly average daily HIGH temperature in °C
- low_c (array of 12 numbers): monthly average daily LOW temperature in °C
- precip_cm (array of 12 numbers): monthly total precipitation in cm (NOT mm)

Constraints:
- Arrays must be length 12, in the same month order.
- For each month, high_c >= low_c.
- Prefer authoritative climate-normal sources (national met agencies, NOAA, Meteostat, etc.).

Location: {location_label.strip()}{hint}
""".strip()
    )


def _required_prompts() -> list[dict[str, str]]:
    """
    Returns prompt definitions that the codebase expects to exist.
    """
    return [
        {
            "prompt_key": "weather_normals_perplexity_v1",
            "name": "Weather normals (Perplexity)",
            "natural_name": "Perplexity: monthly climate normals JSON",
            "description": "Fetch monthly climate normals (high/low °C, precip cm) as strict JSON.",
            "provider": "perplexity",
            "model": os.environ.get("PERPLEXITY_MODEL", "").strip() or "sonar-pro",
            # Stored as a template; code will fill {accessed_utc}/{location_label}/{location_hint}.
            "prompt_text": (
                "TEMPLATE: used by code. This text is regenerated from the current code default.\n\n"
                + _default_weather_normals_prompt_template(accessed_utc="{accessed_utc}", location_label="{location_label}", location_hint="{location_hint}")
            ),
        },
        {
            "prompt_key": "weather_titles_openai_v1",
            "name": "Weather chart title/subtitle (OpenAI)",
            "natural_name": "OpenAI: weather PNG title + subtitle",
            "description": "Create a concise title and subtitle for the weather chart PNG.",
            "provider": "openai",
            "model": os.environ.get("OPENAI_MODEL", "").strip() or "gpt-5-mini",
            "prompt_text": """
Prompt: weather chart title and subtitle generation

You are writing a title and subtitle for a city weather chart.

Write in a calm, precise, non-promotional tone.
Do not use emojis.
Do not mention ETI360.
Return only the requested text.

Step 1 — Identify the climate story

First, examine the full annual weather data and identify one dominant climate story for the city.

You must consider both temperature and precipitation together.

Possible dominant stories include (choose one only):

- Strong wet or dry season
- Snow-dominated cold season
- Large annual temperature range
- Minimal seasonal variation
- Compound seasonal pattern (e.g. hot + wet summers, cold + wet winters)

Do not default to rainfall unless it is clearly the dominant feature.

Step 2 — Write the title

Role of the title

The title states the primary annual climate insight and clearly identifies the location.
It explains what kind of climate this is and where it applies.

Title rules

- Must include the city or region name
- Sentence case
- No trailing period
- Maximum 12 words
- Plain, non-technical language
- Express the climate story, not chart mechanics or variable lists

Title must not

- Describe the chart itself
- Use evaluative or promotional language
- Combine multiple climate stories

Step 3 — Write the subtitle

Role of the subtitle

The subtitle supports the title by pointing to a concrete, observable pattern in the data.
It grounds the title’s insight without restating it.

Core requirement

The subtitle must reference a different climate dimension than the title (e.g. timing, concentration, contrast, persistence) while supporting the same overall story.

Subtitle rules

- Maximum 14 words
- One clause only
- Plain, non-technical language
- Neutral and factual
- No evaluative or promotional terms

What the subtitle should do

- Explain how the title’s insight appears in the data
- Reference observable structure such as: timing, concentration, range, persistence

What the subtitle must not do

- Restate the title in different words
- Introduce a second climate story
- Summarize the entire climate
- Use technical climatology terminology
- Imply impacts, suitability, or recommendations

Failure test (must enforce)

If the subtitle could be replaced with a paraphrase of the title, it must be rewritten.

Input

- location: {display_name}
- summary: {summary_json}

Output

Return:

title
subtitle

Nothing else.
""".strip(),
        },
        {
            "prompt_key": "daylight_titles_openai_v1",
            "name": "Daylight chart title/subtitle (OpenAI)",
            "natural_name": "OpenAI: sunlight PNG title + subtitle",
            "description": "Write a title and subtitle for an annual daylight chart based on computed daylight summary.",
            "provider": "openai",
            "model": os.environ.get("OPENAI_MODEL", "").strip() or "gpt-5-mini",
            "prompt_text": """
You are writing a title and subtitle for an annual daylight chart.

Write in a calm, precise, non-promotional tone.
Do not use emojis.
Do not mention ETI360.
Return only the requested text.

Task: Write a title and subtitle for an annual daylight chart.
Location: {display_name}

Analysis guidance:
- intent: Identify the dominant annual daylight story shown by the data.
- insight hierarchy:
  1) Magnitude of daylight variation across the year
  2) Presence of extreme summer or winter day lengths
  3) Rate and symmetry of seasonal transitions
- selection rule: Choose one primary daylight insight. Do not combine multiple stories.

Title rules:
- must include city name
- max 12 words
- sentence case, no trailing period
- intent: State the primary annual daylight pattern visible in the data, not chart mechanics.

Subtitle rules:
- max 14 words
- one clause only
- no mechanics
- allowed support dimensions:
  - seasonal range
  - duration of extremes
  - timing of peak or minimum daylight
  - rate of change through the year
- dimension rule: Subtitle must reference a different observable aspect than the title while supporting the same insight.
- failure test: If the subtitle paraphrases the title or could be removed without loss of clarity, rewrite it.

Avoid terms (do not use these words): sunrise, sunset, rise/set, chart, graph, twilight

Summary (computed): {summary_json}

Output

Return:

title
subtitle

Nothing else.
""".strip(),
        },
    ]


@app.get("/prompts/required")
def prompts_required(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_access(request=request, x_api_key=x_api_key, role="viewer")
    reqs = _required_prompts()
    return {"ok": True, "required": [{k: v for k, v in r.items() if k != "prompt_text"} for r in reqs]}


@app.post("/prompts/seed")
def prompts_seed(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    actor = _require_access(request=request, x_api_key=x_api_key, role="editor") or {}
    reqs = _required_prompts()

    created: list[str] = []
    updated: list[str] = []
    skipped: list[str] = []

    username = str(actor.get("username") or "")
    role = str(actor.get("role") or "")
    user_id_uuid = None
    try:
        if str(actor.get("id") or "").strip() and str(actor.get("id")) not in {"disabled", "api_key"}:
            user_id_uuid = uuid.UUID(str(actor.get("id")))
    except Exception:
        user_id_uuid = None

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_prompts_tables(cur)
            for r in reqs:
                key = _require_prompt_key(r["prompt_key"])
                cur.execute(
                    _prompts_schema(
                        """
                        SELECT id, provider, model
                        FROM "__SCHEMA__".prompts
                        WHERE prompt_key=%s
                        LIMIT 1;
                        """
                    ),
                    (key,),
                )
                existing = cur.fetchone()
                if existing:
                    (pid, existing_provider, existing_model) = existing
                    existing_provider = str(existing_provider or "").strip().lower()
                    existing_model = str(existing_model or "").strip()

                    # Model migrations: keep user-edited prompt text, but update deprecated OpenAI models
                    # to the new default so existing prompt rows continue working after deprecations.
                    desired_model = str(r.get("model") or "").strip()
                    deprecated_openai_models = {
                        "gpt-4o-mini",
                        "gpt-4o",
                        "gpt-4.1",
                        "gpt-4.1-mini",
                        "gpt-4.1-nano",
                        "o4-mini",
                    }
                    if existing_provider == "openai" and desired_model and (not existing_model or existing_model in deprecated_openai_models):
                        cur.execute(
                            _prompts_schema(
                                """
                                UPDATE "__SCHEMA__".prompts
                                SET model=%s, updated_at=NOW()
                                WHERE id=%s;
                                """
                            ),
                            (desired_model, pid),
                        )
                        cur.execute(
                            _prompts_schema(
                                """
                                INSERT INTO "__SCHEMA__".prompt_revisions
                                  (prompt_id, prompt_key, edited_by_user_id, edited_by_username, edited_by_role, change_note,
                                   before_text, after_text, before_provider, after_provider, before_model, after_model)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                                """
                            ),
                            (
                                pid,
                                key,
                                user_id_uuid,
                                username,
                                role,
                                "Seed: migrate model default",
                                "",
                                "",
                                str(existing_provider or ""),
                                str(existing_provider or ""),
                                existing_model,
                                desired_model,
                            ),
                        )
                        updated.append(key)
                    else:
                        skipped.append(key)
                    continue
                cur.execute(
                    _prompts_schema(
                        """
                        INSERT INTO "__SCHEMA__".prompts (prompt_key, name, natural_name, description, provider, model, prompt_text)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        RETURNING id;
                        """
                    ),
                    (
                        key,
                        str(r.get("name") or ""),
                        str(r.get("natural_name") or ""),
                        str(r.get("description") or ""),
                        str(r.get("provider") or ""),
                        str(r.get("model") or ""),
                        str(r.get("prompt_text") or ""),
                    ),
                )
                (pid,) = cur.fetchone()  # type: ignore[misc]
                cur.execute(
                    _prompts_schema(
                        """
                        INSERT INTO "__SCHEMA__".prompt_revisions
                          (prompt_id, prompt_key, edited_by_user_id, edited_by_username, edited_by_role, change_note,
                           before_text, after_text, before_provider, after_provider, before_model, after_model)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                        """
                    ),
                    (
                        pid,
                        key,
                        user_id_uuid,
                        username,
                        role,
                        "Seed default prompt",
                        "",
                        str(r.get("prompt_text") or ""),
                        "",
                        str(r.get("provider") or ""),
                        "",
                        str(r.get("model") or ""),
                    ),
                )
                created.append(key)
        conn.commit()

    return {"ok": True, "created": created, "updated": updated, "skipped": skipped}


@app.get("/prompts/item/{prompt_key}")
def prompts_get(
    prompt_key: str,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_access(request=request, x_api_key=x_api_key, role="viewer")
    key = _require_prompt_key(prompt_key)
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_prompts_tables(cur)
            cur.execute(
                _prompts_schema(
                    """
                    SELECT id, prompt_key, name, natural_name, description, provider, model, prompt_text, is_active, created_at, updated_at
                    FROM "__SCHEMA__".prompts
                    WHERE prompt_key=%s
                    LIMIT 1;
                    """
                ),
                (key,),
            )
            row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Prompt not found")

    (pid, k, name, natural_name, desc, provider, model, text, is_active, created_at, updated_at) = row
    return {
        "ok": True,
        "prompt": {
            "id": str(pid),
            "prompt_key": str(k),
            "name": str(name or ""),
            "natural_name": str(natural_name or ""),
            "description": str(desc or ""),
            "provider": str(provider or ""),
            "model": str(model or ""),
            "prompt_text": str(text or ""),
            "is_active": bool(is_active),
            "created_at": created_at.isoformat() if created_at else None,
            "updated_at": updated_at.isoformat() if updated_at else None,
        },
    }


@app.post("/prompts/item/{prompt_key}")
def prompts_upsert(
    prompt_key: str,
    body: PromptUpsertIn,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    actor = _require_access(request=request, x_api_key=x_api_key, role="editor") or {}
    key = _require_prompt_key(prompt_key)
    if _require_prompt_key(body.prompt_key) != key:
        raise HTTPException(status_code=400, detail="prompt_key mismatch")

    note = (body.change_note or "").strip()
    if not note:
        note = "Update prompt"

    username = str(actor.get("username") or "")
    role = str(actor.get("role") or "")
    user_id_uuid = None
    try:
        if str(actor.get("id") or "").strip() and str(actor.get("id")) not in {"disabled", "api_key"}:
            user_id_uuid = uuid.UUID(str(actor.get("id")))
    except Exception:
        user_id_uuid = None

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_prompts_tables(cur)

            cur.execute(
                _prompts_schema(
                    """
                    SELECT id, provider, model, prompt_text
                    FROM "__SCHEMA__".prompts
                    WHERE prompt_key=%s
                    LIMIT 1;
                    """
                ),
                (key,),
            )
            existing = cur.fetchone()

            before_provider = ""
            before_model = ""
            before_text = ""
            prompt_id = None

            if existing:
                prompt_id, before_provider, before_model, before_text = existing

                cur.execute(
                    _prompts_schema(
                        """
                        UPDATE "__SCHEMA__".prompts
                        SET name=%s, natural_name=%s, description=%s, provider=%s, model=%s, prompt_text=%s, updated_at=now()
                        WHERE id=%s;
                        """
                    ),
                    (
                        (body.name or "").strip(),
                        (body.natural_name or "").strip(),
                        (body.description or "").strip(),
                        (body.provider or "").strip(),
                        (body.model or "").strip(),
                        (body.prompt_text or ""),
                        prompt_id,
                    ),
                )
            else:
                cur.execute(
                    _prompts_schema(
                        """
                        INSERT INTO "__SCHEMA__".prompts (prompt_key, name, natural_name, description, provider, model, prompt_text)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        RETURNING id;
                        """
                    ),
                    (
                        key,
                        (body.name or "").strip(),
                        (body.natural_name or "").strip(),
                        (body.description or "").strip(),
                        (body.provider or "").strip(),
                        (body.model or "").strip(),
                        (body.prompt_text or ""),
                    ),
                )
                (prompt_id,) = cur.fetchone()  # type: ignore[misc]

            cur.execute(
                _prompts_schema(
                    """
                    INSERT INTO "__SCHEMA__".prompt_revisions
                      (prompt_id, prompt_key, edited_by_user_id, edited_by_username, edited_by_role, change_note,
                       before_text, after_text, before_provider, after_provider, before_model, after_model)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                    """
                ),
                (
                    prompt_id,
                    key,
                    user_id_uuid,
                    username,
                    role,
                    note,
                    str(before_text or ""),
                    str(body.prompt_text or ""),
                    str(before_provider or ""),
                    str((body.provider or "").strip()),
                    str(before_model or ""),
                    str((body.model or "").strip()),
                ),
            )

        conn.commit()

    return {"ok": True, "prompt_key": key, "edited_by": {"username": username, "role": role}, "note": note}


@app.get("/prompts/log")
def prompts_log(
    request: Request,
    limit: int = Query(default=200, ge=1, le=2000),
    prompt_key: str = Query(default=""),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_access(request=request, x_api_key=x_api_key, role="viewer")
    key = (prompt_key or "").strip().lower()
    if key:
        key = _require_prompt_key(key)

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_prompts_tables(cur)
            if key:
                cur.execute(
                    _prompts_schema(
                        """
                        SELECT edited_at, prompt_key, edited_by_username, edited_by_role, change_note
                        FROM "__SCHEMA__".prompt_revisions
                        WHERE prompt_key=%s
                        ORDER BY edited_at DESC
                        LIMIT %s;
                        """
                    ),
                    (key, limit),
                )
            else:
                cur.execute(
                    _prompts_schema(
                        """
                        SELECT edited_at, prompt_key, edited_by_username, edited_by_role, change_note
                        FROM "__SCHEMA__".prompt_revisions
                        ORDER BY edited_at DESC
                        LIMIT %s;
                        """
                    ),
                    (limit,),
                )
            rows = cur.fetchall()

    items = [
        {
            "edited_at": ea.isoformat() if ea else None,
            "prompt_key": str(pk),
            "user": str(u or ""),
            "role": str(r or ""),
            "note": str(n or ""),
        }
        for (ea, pk, u, r, n) in rows
    ]
    return {"ok": True, "schema": _require_safe_ident("PROMPTS_SCHEMA", PROMPTS_SCHEMA), "items": items}


@app.get("/prompts/ui", response_class=HTMLResponse)
def prompts_ui(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="viewer") or {}

    can_edit = _role_ge(str(user.get("role") or ""), required="editor")

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_prompts_tables(cur)
            cur.execute(
                _prompts_schema(
                    """
                    SELECT prompt_key, natural_name, provider, model, is_active, updated_at
                    FROM "__SCHEMA__".prompts
                    ORDER BY prompt_key ASC;
                    """
                )
            )
            rows = cur.fetchall()

    existing = {str(r[0]) for r in rows}
    required = _required_prompts()
    missing_required = [r for r in required if str(r.get("prompt_key") or "") not in existing]

    def _esc(s: Any) -> str:
        return (
            str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )

    table_rows = ""
    for (k, natural_name, provider, model, is_active, updated_at) in rows:
        key = str(k)
        table_rows += (
            "<tr>"
            f"<td><code>{_esc(key)}</code></td>"
            f"<td>{_esc(natural_name or '')}</td>"
            f"<td class=\"muted\">{_esc(provider or '')}</td>"
            f"<td class=\"muted\"><code>{_esc(model or '')}</code></td>"
            f"<td>{'YES' if bool(is_active) else 'NO'}</td>"
            f"<td class=\"muted\"><code>{_esc(updated_at.isoformat() if updated_at else '')}</code></td>"
            f"<td><a class=\"btn\" href=\"/prompts/edit?prompt_key={_esc(key)}\">Details</a> <a class=\"btn\" href=\"/prompts/log/ui?prompt_key={_esc(key)}\">Log</a></td>"
            "</tr>"
        )
    if not table_rows:
        table_rows = '<tr><td colspan="7" class="muted">No prompts yet. Click “Seed defaults” to create the expected prompts.</td></tr>'

    missing_html = ""
    if missing_required:
        items = "".join(f"<li><code>{_esc(r['prompt_key'])}</code> <span class=\"muted\">— {_esc(r.get('natural_name') or r.get('name') or '')}</span></li>" for r in missing_required)
        missing_html = f"""
        <div class="card">
          <h2>Missing required prompts</h2>
          <div class="muted">These prompts are referenced by the codebase and should exist in the DB.</div>
          <ul style="margin: 10px 0 0 18px; padding: 0;">{items}</ul>
        </div>
        """.strip()

    body_html = f"""
      <div class="card">
        <h1>Prompts</h1>
        <p class="muted">Central place to review and edit prompts. Every save writes an audit log (UTC timestamp + user + note).</p>
        <div class="btnrow">
          <button id="btnSeed" class="btn" type="button" {'disabled' if not can_edit else ''}>Seed defaults</button>
          <a class="btn" href="/prompts/log/ui">View change log</a>
        </div>
        <div id="seedStatus" class="statusbox mono" style="display:none;"></div>
      </div>

      {missing_html}

      <div class="card">
        <h2>Prompt inventory</h2>
        <div class="muted">This table is the source of truth for prompts used by workflows.</div>
        <div class="section tablewrap">
          <table>
            <thead>
              <tr>
                <th>Key</th>
                <th>Natural name</th>
                <th>Provider</th>
                <th>Model</th>
                <th>Active</th>
                <th>Updated</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {table_rows}
            </tbody>
          </table>
        </div>
      </div>
    """.strip()

    script = f"""
    <script>
      const btn = document.getElementById('btnSeed');
      const status = document.getElementById('seedStatus');
      function show(msg) {{ status.style.display = 'block'; status.textContent = msg; }}

      btn && btn.addEventListener('click', async () => {{
        btn.disabled = true;
        show('Seeding…');
        try {{
          const res = await fetch('/prompts/seed', {{ method: 'POST', headers: {{ 'Content-Type': 'application/json' }} }});
          const body = await res.json().catch(() => ({{}}));
          if (!res.ok) throw new Error(body.detail || `HTTP ${{res.status}}`);
          show('Created: ' + (body.created || []).join(', ') + (body.skipped?.length ? '\\nSkipped: ' + body.skipped.join(', ') : ''));
          setTimeout(() => window.location.reload(), 700);
        }} catch (e) {{
          show('Error: ' + String(e.message || e));
          btn.disabled = false;
        }}
      }});
    </script>
    """.strip()

    return _ui_shell(title="ETI360 Prompts", active="apps", body_html=body_html, max_width_px=1400, user=user, extra_script=script)


@app.get("/prompts/edit", response_class=HTMLResponse)
def prompts_edit_ui(
    request: Request,
    prompt_key: str = Query(..., min_length=1),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="viewer") or {}
    key = _require_prompt_key(prompt_key)
    can_edit = _role_ge(str(user.get("role") or ""), required="editor")

    prompt = None
    try:
        prompt = prompts_get(prompt_key=key, request=request, x_api_key=x_api_key).get("prompt")
    except Exception:
        prompt = None
    if not isinstance(prompt, dict):
        raise HTTPException(status_code=404, detail="Prompt not found")

    p = prompt

    def _esc(s: Any) -> str:
        return (
            str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )

    body_html = f"""
      <div class="card">
        <h1>Prompt details</h1>
        <p class="muted">Key: <code>{_esc(p.get('prompt_key') or '')}</code></p>
        <div class="btnrow">
          <a class="btn" href="/prompts/ui">Back to prompts</a>
          <a class="btn" href="/prompts/log/ui?prompt_key={_esc(p.get('prompt_key') or '')}">View log</a>
        </div>
      </div>

      <div class="card">
        <form id="form">
          <label>Natural language name (human)</label>
          <input id="natural_name" type="text" value="{_esc(p.get('natural_name') or '')}" {'disabled' if not can_edit else ''} />
          <div style="height:12px;"></div>

          <label>Name (system)</label>
          <input id="name" type="text" value="{_esc(p.get('name') or '')}" {'disabled' if not can_edit else ''} />
          <div style="height:12px;"></div>

          <label>Description</label>
          <input id="description" type="text" value="{_esc(p.get('description') or '')}" {'disabled' if not can_edit else ''} />
          <div style="height:12px;"></div>

          <label>Provider</label>
          <input id="provider" type="text" value="{_esc(p.get('provider') or '')}" placeholder="perplexity / openai" {'disabled' if not can_edit else ''} />
          <div style="height:12px;"></div>

          <label>Model</label>
          <input id="model" type="text" value="{_esc(p.get('model') or '')}" placeholder="sonar-pro / gpt-5-mini" {'disabled' if not can_edit else ''} />
          <div style="height:12px;"></div>

          <label>Prompt text</label>
          <textarea id="prompt_text" class="mono" {'disabled' if not can_edit else ''}>{_esc(p.get('prompt_text') or '')}</textarea>
          <div style="height:12px;"></div>

          <label>Change note</label>
          <input id="change_note" type="text" placeholder="What changed and why?" {'disabled' if not can_edit else ''} />

          <div class="btnrow">
            <button id="btnSave" class="btn primary" type="button" {'disabled' if not can_edit else ''}>Save</button>
          </div>

          <div id="status" class="statusbox mono" style="display:none;"></div>
        </form>
      </div>
    """.strip()

    script = f"""
    <script>
      const canEdit = {json.dumps(bool(can_edit))};
      const status = document.getElementById('status');
      const btnSave = document.getElementById('btnSave');
      function show(msg) {{ status.style.display = 'block'; status.textContent = msg; }}

      btnSave && btnSave.addEventListener('click', async () => {{
        if (!canEdit) return;
        btnSave.disabled = true;
        show('Saving…');
        const payload = {{
          prompt_key: {json.dumps(key)},
          natural_name: String(document.getElementById('natural_name').value || ''),
          name: String(document.getElementById('name').value || ''),
          description: String(document.getElementById('description').value || ''),
          provider: String(document.getElementById('provider').value || ''),
          model: String(document.getElementById('model').value || ''),
          prompt_text: String(document.getElementById('prompt_text').value || ''),
          change_note: String(document.getElementById('change_note').value || '').trim() || 'Update prompt'
        }};
        try {{
          const res = await fetch('/prompts/item/' + encodeURIComponent(payload.prompt_key), {{
            method: 'POST',
            headers: {{ 'Content-Type': 'application/json' }},
            body: JSON.stringify(payload)
          }});
          const body = await res.json().catch(() => ({{}}));
          if (!res.ok) throw new Error(body.detail || `HTTP ${{res.status}}`);
          show('Saved.');
          setTimeout(() => window.location.href = '/prompts/ui', 600);
        }} catch (e) {{
          show('Error: ' + String(e.message || e));
          btnSave.disabled = false;
        }}
      }});
    </script>
    """.strip()

    return _ui_shell(title="ETI360 Prompt Details", active="apps", body_html=body_html, max_width_px=1100, user=user, extra_script=script)


@app.get("/prompts/log/ui", response_class=HTMLResponse)
def prompts_log_ui(
    request: Request,
    prompt_key: str = Query(default="", min_length=0),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="viewer")
    key = (prompt_key or "").strip().lower()
    if key:
        key = _require_prompt_key(key)

    body_html = f"""
      <div class="card">
        <h1>Prompt Change Log</h1>
        <p class="muted">Audit trail of prompt edits (UTC). Filter by prompt key.</p>
      </div>

      <div class="card">
        <label>Prompt key (optional)</label>
        <div style="display:flex; gap:10px; flex-wrap:wrap;">
          <input id="key" type="text" value="{key}" placeholder="e.g. weather_normals_v1" style="max-width:420px;" />
          <button id="btnFilter" class="btn primary" type="button">Filter</button>
          <a class="btn" href="/prompts/ui">Back to prompts</a>
        </div>
        <div class="divider"></div>
        <div class="section tablewrap" style="max-height: 70vh;">
          <table>
            <thead>
              <tr>
                <th>Date (UTC)</th>
                <th>Prompt</th>
                <th>User</th>
                <th>Role</th>
                <th>Note</th>
              </tr>
            </thead>
            <tbody id="rows"></tbody>
          </table>
        </div>
      </div>
    """.strip()

    script = f"""
    <script>
      const rowsEl = document.getElementById('rows');
      const keyEl = document.getElementById('key');
      const initial = {json.dumps(key)};

      function esc(s) {{
        return String(s || '')
          .replaceAll('&','&amp;')
          .replaceAll('<','&lt;')
          .replaceAll('>','&gt;')
          .replaceAll('"','&quot;')
          .replaceAll(\"'\",'&#39;');
      }}

      async function load() {{
        const key = String(keyEl.value || '').trim();
        const qs = key ? ('?prompt_key=' + encodeURIComponent(key) + '&limit=500') : '?limit=500';
        rowsEl.innerHTML = '<tr><td colspan="5" class="muted">Loading…</td></tr>';
        try {{
          const res = await fetch('/prompts/log' + qs, {{ headers: {{ 'Content-Type': 'application/json' }} }});
          const body = await res.json().catch(() => ({{}}));
          if (!res.ok) throw new Error(body.detail || `HTTP ${{res.status}}`);
          const items = body.items || [];
          rowsEl.innerHTML = '';
          for (const r of items) {{
            const tr = document.createElement('tr');
            const date = String(r.edited_at || '').replace('T',' ').replace('Z','');
            tr.innerHTML = `
              <td><code>${{esc(date)}}</code></td>
              <td><a href="/prompts/ui?prompt_key=${{encodeURIComponent(r.prompt_key || '')}}"><code>${{esc(r.prompt_key)}}</code></a></td>
              <td>${{esc(r.user)}}</td>
              <td><span class="pill">${{esc(r.role)}}</span></td>
              <td class="muted">${{esc(r.note)}}</td>
            `;
            rowsEl.appendChild(tr);
          }}
          if (!items.length) {{
            rowsEl.innerHTML = '<tr><td colspan="5" class="muted">No changes yet.</td></tr>';
          }}
        }} catch (e) {{
          rowsEl.innerHTML = '<tr><td colspan="5" class="muted">Error: ' + esc(e.message || e) + '</td></tr>';
        }}
      }}

      document.getElementById('btnFilter').addEventListener('click', load);
      if (initial) load(); else load();
    </script>
    """.strip()

    return _ui_shell(title="ETI360 Prompt Log", active="apps", body_html=body_html, max_width_px=1400, user=user, extra_script=script)


@app.get("/db/schemas")
def db_schemas(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    """
    List non-system schemas. Useful for quickly confirming what namespaces exist in the DB.
    """
    _require_access(request=request, x_api_key=x_api_key, role="admin")

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('pg_catalog','information_schema')
                ORDER BY schema_name ASC;
                """
            )
            rows = cur.fetchall()

    return {"ok": True, "schemas": [str(r[0]) for r in rows]}


@app.get("/db/tables")
def db_tables(
    request: Request,
    schema: str = Query(default=WEATHER_SCHEMA, min_length=1),
    include_views: bool = Query(default=False),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    """
    List tables (and optionally views) for a schema.
    """
    _require_access(request=request, x_api_key=x_api_key, role="admin")
    schema = _require_safe_ident("schema", schema)

    types = ["BASE TABLE"]
    if include_views:
        types.append("VIEW")

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name, table_type
                FROM information_schema.tables
                WHERE table_schema=%s AND table_type = ANY(%s)
                ORDER BY table_name ASC;
                """,
                (schema, types),
            )
            rows = cur.fetchall()

    return {
        "ok": True,
        "schema": schema,
        "tables": [{"name": str(name), "type": str(ttype)} for (name, ttype) in rows],
    }


@app.get("/db/columns")
def db_columns(
    request: Request,
    schema: str = Query(default=WEATHER_SCHEMA, min_length=1),
    table: str = Query(..., min_length=1),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    """
    Describe a table/view: columns, types, nullability, and defaults.
    """
    _require_access(request=request, x_api_key=x_api_key, role="admin")
    schema = _require_safe_ident("schema", schema)
    table = _require_safe_ident("table", table)

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  column_name,
                  data_type,
                  udt_name,
                  is_nullable,
                  column_default,
                  ordinal_position
                FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s
                ORDER BY ordinal_position ASC;
                """,
                (schema, table),
            )
            rows = cur.fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail="No such table (or no columns visible)")

    cols: list[dict[str, Any]] = []
    for (name, data_type, udt_name, is_nullable, default, ordinal) in rows:
        cols.append(
            {
                "name": str(name),
                "data_type": str(data_type),
                "udt_name": str(udt_name),
                "nullable": str(is_nullable).upper() == "YES",
                "default": str(default) if default is not None else None,
                "ordinal_position": int(ordinal),
            }
        )

    return {"ok": True, "schema": schema, "table": table, "columns": cols}


@app.get("/db/ui", response_class=HTMLResponse)
def db_ui(
    request: Request,
    schema: str = Query(default=WEATHER_SCHEMA, min_length=1),
    table: str = Query(default="", min_length=0),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="admin")
    schema = _require_safe_ident("schema", schema)
    picked_table = ""
    if (table or "").strip():
        picked_table = _require_safe_ident("table", table)

    tables = db_tables(schema=schema, include_views=True, request=request, x_api_key=x_api_key)["tables"]
    cols: list[dict[str, Any]] = []
    if picked_table:
        cols = db_columns(schema=schema, table=picked_table, request=request, x_api_key=x_api_key)["columns"]

    def _esc(s: Any) -> str:
        return (
            str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )

    table_links = "\n".join(
        f'<li><a href="/db/ui?schema={_esc(schema)}&table={_esc(t["name"])}">{_esc(t["name"])}</a> <span class="muted">({ _esc(t["type"]) })</span></li>'
        for t in tables
    )

    cols_rows = ""
    if picked_table and cols:
        cols_rows = "\n".join(
            "<tr>"
            f"<td><code>{_esc(c['name'])}</code></td>"
            f"<td class=\"muted\">{_esc(c['data_type'])}</td>"
            f"<td class=\"muted\">{_esc(c['udt_name'])}</td>"
            f"<td>{'YES' if c['nullable'] else 'NO'}</td>"
            f"<td class=\"muted\">{_esc(c['default'] or '')}</td>"
            "</tr>"
            for c in cols
        )

    detail_html = ""
    if picked_table:
        detail_html = f"""
        <div class="card">
          <h1>DB Schema</h1>
          <p class="muted">Viewing <code>{_esc(schema)}.{_esc(picked_table)}</code></p>
        </div>
        <div class="card">
          <div class="muted">JSON: <a href="/db/columns?schema={_esc(schema)}&table={_esc(picked_table)}">/db/columns</a></div>
          <div class="section tablewrap">
            <table>
              <thead>
                <tr><th>Column</th><th>Type</th><th>Udt</th><th>Nullable</th><th>Default</th></tr>
              </thead>
              <tbody>
                {cols_rows}
              </tbody>
            </table>
          </div>
        </div>
        """
    else:
        detail_html = """
        <div class="card">
          <h1>DB Schema</h1>
          <p class="muted">Pick a table on the left to view its fields.</p>
        </div>
        """.strip()

    body_html = f"""
      <div class="grid-sidebar">
        <div class="card">
          <h2>Tables</h2>
          <div class="muted">Schema: <code>{_esc(schema)}</code></div>
          <div class="divider"></div>
          <ul style="margin: 8px 0 0 18px; padding: 0;">{table_links}</ul>
          <div class="divider"></div>
          <div class="muted">JSON: <a href="/db/tables?schema={_esc(schema)}&include_views=true">/db/tables</a> · <a href="/db/schemas">/db/schemas</a></div>
        </div>
        <div>
          {detail_html}
        </div>
      </div>
    """.strip()

    return _ui_shell(title="ETI360 DB Schema", active="db", body_html=body_html, max_width_px=1400, user=user)


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
def admin_schema_init(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_access(request=request, x_api_key=x_api_key, role="admin")

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
    request: Request,
    q: str = Query(..., min_length=1),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_access(request=request, x_api_key=x_api_key, role="editor")
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
    request: Request,
    q: str = Query(..., min_length=1),
    place_id: str = Query(default=""),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    """
    Convenience wrapper: run a Places search and return a single picked result.
    """
    _require_access(request=request, x_api_key=x_api_key, role="editor")
    results = (places_search(q=q, request=request, x_api_key=x_api_key) or {}).get("results") or []
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
                (payload.source.label, _domain_only(payload.source.url), payload.source.accessed_utc, payload.source.notes),
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
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_access(request=request, x_api_key=x_api_key, role="editor")
    return _save_weather_payload(body)


@app.post("/weather/import")
def import_weather_json(
    body: WeatherJsonIn,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_access(request=request, x_api_key=x_api_key, role="editor")

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
    domain = _domain_only(url)
    if domain:
        return f"Source: {domain}"
    label = (label or "").strip()
    return f"Source: {label}".strip() if label else "Source:"


class GenerateIn(BaseModel):
    location_slug: str = Field(..., min_length=1)
    year: int = 2026


@app.post("/weather/generate")
def generate_weather_png(
    body: GenerateIn,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_access(request=request, x_api_key=x_api_key, role="editor")
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
) -> tuple[dict[str, Any], dict[str, int], str, dict[str, int], str]:
    """
    Returns (result, perplexity_token_totals, perplexity_model, openai_token_totals, openai_model).
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

    # OpenAI chart titles/subtitles (optional; if OPENAI_API_KEY/prompt missing, this is a no-op).
    openai_prompt = 0
    openai_completion = 0
    openai_total = 0
    openai_model_used = ""

    weather_title = ""
    weather_subtitle = ""
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(_schema('SELECT id, city, country, lat, lng, timezone_id FROM "__SCHEMA__".locations WHERE location_slug=%s;'), (effective_slug,))
                loc_row = cur.fetchone()
                if loc_row:
                    _loc_id, city, country, lat, lng, _tzid = loc_row
                    display_name = str(city or "").strip() or effective_slug
                    ctry = str(country or "").strip()
                    if display_name and ctry:
                        display_name = f"{display_name}, {ctry}"

                    cur.execute(
                        _schema(
                            """
                            SELECT d.id, d.title, d.subtitle, s.label, s.url
                            FROM "__SCHEMA__".weather_datasets d
                            LEFT JOIN "__SCHEMA__".weather_sources s ON s.id = d.source_id
                            WHERE d.location_id=%s
                            ORDER BY d.updated_at DESC
                            LIMIT 1;
                            """
                        ),
                        (_loc_id,),
                    )
                    ds = cur.fetchone()
                    if ds:
                        dataset_id, _t, _st, _sl, _su = ds
                        cur.execute(
                            _schema(
                                'SELECT month, high_c, low_c, precip_cm FROM "__SCHEMA__".weather_monthly_normals WHERE dataset_id=%s ORDER BY month ASC;'
                            ),
                            (dataset_id,),
                        )
                        rows = cur.fetchall()
                        if len(rows) == 12:
                            monthly = [
                                MonthlyWeather(month=MONTHS[int(m) - 1], high_c=float(h), low_c=float(l), precip_cm=float(p))
                                for (m, h, l, p) in rows
                            ]
                            ws = _weather_summary(monthly=monthly)
                            wt, wst, tok, model = _maybe_openai_title_subtitle(
                                prompt_key="weather_titles_openai_v1", display_name=display_name, summary=ws
                            )
                            weather_title = wt
                            weather_subtitle = wst
                            openai_prompt += int(tok.get("prompt_tokens") or 0)
                            openai_completion += int(tok.get("completion_tokens") or 0)
                            openai_total += int(tok.get("total_tokens") or 0)
                            openai_model_used = model or openai_model_used
    except Exception:
        pass

    daylight_title = ""
    daylight_subtitle = ""
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    _schema('SELECT id, city, country, lat, lng, timezone_id FROM "__SCHEMA__".locations WHERE location_slug=%s;'),
                    (effective_slug,),
                )
                loc_row = cur.fetchone()
                if loc_row:
                    loc_id, city, country, lat, lng, tzid = loc_row
                    lat_f = float(lat)
                    lng_f = float(lng)
                    tzid_s = str(tzid or "").strip()
                    if not tzid_s:
                        tzid_s = _require_timezone_id(lat=lat_f, lng=lng_f)
                        cur.execute(
                            _schema('UPDATE "__SCHEMA__".locations SET timezone_id=%s, updated_at=now() WHERE id=%s;'),
                            (tzid_s, loc_id),
                        )
                        conn.commit()

                    display_name = str(city or "").strip() or effective_slug
                    ctry = str(country or "").strip()
                    if display_name and ctry:
                        display_name = f"{display_name}, {ctry}"

                    ds = compute_daylight_summary(
                        inputs=DaylightInputs(display_name=display_name, lat=lat_f, lng=lng_f, timezone_id=tzid_s),
                        year=year,
                    )
                    dt, dst, tok, model = _maybe_openai_title_subtitle(
                        prompt_key="daylight_titles_openai_v1", display_name=display_name, summary=ds
                    )
                    daylight_title = dt
                    daylight_subtitle = dst
                    openai_prompt += int(tok.get("prompt_tokens") or 0)
                    openai_completion += int(tok.get("completion_tokens") or 0)
                    openai_total += int(tok.get("total_tokens") or 0)
                    openai_model_used = model or openai_model_used
    except Exception:
        pass

    generated_weather = _generate_weather_png_for_slug(
        location_slug=effective_slug, year=year, title_override=weather_title, subtitle_override=weather_subtitle
    )
    generated_daylight = _generate_daylight_png_for_slug(
        location_slug=effective_slug, year=year, title_override=daylight_title, subtitle_override=daylight_subtitle
    )

    result = {
        "ok": True,
        "picked_place": picked_place,
        "location_query": location_query,
        "location_slug": effective_slug,
        "imported": imported,
        "year": year,
        "generated": {"weather": generated_weather, "daylight": generated_daylight},
    }
    openai_tokens = {"prompt_tokens": openai_prompt, "completion_tokens": openai_completion, "total_tokens": openai_total}
    return result, perplexity_tokens, perplexity_model, openai_tokens, openai_model_used


@app.post("/weather/auto")
def auto_weather(
    body: AutoWeatherIn,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_access(request=request, x_api_key=x_api_key, role="editor")
    batch = AutoBatchIn(locations=[body.location_query], force_refresh=body.force_refresh)
    return auto_weather_batch(batch, request=request, x_api_key=x_api_key)


@app.post("/weather/auto_batch")
def auto_weather_batch(
    body: AutoBatchIn,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_access(request=request, x_api_key=x_api_key, role="editor")

    locations = [str(x).strip() for x in (body.locations or []) if str(x).strip()]
    if not locations:
        raise HTTPException(status_code=400, detail="Provide at least one location")

    run_id = _create_run_id()
    results: list[dict[str, Any]] = []
    perplexity_prompt = 0
    perplexity_completion = 0
    perplexity_total = 0
    perplexity_model = ""
    openai_prompt = 0
    openai_completion = 0
    openai_total = 0
    openai_model = ""

    for q in locations:
        try:
            res, tok, model, otok, omodel = _auto_generate_one(location_query=q, force_refresh=body.force_refresh)
            results.append(res)
            perplexity_prompt += int(tok.get("prompt_tokens") or 0)
            perplexity_completion += int(tok.get("completion_tokens") or 0)
            perplexity_total += int(tok.get("total_tokens") or 0)
            perplexity_model = model or perplexity_model
            openai_prompt += int(otok.get("prompt_tokens") or 0)
            openai_completion += int(otok.get("completion_tokens") or 0)
            openai_total += int(otok.get("total_tokens") or 0)
            openai_model = omodel or openai_model
        except Exception as e:
            results.append({"ok": False, "location_query": q, "error": str(getattr(e, "detail", e))})

    ok_count = sum(1 for r in results if r.get("ok"))
    fail_count = sum(1 for r in results if not r.get("ok"))
    workflow = "weather+sunlight"

    usage_rows: list[dict[str, Any]] = []
    usage_rows.append(
        _record_llm_usage(
            run_id=run_id,
            workflow=workflow,
            kind="auto_batch",
            provider="perplexity",
            model=perplexity_model or os.environ.get("PERPLEXITY_MODEL", "").strip() or "unused",
            prompt_tokens=perplexity_prompt,
            completion_tokens=perplexity_completion,
            total_tokens=perplexity_total,
            locations_count=len(locations),
            ok_count=ok_count,
            fail_count=fail_count,
        )
    )

    usage_rows.append(
        _record_llm_usage(
            run_id=run_id,
            workflow=workflow,
            kind="auto_batch",
            provider="openai",
            model=openai_model or os.environ.get("OPENAI_MODEL", "").strip() or "gpt-5-mini",
            prompt_tokens=openai_prompt,
            completion_tokens=openai_completion,
            total_tokens=openai_total,
            locations_count=len(locations),
            ok_count=ok_count,
            fail_count=fail_count,
        )
    )

    run_cost_usd = float(sum(r.get("cost_usd", 0.0) for r in usage_rows))

    # Cumulative total cost (all runs).
    cumulative_total = 0.0
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_usage_tables(cur)
            cur.execute(_usage_schema('SELECT COALESCE(SUM(cost_usd),0) FROM "__SCHEMA__".llm_usage;'))
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
def weather_ui(request: Request) -> str:
    user = _get_current_user(request)
    body_html = """
      <div class="card">
        <h1>Weather + Sunlight</h1>
        <p class="muted">Paste one city per line, then run. Cities and chart links are listed alphabetically.</p>
      </div>

      <div class="section grid-2">
        <div class="card">
          <label>Location input (one per line)</label>
          <textarea id="citiesInput" class="mono" placeholder="Lima, Peru&#10;Nagasaki, Japan"></textarea>
          <div class="btnrow">
            <button id="btnRun" class="btn primary" type="button">Run</button>
            <button id="btnClear" class="btn" type="button">Clear</button>
          </div>
          <div id="status" class="statusbox mono">Ready.</div>
        </div>

        <div class="card">
          <div style="display:flex; justify-content:space-between; gap:10px; align-items:baseline;">
            <h2>Cities and Links</h2>
            <span class="pill">Alphabetical</span>
          </div>
          <div class="section tablewrap">
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
      </div>
    """.strip()

    script = """
    <script>
      const citiesEl = document.getElementById('citiesInput');
      const statusEl = document.getElementById('status');
      const locRowsEl = document.getElementById('locRows');
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
    </script>
    """.strip()

    return _ui_shell(
        title="ETI360 Weather",
        active="weather",
        body_html=body_html,
        max_width_px=1400,
        extra_script=script,
        user=user,
    )
