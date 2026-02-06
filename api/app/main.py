from __future__ import annotations

import json
import os
import re
import tempfile
import uuid
from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import datetime, timedelta, timezone
from hashlib import pbkdf2_hmac, sha256
from pathlib import Path
from secrets import token_bytes
from typing import Any
from urllib.parse import urlencode, urlparse, quote
from urllib.request import urlopen

import bleach
import markdown as mdlib
import psycopg
from fastapi import FastAPI, File, Form, Header, HTTPException, Query, Request, Response, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel, Field

from app.weather.perplexity import fetch_monthly_weather_normals
from app.weather.daylight_chart import DaylightInputs, compute_daylight_summary, render_daylight_chart
from app.weather.llm_usage import estimate_cost_usd
from app.weather.openai_chat import OpenAIResult, chat_text
from app.weather.s3 import get_bytes, get_s3_config, presign_get, presign_get_inline, put_bytes, put_png
from app.weather.weather_chart import MONTHS, MonthlyWeather, render_weather_chart

app = FastAPI(title="ETI360 Internal API", docs_url="/docs", redoc_url=None)

WEATHER_SCHEMA = "weather"
USAGE_SCHEMA = os.environ.get("USAGE_SCHEMA", "ops").strip() or "ops"
AUTH_SCHEMA = os.environ.get("AUTH_SCHEMA", "ops").strip() or "ops"
DOCS_SCHEMA = os.environ.get("DOCS_SCHEMA", "ops").strip() or "ops"
DIRECTORY_SCHEMA = os.environ.get("DIRECTORY_SCHEMA", "directory").strip() or "directory"
SESSION_COOKIE_NAME = os.environ.get("SESSION_COOKIE_NAME", "eti360_session").strip() or "eti360_session"


@app.on_event("startup")
def _startup_reconcile() -> None:
    """
    Best-effort reconciliation so required prompts/usage tables exist without manual UI actions.
    """
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                _ensure_usage_tables(cur)
                _ensure_prompts_tables(cur)
                _ensure_documents_tables(cur)
            conn.commit()
        _reconcile_required_prompts(edited_by={"id": "startup", "username": "startup", "role": "admin"}, change_note="Startup reconcile")
    except Exception as e:
        # Don't block the service from starting; surface via logs.
        print(f"[startup] reconcile skipped: {e}")


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


def _docs_schema(sql: str) -> str:
    schema = _require_safe_ident("DOCS_SCHEMA", DOCS_SCHEMA)
    return sql.replace("__SCHEMA__", schema)


def _auth_schema(sql: str) -> str:
    schema = _require_safe_ident("AUTH_SCHEMA", AUTH_SCHEMA)
    return sql.replace("__SCHEMA__", schema)


def _directory_schema(sql: str) -> str:
    schema = _require_safe_ident("DIRECTORY_SCHEMA", DIRECTORY_SCHEMA)
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
              prompt_key TEXT NOT NULL DEFAULT '',
              app_key TEXT NOT NULL DEFAULT '',
              workflow TEXT NOT NULL DEFAULT '',
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
    # For older deployments where llm_usage existed without these columns.
    cur.execute(_usage_schema('ALTER TABLE "__SCHEMA__".llm_usage ADD COLUMN IF NOT EXISTS prompt_key TEXT NOT NULL DEFAULT \'\';'))
    cur.execute(_usage_schema('ALTER TABLE "__SCHEMA__".llm_usage ADD COLUMN IF NOT EXISTS app_key TEXT NOT NULL DEFAULT \'\';'))
    cur.execute(_usage_schema('ALTER TABLE "__SCHEMA__".llm_usage ADD COLUMN IF NOT EXISTS workflow TEXT NOT NULL DEFAULT \'\';'))
    cur.execute(_usage_schema('CREATE INDEX IF NOT EXISTS llm_usage_run_id_idx ON "__SCHEMA__".llm_usage(run_id);'))
    cur.execute(_usage_schema('CREATE INDEX IF NOT EXISTS llm_usage_created_at_idx ON "__SCHEMA__".llm_usage(created_at DESC);'))
    cur.execute(_usage_schema('CREATE INDEX IF NOT EXISTS llm_usage_prompt_key_idx ON "__SCHEMA__".llm_usage(prompt_key, created_at DESC);'))
    cur.execute(_usage_schema('CREATE INDEX IF NOT EXISTS llm_usage_app_workflow_prompt_key_idx ON "__SCHEMA__".llm_usage(app_key, workflow, prompt_key, created_at DESC);'))

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


_DOCS_FOLDER_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9/_\\-\\.]{0,255}$")


def _require_docs_folder(folder: str) -> str:
    folder = (folder or "").strip().strip("/")
    if not folder:
        return ""
    if not _DOCS_FOLDER_RE.match(folder):
        raise HTTPException(status_code=400, detail="Invalid folder (use letters/numbers, /, -, _, .)")
    return folder


def _docs_max_upload_bytes() -> int:
    raw = os.environ.get("DOCS_MAX_UPLOAD_BYTES", "").strip()
    try:
        v = int(raw) if raw else 10 * 1024 * 1024
    except Exception:
        v = 10 * 1024 * 1024
    return max(64 * 1024, min(v, 50 * 1024 * 1024))


def _docs_s3_prefix() -> str:
    """
    Prefix within S3_PREFIX for document storage.

    Example: if S3_PREFIX is blank and DOCS_S3_PREFIX=documents/, keys are:
      documents/<folder>/<doc_id>/<filename>
    """
    raw = os.environ.get("DOCS_S3_PREFIX", "documents/").strip()
    if not raw:
        raw = "documents/"
    if not raw.endswith("/"):
        raw += "/"
    if raw.startswith("/"):
        raw = raw.lstrip("/")
    return raw


def _docs_max_preview_bytes() -> int:
    raw = os.environ.get("DOCS_MAX_PREVIEW_BYTES", "").strip()
    try:
        v = int(raw) if raw else 1024 * 1024
    except Exception:
        v = 1024 * 1024
    return max(32 * 1024, min(v, 5 * 1024 * 1024))


def _require_docs_status(status: str) -> str:
    s = (status or "").strip().lower()
    if not s:
        return "future"
    if s not in {"future", "in_progress", "finished"}:
        raise HTTPException(status_code=400, detail="Invalid status (future, in_progress, finished)")
    return s


_MD_EXTS = {".md", ".markdown", ".mdown", ".mkd"}


def _is_markdown(*, filename: str, content_type: str) -> bool:
    fn = (filename or "").strip().lower()
    ct = (content_type or "").strip().lower()
    if ct in {"text/markdown", "text/x-markdown"}:
        return True
    for ext in _MD_EXTS:
        if fn.endswith(ext):
            return True
    return False


_MD_ALLOWED_TAGS = [
    "p",
    "br",
    "hr",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "ul",
    "ol",
    "li",
    "strong",
    "em",
    "code",
    "pre",
    "blockquote",
    "a",
]
_MD_ALLOWED_ATTRS = {"a": ["href", "title", "rel", "target"]}


def _render_markdown_safe(md_text: str) -> str:
    html = mdlib.markdown(
        md_text or "",
        extensions=["fenced_code", "sane_lists"],
        output_format="html",
    )
    cleaned = bleach.clean(
        html,
        tags=_MD_ALLOWED_TAGS,
        attributes=_MD_ALLOWED_ATTRS,
        protocols=["http", "https", "mailto"],
        strip=True,
    )
    cleaned = bleach.linkify(cleaned, callbacks=[bleach.callbacks.nofollow, bleach.callbacks.target_blank])
    return cleaned


def _require_docs_app_key(app_key: str) -> str:
    app_key = (app_key or "").strip()
    if not app_key:
        raise HTTPException(status_code=400, detail="app_key is required")
    return _slugify(app_key)


def _normalize_group_key(group_key: str) -> str:
    group_key = (group_key or "").strip().strip("/")
    if not group_key:
        return ""
    parts = [p for p in group_key.split("/") if p.strip()]
    return "/".join(_slugify(p) for p in parts)


def _safe_s3_filename(filename: str) -> str:
    # Avoid path traversal and keep keys readable.
    filename = (filename or "").strip().replace("\\", "/")
    filename = filename.split("/")[-1]
    filename = re.sub(r"\s+", " ", filename).strip()
    filename = filename.replace(" ", "_")
    filename = re.sub(r"[^A-Za-z0-9._-]", "", filename)
    return filename or "document"


def _docs_folder_for(*, app_key: str, group_key: str) -> str:
    ak = _require_docs_app_key(app_key)
    gk = _normalize_group_key(group_key)
    return f"{ak}/{gk}" if gk else ak


def _ensure_documents_tables(cur: psycopg.Cursor) -> None:
    """
    Store uploaded documents in Postgres (bytea) + metadata in ops schema.
    """
    cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    cur.execute(_docs_schema('CREATE SCHEMA IF NOT EXISTS "__SCHEMA__";'))
    cur.execute(
        _docs_schema(
            """
            CREATE TABLE IF NOT EXISTS "__SCHEMA__".documents (
              id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
              folder TEXT NOT NULL DEFAULT '',
              filename TEXT NOT NULL,
              content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
              bytes BIGINT NOT NULL DEFAULT 0,
              sha256 TEXT NOT NULL DEFAULT '',
              status TEXT NOT NULL DEFAULT 'future',
              notes TEXT NOT NULL DEFAULT '',
              uploaded_by_user_id UUID,
              uploaded_by_username TEXT NOT NULL DEFAULT '',
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              content BYTEA NOT NULL
            );
            """
        ).strip()
    )
    cur.execute(_docs_schema('ALTER TABLE "__SCHEMA__".documents ADD COLUMN IF NOT EXISTS folder TEXT NOT NULL DEFAULT \'\';'))
    cur.execute(_docs_schema('ALTER TABLE "__SCHEMA__".documents ADD COLUMN IF NOT EXISTS app_key TEXT NOT NULL DEFAULT \'\';'))
    cur.execute(_docs_schema('ALTER TABLE "__SCHEMA__".documents ADD COLUMN IF NOT EXISTS group_key TEXT NOT NULL DEFAULT \'\';'))
    cur.execute(_docs_schema('ALTER TABLE "__SCHEMA__".documents ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT \'future\';'))
    cur.execute(_docs_schema('ALTER TABLE "__SCHEMA__".documents ADD COLUMN IF NOT EXISTS notes TEXT NOT NULL DEFAULT \'\';'))
    cur.execute(_docs_schema('ALTER TABLE "__SCHEMA__".documents ADD COLUMN IF NOT EXISTS uploaded_by_user_id UUID;'))
    cur.execute(_docs_schema('ALTER TABLE "__SCHEMA__".documents ADD COLUMN IF NOT EXISTS uploaded_by_username TEXT NOT NULL DEFAULT \'\';'))
    cur.execute(_docs_schema('ALTER TABLE "__SCHEMA__".documents ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();'))
    cur.execute(_docs_schema('ALTER TABLE "__SCHEMA__".documents ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();'))
    cur.execute(_docs_schema('ALTER TABLE "__SCHEMA__".documents ADD COLUMN IF NOT EXISTS storage TEXT NOT NULL DEFAULT \'db\';'))
    cur.execute(_docs_schema('ALTER TABLE "__SCHEMA__".documents ADD COLUMN IF NOT EXISTS s3_bucket TEXT NOT NULL DEFAULT \'\';'))
    cur.execute(_docs_schema('ALTER TABLE "__SCHEMA__".documents ADD COLUMN IF NOT EXISTS s3_key TEXT NOT NULL DEFAULT \'\';'))
    cur.execute(_docs_schema('CREATE UNIQUE INDEX IF NOT EXISTS documents_folder_filename_uniq_idx ON "__SCHEMA__".documents(folder, filename);'))
    cur.execute(_docs_schema('CREATE INDEX IF NOT EXISTS documents_updated_at_idx ON "__SCHEMA__".documents(updated_at DESC);'))
    cur.execute(_docs_schema('CREATE INDEX IF NOT EXISTS documents_status_idx ON "__SCHEMA__".documents(status, updated_at DESC);'))
    cur.execute(_docs_schema('CREATE INDEX IF NOT EXISTS documents_app_group_idx ON "__SCHEMA__".documents(app_key, group_key, updated_at DESC);'))


_DIRECTORY_SCHEMA_STATEMENTS: list[str] = [
    "CREATE EXTENSION IF NOT EXISTS pgcrypto;",
    'CREATE SCHEMA IF NOT EXISTS "__SCHEMA__";',
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
    """.strip(),
    'CREATE INDEX IF NOT EXISTS providers_name_idx ON "__SCHEMA__".providers(provider_name);',
    'CREATE INDEX IF NOT EXISTS providers_status_idx ON "__SCHEMA__".providers(status);',
    """
    CREATE TABLE IF NOT EXISTS "__SCHEMA__".provider_classifications (
      provider_id UUID PRIMARY KEY REFERENCES "__SCHEMA__".providers(id) ON DELETE CASCADE,
      market_orientation TEXT NOT NULL DEFAULT '',
      client_profile_indicators TEXT NOT NULL DEFAULT '',
      educational_market_orientation TEXT NOT NULL DEFAULT '',
      commercial_posture_signal TEXT NOT NULL DEFAULT '',
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """.strip(),
    'CREATE INDEX IF NOT EXISTS provider_classifications_market_idx ON "__SCHEMA__".provider_classifications(market_orientation);',
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
    """.strip(),
    'CREATE INDEX IF NOT EXISTS provider_social_provider_idx ON "__SCHEMA__".provider_social_links(provider_id);',
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
    """.strip(),
    'CREATE INDEX IF NOT EXISTS provider_analysis_provider_idx ON "__SCHEMA__".provider_analysis_runs(provider_id);',
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
    """.strip(),
    'CREATE INDEX IF NOT EXISTS provider_evidence_provider_idx ON "__SCHEMA__".provider_evidence(provider_id);',
    """
    CREATE TABLE IF NOT EXISTS "__SCHEMA__".provider_country (
      provider_key TEXT NOT NULL,
      country_or_territory TEXT NOT NULL,
      source TEXT NOT NULL DEFAULT '',
      generated_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      CONSTRAINT provider_country_provider_country_uniq UNIQUE (provider_key, country_or_territory)
    );
    """.strip(),
    'CREATE INDEX IF NOT EXISTS provider_country_provider_key_idx ON "__SCHEMA__".provider_country(provider_key);',
    'CREATE INDEX IF NOT EXISTS provider_country_country_idx ON "__SCHEMA__".provider_country(country_or_territory);',
]


def _ensure_directory_tables(cur: psycopg.Cursor) -> None:
    for stmt in _DIRECTORY_SCHEMA_STATEMENTS:
        cur.execute(_directory_schema(stmt))


def _ensure_prompts_tables(cur: psycopg.Cursor) -> None:
    cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    cur.execute(_prompts_schema('CREATE SCHEMA IF NOT EXISTS "__SCHEMA__";'))

    cur.execute(
        _prompts_schema(
            """
            CREATE TABLE IF NOT EXISTS "__SCHEMA__".prompts (
              id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
              prompt_key TEXT NOT NULL UNIQUE,
              app_key TEXT NOT NULL DEFAULT '',
              workflow TEXT NOT NULL DEFAULT '',
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
    cur.execute(_prompts_schema('ALTER TABLE "__SCHEMA__".prompts ADD COLUMN IF NOT EXISTS app_key TEXT NOT NULL DEFAULT \'\';'))
    cur.execute(_prompts_schema('ALTER TABLE "__SCHEMA__".prompts ADD COLUMN IF NOT EXISTS workflow TEXT NOT NULL DEFAULT \'\';'))
    cur.execute(_prompts_schema('CREATE INDEX IF NOT EXISTS prompts_updated_at_idx ON "__SCHEMA__".prompts(updated_at DESC);'))
    cur.execute(_prompts_schema('CREATE INDEX IF NOT EXISTS prompts_app_workflow_idx ON "__SCHEMA__".prompts(app_key, workflow, prompt_key);'))

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
              before_app_key TEXT NOT NULL DEFAULT '',
              after_app_key TEXT NOT NULL DEFAULT '',
              before_workflow TEXT NOT NULL DEFAULT '',
              after_workflow TEXT NOT NULL DEFAULT '',
              before_provider TEXT NOT NULL DEFAULT '',
              after_provider TEXT NOT NULL DEFAULT '',
              before_model TEXT NOT NULL DEFAULT '',
              after_model TEXT NOT NULL DEFAULT ''
            );
            """
        ).strip()
    )
    cur.execute(_prompts_schema('ALTER TABLE "__SCHEMA__".prompt_revisions ADD COLUMN IF NOT EXISTS before_app_key TEXT NOT NULL DEFAULT \'\';'))
    cur.execute(_prompts_schema('ALTER TABLE "__SCHEMA__".prompt_revisions ADD COLUMN IF NOT EXISTS after_app_key TEXT NOT NULL DEFAULT \'\';'))
    cur.execute(_prompts_schema('ALTER TABLE "__SCHEMA__".prompt_revisions ADD COLUMN IF NOT EXISTS before_workflow TEXT NOT NULL DEFAULT \'\';'))
    cur.execute(_prompts_schema('ALTER TABLE "__SCHEMA__".prompt_revisions ADD COLUMN IF NOT EXISTS after_workflow TEXT NOT NULL DEFAULT \'\';'))
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


def _reconcile_required_prompts(*, edited_by: dict[str, Any] | None, change_note: str) -> dict[str, list[str]]:
    """
    Ensure required prompts exist and have required defaults (app_key/workflow) populated.

    - Does NOT overwrite prompt_text for existing prompts.
    - May migrate deprecated OpenAI models to the current default.
    """
    created: list[str] = []
    updated: list[str] = []

    actor = edited_by or {}
    username = str(actor.get("username") or "")
    role = str(actor.get("role") or "")
    user_id_uuid = None
    try:
        if str(actor.get("id") or "").strip() and str(actor.get("id")) not in {"disabled", "api_key"}:
            user_id_uuid = uuid.UUID(str(actor.get("id")))
    except Exception:
        user_id_uuid = None

    reqs = _required_prompts()

    deprecated_openai_models = {
        "gpt-4o-mini",
        "gpt-4o",
        "gpt-4.1",
        "gpt-4.1-mini",
        "gpt-4.1-nano",
        "o4-mini",
    }

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_prompts_tables(cur)
            for r in reqs:
                key = _require_prompt_key(str(r.get("prompt_key") or ""))
                desired_app_key = str(r.get("app_key") or "").strip()
                desired_workflow = str(r.get("workflow") or "").strip()
                desired_provider = str(r.get("provider") or "").strip()
                desired_model = str(r.get("model") or "").strip()

                cur.execute(
                    _prompts_schema(
                        """
                        SELECT id, app_key, workflow, provider, model
                        FROM "__SCHEMA__".prompts
                        WHERE prompt_key=%s
                        LIMIT 1;
                        """
                    ),
                    (key,),
                )
                row = cur.fetchone()
                if not row:
                    cur.execute(
                        _prompts_schema(
                            """
                            INSERT INTO "__SCHEMA__".prompts
                              (prompt_key, app_key, workflow, name, natural_name, description, provider, model, prompt_text)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            RETURNING id;
                            """
                        ),
                        (
                            key,
                            desired_app_key,
                            desired_workflow,
                            str(r.get("name") or ""),
                            str(r.get("natural_name") or ""),
                            str(r.get("description") or ""),
                            desired_provider,
                            desired_model,
                            str(r.get("prompt_text") or ""),
                        ),
                    )
                    (pid,) = cur.fetchone()  # type: ignore[misc]
                    cur.execute(
                        _prompts_schema(
                            """
                            INSERT INTO "__SCHEMA__".prompt_revisions
                              (prompt_id, prompt_key, edited_by_user_id, edited_by_username, edited_by_role, change_note,
                               before_text, after_text,
                               before_app_key, after_app_key, before_workflow, after_workflow,
                               before_provider, after_provider, before_model, after_model)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                            """
                        ),
                        (
                            pid,
                            key,
                            user_id_uuid,
                            username,
                            role,
                            change_note,
                            "",
                            str(r.get("prompt_text") or ""),
                            "",
                            desired_app_key,
                            "",
                            desired_workflow,
                            "",
                            desired_provider,
                            "",
                            desired_model,
                        ),
                    )
                    created.append(key)
                    continue

                (pid, before_app_key, before_workflow, before_provider, before_model) = row
                before_app_key_s = str(before_app_key or "").strip()
                before_workflow_s = str(before_workflow or "").strip()
                before_provider_s = str(before_provider or "").strip()
                before_model_s = str(before_model or "").strip()

                new_app_key = before_app_key_s or desired_app_key
                new_workflow = before_workflow_s or desired_workflow
                new_model = before_model_s

                if before_provider_s.lower() == "openai":
                    if not new_model or new_model in deprecated_openai_models:
                        new_model = os.environ.get("OPENAI_MODEL", "").strip() or "gpt-5-mini"

                if new_app_key != before_app_key_s or new_workflow != before_workflow_s or new_model != before_model_s:
                    cur.execute(
                        _prompts_schema(
                            """
                            UPDATE "__SCHEMA__".prompts
                            SET app_key=%s, workflow=%s, model=%s, updated_at=NOW()
                            WHERE id=%s;
                            """
                        ),
                        (new_app_key, new_workflow, new_model, pid),
                    )
                    cur.execute(
                        _prompts_schema(
                            """
                            INSERT INTO "__SCHEMA__".prompt_revisions
                              (prompt_id, prompt_key, edited_by_user_id, edited_by_username, edited_by_role, change_note,
                               before_text, after_text,
                               before_app_key, after_app_key, before_workflow, after_workflow,
                               before_provider, after_provider, before_model, after_model)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                            """
                        ),
                        (
                            pid,
                            key,
                            user_id_uuid,
                            username,
                            role,
                            change_note,
                            "",
                            "",
                            before_app_key_s,
                            new_app_key,
                            before_workflow_s,
                            new_workflow,
                            before_provider_s,
                            before_provider_s,
                            before_model_s,
                            new_model,
                        ),
                    )
                    updated.append(key)

        conn.commit()

    return {"created": created, "updated": updated}


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
        ("Trip Providers", "/trip_providers_research", "trip_providers"),
        ("Countries", "/trip_providers/countries", "trip_providers_countries"),
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
    prompt_key: str,
    app_key: str,
    prompt_workflow: str,
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
    prompt_key_s = _require_prompt_key(prompt_key) if (prompt_key or "").strip() else ""
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
                    INSERT INTO "__SCHEMA__".llm_usage (run_id, prompt_key, app_key, workflow, provider, model, prompt_tokens, completion_tokens, total_tokens, cost_usd)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                    """
                ),
                (
                    run_uuid,
                    prompt_key_s,
                    (app_key or "").strip(),
                    (prompt_workflow or "").strip(),
                    provider,
                    model,
                    int(prompt_tokens),
                    int(completion_tokens),
                    int(total_tokens),
                    cost_usd,
                ),
            )
        conn.commit()

    return {
        "prompt_key": prompt_key_s,
        "app_key": (app_key or "").strip(),
        "workflow": (prompt_workflow or "").strip(),
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
                      u.prompt_key,
                      u.app_key,
                      u.workflow,
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
            "prompt_key": str(prompt_key or ""),
            "app_key": str(app_key or ""),
            "prompt_workflow": str(pworkflow or ""),
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
            prompt_key,
            app_key,
            pworkflow,
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
                <th>Prompt</th>
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
              <td><code>${safe(r.prompt_key || '')}</code></td>
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
            rowsEl.innerHTML = '<tr><td colspan="10" class="muted">No usage yet.</td></tr>';
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
                <td>Trip Providers (Research)</td>
                <td class="muted">Search and review educational trip provider profiles.</td>
                <td><a href="/trip_providers_research">Open</a></td>
              </tr>
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
              <tr>
                <td>Documents</td>
                <td class="muted">Upload/download project notes (stored in Postgres).</td>
                <td><a href="/documents/ui">Open</a></td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    """.strip()

    return _ui_shell(title="ETI360 Apps", active="apps", body_html=body_html, max_width_px=1100, user=user)


def _safe_provider_key(provider_key: str) -> str:
    provider_key = (provider_key or "").strip()
    if not provider_key:
        raise HTTPException(status_code=400, detail="Missing provider_key")
    if "/" in provider_key or "\\" in provider_key:
        raise HTTPException(status_code=400, detail="Invalid provider_key")
    if len(provider_key) > 200:
        raise HTTPException(status_code=400, detail="provider_key too long")
    return provider_key


_COUNTRY_ALIASES: dict[str, str] = {
    "us": "United States",
    "usa": "United States",
    "u.s.": "United States",
    "u.s.a.": "United States",
    "united states of america": "United States",
    "uk": "United Kingdom",
    "u.k.": "United Kingdom",
    "uae": "United Arab Emirates",
    "u.a.e.": "United Arab Emirates",
}


def _normalize_country_query(q: str) -> tuple[str, str]:
    """
    Returns (raw_query, alias_canonical) where alias_canonical may be "".
    """
    raw = (q or "").strip()
    k = raw.lower().replace(",", "").strip()
    alias = _COUNTRY_ALIASES.get(k, "")
    return raw, alias


def _render_social_links_html(social_links: Any) -> str:
    if not isinstance(social_links, dict):
        return ""

    def _esc(s: Any) -> str:
        return (
            str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )

    labels = {
        "linkedin": "LinkedIn",
        "facebook": "Facebook",
        "instagram": "Instagram",
        "twitter": "X",
        "youtube": "YouTube",
        "tiktok": "TikTok",
        "other": "Other",
    }
    parts: list[str] = []
    for kind in ["linkedin", "facebook", "instagram", "twitter", "youtube", "tiktok", "other"]:
        url = str(social_links.get(kind) or "").strip()
        if not url:
            continue
        parts.append(f'<a href="{_esc(url)}" target="_blank" rel="noopener">{_esc(labels.get(kind, kind))}</a>')
    return " · ".join(parts)


@app.get("/trip_providers/countries", response_class=HTMLResponse)
def trip_providers_countries_index_ui(
    request: Request,
    q: str = Query(default=""),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="viewer") or {}
    q_raw, q_alias = _normalize_country_query(q)

    def _esc(s: Any) -> str:
        return (
            str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )

    where_sql = ""
    params: list[Any] = ["Education-focused"]
    if q_raw:
        where_sql = "AND (pc.country_or_territory ILIKE %s OR pc.country_or_territory = %s)"
        params.extend([f"%{q_raw}%", q_alias or q_raw])

    sql = _directory_schema(
        f"""
        SELECT
          pc.country_or_territory,
          COUNT(DISTINCT pc.provider_key) AS provider_count
        FROM "__SCHEMA__".provider_country pc
        JOIN "__SCHEMA__".providers p ON p.provider_key = pc.provider_key
        LEFT JOIN "__SCHEMA__".provider_classifications c ON c.provider_id = p.id
        WHERE p.status='active'
          AND NULLIF(TRIM(p.website_url), '') IS NOT NULL
          AND c.market_orientation = %s
          {where_sql}
        GROUP BY pc.country_or_territory
        ORDER BY pc.country_or_territory ASC;
        """
    ).strip()

    rows: list[tuple[Any, ...]] = []
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_directory_tables(cur)
            cur.execute(sql, params)
            rows = list(cur.fetchall())

    lis: list[str] = []
    for country, provider_count in rows:
        c = str(country or "").strip()
        if not c:
            continue
        slug = _slugify(c)
        href = f"/trip_providers/countries/{quote(slug)}"
        lis.append(f'<li><a href="{href}">{_esc(c)}</a> <span class="muted">({int(provider_count or 0)})</span></li>')

    list_html = "<ul style=\"margin: 8px 0 0 18px; padding: 0;\">" + "".join(lis) + "</ul>" if lis else "<p class=\"muted\">No countries found.</p>"

    body_html = f"""
      <div class="card">
        <h1>Trip Providers — Countries</h1>
        <p class="muted">Browse countries/territories and see education-focused providers operating there.</p>
      </div>

      <div class="card">
        <h2>Search</h2>
        <form method="get" action="/trip_providers/countries">
          <div class="grid-2">
            <div>
              <label>Country / territory</label>
              <input type="text" name="q" value="{_esc(q_raw)}" placeholder="e.g., Japan, USA, UK" />
            </div>
            <div>
              <label>&nbsp;</label>
              <div style="margin-top:2px;">
                <button class="btn primary" type="submit">Search</button>
                <a class="btn" href="/trip_providers/countries">Reset</a>
              </div>
            </div>
          </div>
        </form>
      </div>

      <div class="card">
        <h2>Countries</h2>
        <div class="muted">Showing {len(lis)} result(s).</div>
        <div class="divider"></div>
        <div class="section">
          {list_html}
        </div>
      </div>
    """.strip()

    return _ui_shell(title="Trip Providers — Countries", active="trip_providers_countries", body_html=body_html, max_width_px=1100, user=user)


@app.get("/trip_providers/countries/{country_slug}", response_class=HTMLResponse)
def trip_providers_country_detail_ui(
    request: Request,
    country_slug: str,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="viewer") or {}
    slug = (country_slug or "").strip().lower()
    if not slug:
        raise HTTPException(status_code=400, detail="Missing country")

    def _esc(s: Any) -> str:
        return (
            str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )

    # Resolve slug -> canonical country label from DB.
    sql_countries = _directory_schema(
        """
        SELECT DISTINCT pc.country_or_territory
        FROM "__SCHEMA__".provider_country pc
        ORDER BY pc.country_or_territory ASC;
        """
    ).strip()
    countries: list[str] = []
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_directory_tables(cur)
            cur.execute(sql_countries)
            countries = [str(r[0] or "").strip() for r in cur.fetchall() if str(r[0] or "").strip()]

    canonical = ""
    for c in countries:
        if _slugify(c) == slug:
            canonical = c
            break
    if not canonical:
        raise HTTPException(status_code=404, detail="Country not found")

    sql = _directory_schema(
        """
        SELECT
          p.provider_key,
          p.provider_name,
          p.website_url,
          c.market_orientation,
          c.client_profile_indicators,
          (
            SELECT jsonb_object_agg(sl.kind, sl.url)
            FROM "__SCHEMA__".provider_social_links sl
            WHERE sl.provider_id = p.id
          ) AS social_links
        FROM "__SCHEMA__".provider_country pc
        JOIN "__SCHEMA__".providers p ON p.provider_key = pc.provider_key
        LEFT JOIN "__SCHEMA__".provider_classifications c ON c.provider_id = p.id
        WHERE pc.country_or_territory = %s
          AND p.status = 'active'
          AND NULLIF(TRIM(p.website_url), '') IS NOT NULL
          AND c.market_orientation = %s
        ORDER BY p.provider_name ASC;
        """
    ).strip()

    rows: list[tuple[Any, ...]] = []
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_directory_tables(cur)
            cur.execute(sql, (canonical, "Education-focused"))
            rows = list(cur.fetchall())

    tr_rows: list[str] = []
    for provider_key, provider_name, website_url, _market_orientation, client_profile, social_links in rows:
        key = str(provider_key or "").strip()
        name = str(provider_name or "").strip() or key
        website = str(website_url or "").strip()
        website_html = f'<a href="{_esc(website)}" target="_blank" rel="noopener">Website</a>' if website else ""
        client = str(client_profile or "").strip()
        social_html = _render_social_links_html(social_links) or ""
        tr_rows.append(
            f"""
            <tr>
              <td><a href="/trip_providers_research/{_esc(quote(key))}">{_esc(name)}</a><div class="muted" style="margin-top:4px;"><code>{_esc(key)}</code></div></td>
              <td class="muted">{_esc(client)}</td>
              <td>{website_html}</td>
              <td class="muted">{social_html}</td>
            </tr>
            """.strip()
        )

    tbody_html = "".join(tr_rows) if tr_rows else '<tr><td colspan="4" class="muted">No providers found.</td></tr>'

    body_html = f"""
      <div class="card">
        <div class="muted"><a href="/trip_providers/countries">← Back to countries</a></div>
        <h1>{_esc(canonical)}</h1>
        <div class="muted">{len(tr_rows)} provider(s)</div>
      </div>

      <div class="card">
        <h2>Providers</h2>
        <div class="section tablewrap">
          <table>
            <thead>
              <tr>
                <th>Provider</th>
                <th>Client profile</th>
                <th>Links</th>
                <th>Social</th>
              </tr>
            </thead>
            <tbody>
              {tbody_html}
            </tbody>
          </table>
        </div>
      </div>
    """.strip()

    return _ui_shell(title=f"Trip Providers — {canonical}", active="trip_providers_countries", body_html=body_html, max_width_px=1100, user=user)


@app.get("/trip_providers_research", response_class=HTMLResponse)
def trip_providers_research_ui(
    request: Request,
    q: str = Query(default=""),
    scope: str = Query(default="educational"),
    include_excluded: bool = Query(default=False),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="viewer") or {}
    q = (q or "").strip()
    scope = (scope or "").strip().lower() or "educational"
    can_edit = _role_ge(str(user.get("role") or ""), required="editor")
    next_path = request.url.path
    if request.url.query:
        next_path = next_path + "?" + str(request.url.query)

    def _esc(s: Any) -> str:
        return (
            str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )

    filters: list[str] = []
    params: list[Any] = []
    if not include_excluded:
        filters.append("p.status = 'active'")
    # Hide providers that don't have a usable website URL (V1 cleanup rule).
    filters.append("NULLIF(TRIM(p.website_url), '') IS NOT NULL")
    if q:
        filters.append("(p.provider_name ILIKE %s OR p.provider_key ILIKE %s)")
        like = f"%{q}%"
        params.extend([like, like])
    if scope == "educational":
        filters.append("c.market_orientation = %s")
        params.append("Education-focused")

    where_sql = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = _directory_schema(
        f"""
        SELECT
          p.provider_key,
          p.provider_name,
          p.website_url,
          p.status,
          p.last_reviewed_at,
          p.review_interval_days,
          c.market_orientation,
          c.client_profile_indicators,
          c.educational_market_orientation,
          c.commercial_posture_signal,
          (
            SELECT jsonb_object_agg(sl.kind, sl.url)
            FROM "__SCHEMA__".provider_social_links sl
            WHERE sl.provider_id = p.id
          ) AS social_links,
          e.s3_bucket,
          e.s3_key
        FROM "__SCHEMA__".providers p
        LEFT JOIN "__SCHEMA__".provider_classifications c ON c.provider_id = p.id
        LEFT JOIN "__SCHEMA__".provider_evidence e ON e.provider_id = p.id AND e.kind = 'markdown'
        {where_sql}
        ORDER BY p.provider_name ASC
        LIMIT 2000;
        """
    ).strip()

    rows: list[tuple[Any, ...]] = []
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_directory_tables(cur)
            cur.execute(sql, params)
            rows = list(cur.fetchall())

    now = datetime.now(timezone.utc)
    tr_rows: list[str] = []
    for (
        provider_key,
        provider_name,
        website_url,
        status,
        last_reviewed_at,
        review_interval_days,
        market_orientation,
        client_profile_indicators,
        _educational_market_orientation,
        _commercial_posture_signal,
        social_links,
        _s3_bucket,
        s3_key,
    ) in rows:
        key = str(provider_key or "")
        name = str(provider_name or "")
        href = f"/trip_providers_research/{quote(key)}"
        website = str(website_url or "").strip()
        market = str(market_orientation or "").strip()
        client = str(client_profile_indicators or "").strip()

        last = ""
        due = ""
        if isinstance(last_reviewed_at, datetime):
            last = last_reviewed_at.date().isoformat()
            try:
                interval_days = int(review_interval_days or 365)
            except Exception:
                interval_days = 365
            if (now.date() - last_reviewed_at.date()).days >= max(30, interval_days):
                due = "Yes"
        else:
            due = "Yes"

        evidence_link = ""
        if str(s3_key or "").strip():
            evidence_link = f'<a href="/trip_providers_research/{quote(key)}/evidence">View</a>'

        website_html = f'<a href="{_esc(website)}" target="_blank" rel="noopener">Website</a>' if website else ""
        status_pill = f'<span class="pill">{_esc(status)}</span>' if status else ""
        social_html = _render_social_links_html(social_links) or ""
        actions_html = ""
        if can_edit and key:
            desired = "excluded" if str(status or "").strip().lower() != "excluded" else "active"
            label = "Exclude" if desired == "excluded" else "Include"
            actions_html = f"""
              <form method="post" action="/trip_providers_research/{_esc(quote(key))}/set_status" style="display:inline;">
                <input type="hidden" name="status" value="{_esc(desired)}" />
                <input type="hidden" name="next_path" value="{_esc(next_path)}" />
                <button class="btn" type="submit" style="padding:6px 10px; font-size:12px;">{_esc(label)}</button>
              </form>
            """.strip()
        tr_rows.append(
            f"""
            <tr>
              <td><a href="{href}">{_esc(name or key)}</a><div class="muted" style="margin-top:4px;"><code>{_esc(key)}</code></div></td>
              <td>{status_pill}</td>
              <td>{_esc(market)}</td>
              <td class="muted">{_esc(client)}</td>
              <td class="muted">{_esc(last)}</td>
              <td>{_esc(due)}</td>
              <td>{website_html}</td>
              <td class="muted">{social_html}</td>
              <td>{evidence_link}</td>
              <td>{actions_html}</td>
            </tr>
            """.strip()
        )

    scope_edu_selected = "selected" if scope == "educational" else ""
    scope_all_selected = "selected" if scope != "educational" else ""
    inc_excl_checked = "checked" if include_excluded else ""
    tbody_html = "".join(tr_rows) if tr_rows else '<tr><td colspan="10" class="muted">No results.</td></tr>'

    body_html = f"""
      <div class="card">
        <h1>Trip Providers (Research)</h1>
        <p class="muted">Searchable directory of educational trip providers. Default scope is <strong>Educational only</strong> (market orientation = <code>Education-focused</code>).</p>
        <div class="btnrow" style="margin-top:12px;">
          <a class="btn" href="/trip_providers/review_not_stated">Review Trip Providers</a>
        </div>
      </div>

      <div class="card">
        <h2>Search</h2>
        <form method="get" action="/trip_providers_research">
          <div class="grid-2">
            <div>
              <label>Search</label>
              <input type="text" name="q" value="{_esc(q)}" placeholder="Provider name or key" />
              <div class="muted" style="margin-top:6px;">Tip: try <code>korea</code> or <code>adventure_korea</code>.</div>
            </div>
            <div>
              <label>Scope</label>
              <select name="scope">
                <option value="educational" {scope_edu_selected}>Educational only</option>
                <option value="all" {scope_all_selected}>All providers</option>
              </select>
              <label style="margin-top:12px;">
                <input type="checkbox" name="include_excluded" value="true" {inc_excl_checked} />
                Include excluded
              </label>
            </div>
          </div>
          <div style="margin-top:12px;">
            <button class="btn primary" type="submit">Search</button>
            <a class="btn" href="/trip_providers_research">Reset</a>
          </div>
        </form>
      </div>

      <div class="card">
        <h2>Providers</h2>
        <div class="muted">Showing {len(tr_rows)} result(s). Limit: 2000.</div>
        <div class="divider"></div>
        <div class="section tablewrap">
	          <table>
	            <thead>
	              <tr>
	                <th>Provider</th>
	                <th>Status</th>
	                <th>Market</th>
	                <th>Client profile</th>
	                <th>Last reviewed</th>
	                <th>Review due</th>
	                <th>Links</th>
	                <th>Social</th>
	                <th>Evidence</th>
	                <th>Actions</th>
	              </tr>
	            </thead>
	            <tbody>
	              {tbody_html}
            </tbody>
          </table>
        </div>
      </div>
    """.strip()

    return _ui_shell(title="Trip Providers", active="trip_providers", body_html=body_html, max_width_px=1400, user=user)


@app.get("/trip_providers/review_not_stated", response_class=HTMLResponse)
def trip_providers_review_not_stated_ui(
    request: Request,
    done: str = Query(default=""),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="admin") or {}

    def _esc(s: Any) -> str:
        return (
            str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )

    sql = _directory_schema(
        """
        SELECT
          p.id,
          p.provider_key,
          p.provider_name,
          p.website_url,
          p.profile_json,
          COALESCE(NULLIF(c.market_orientation, ''), 'Not stated') AS market_orientation
        FROM "__SCHEMA__".providers p
        LEFT JOIN "__SCHEMA__".provider_classifications c ON c.provider_id = p.id
        WHERE COALESCE(NULLIF(c.market_orientation, ''), 'Not stated') = 'Not stated'
        ORDER BY p.provider_name ASC
        LIMIT 3000;
        """
    ).strip()

    rows: list[tuple[Any, ...]] = []
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_directory_tables(cur)
            cur.execute(sql)
            rows = list(cur.fetchall())

    tr_rows: list[str] = []
    for provider_id, provider_key, provider_name, website_url, profile_json, market_orientation in rows:
        key = str(provider_key or "").strip()
        if not key:
            continue
        name = str(provider_name or "").strip() or key
        website = str(website_url or "").strip()
        website_html = f'<a href="{_esc(website)}" target="_blank" rel="noopener">Website</a>' if website else '<span class="muted">—</span>'
        profile = profile_json if isinstance(profile_json, dict) else {}
        mission = ""
        try:
            v = profile.get("mission_or_purpose")
            if isinstance(v, dict):
                mission = str(v.get("value") or "").strip()
        except Exception:
            mission = ""

        detail_href = f"/trip_providers_research/{quote(key)}"
        action_name = f"action__{key}"
        tr_rows.append(
            f"""
            <tr>
              <td><a href="{detail_href}">{_esc(name)}</a><div class="muted" style="margin-top:4px;"><code>{_esc(key)}</code></div></td>
              <td>{website_html}</td>
              <td class="muted">{_esc(mission)}</td>
              <td class="muted">{_esc(market_orientation)}</td>
              <td>
                <label style="display:inline-flex; gap:6px; align-items:center; margin-right:12px;">
                  <input type="radio" name="{_esc(action_name)}" value="delete" checked />
                  Delete
                </label>
                <label style="display:inline-flex; gap:6px; align-items:center;">
                  <input type="radio" name="{_esc(action_name)}" value="education_focused" />
                  Education-focused
                </label>
                <input type="hidden" name="provider_id__{_esc(key)}" value="{_esc(provider_id)}" />
              </td>
            </tr>
            """.strip()
        )

    tbody_html = "".join(tr_rows) if tr_rows else '<tr><td colspan="5" class="muted">No results.</td></tr>'
    done_html = ""
    if (done or "").strip():
        done_html = '<div class="statusbox" style="margin-top:12px;">Saved.</div>'

    body_html = f"""
      <div class="card">
        <div class="muted"><a href="/trip_providers_research">← Back to Trip Providers</a></div>
        <h1>Review Trip Providers</h1>
        <p class="muted">Providers with market orientation = <code>Not stated</code>. Default action is <strong>Delete</strong> for every row.</p>
        {done_html}
        <div class="btnrow" style="margin-top:12px;">
          <button class="btn primary" type="submit" form="review-form">Apply changes</button>
          <a class="btn" href="/trip_providers/review_not_stated">Reset</a>
        </div>
      </div>

      <div class="card">
        <h2>Providers</h2>
        <div class="muted">Showing {len(tr_rows)} result(s).</div>
        <div class="divider"></div>
        <form id="review-form" method="post" action="/trip_providers/review_not_stated/apply" onsubmit="return confirm('Apply these changes? Deletions are permanent.');">
          <div class="section tablewrap">
            <table>
              <thead>
                <tr>
                  <th>Provider</th>
                  <th>Website</th>
                  <th>Mission</th>
                  <th>Market</th>
                  <th>Decision</th>
                </tr>
              </thead>
              <tbody>
                {tbody_html}
              </tbody>
            </table>
          </div>
        </form>
      </div>
    """.strip()

    return _ui_shell(title="Review Trip Providers", active="trip_providers", body_html=body_html, max_width_px=1400, user=user)


@app.post("/trip_providers/review_not_stated/apply")
async def trip_providers_review_not_stated_apply(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> Response:
    _require_access(request=request, x_api_key=x_api_key, role="admin")

    form = await request.form()
    # Extract actions of the form action__{provider_key} => decision
    actions: dict[str, str] = {}
    for k, v in form.items():
        ks = str(k)
        vs = str(v or "").strip()
        if ks.startswith("action__"):
            provider_key = ks.split("__", 1)[1]
            actions[provider_key] = vs

    if not actions:
        return RedirectResponse(url="/trip_providers/review_not_stated?done=1", status_code=303)

    # Apply changes in a single transaction.
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_directory_tables(cur)
            for provider_key, decision in actions.items():
                pk = _safe_provider_key(provider_key)
                d = (decision or "").strip().lower()
                if d == "delete":
                    cur.execute(_directory_schema('DELETE FROM "__SCHEMA__".provider_country WHERE provider_key=%s;').strip(), (pk,))
                    cur.execute(_directory_schema('DELETE FROM "__SCHEMA__".providers WHERE provider_key=%s;').strip(), (pk,))
                    continue
                if d == "education_focused":
                    # Ensure status=active and classification is set to Education-focused.
                    cur.execute(
                        _directory_schema(
                            """
                            UPDATE "__SCHEMA__".providers
                            SET status='active', updated_at=now()
                            WHERE provider_key=%s;
                            """
                        ).strip(),
                        (pk,),
                    )
                    cur.execute(
                        _directory_schema(
                            """
                            SELECT id
                            FROM "__SCHEMA__".providers
                            WHERE provider_key=%s
                            LIMIT 1;
                            """
                        ).strip(),
                        (pk,),
                    )
                    r = cur.fetchone()
                    provider_id = r[0] if r else None
                    if provider_id:
                        cur.execute(
                            _directory_schema(
                                """
                                INSERT INTO "__SCHEMA__".provider_classifications (
                                  provider_id,
                                  market_orientation,
                                  updated_at
                                )
                                VALUES (%s, %s, now())
                                ON CONFLICT (provider_id) DO UPDATE
                                SET market_orientation=EXCLUDED.market_orientation,
                                    updated_at=now();
                                """
                            ).strip(),
                            (provider_id, "Education-focused"),
                        )
                    continue
        conn.commit()

    return RedirectResponse(url="/trip_providers/review_not_stated?done=1", status_code=303)


@app.get("/trip_providers_research/{provider_key}", response_class=HTMLResponse)
def trip_providers_research_detail_ui(
    request: Request,
    provider_key: str,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="viewer") or {}
    provider_key = _safe_provider_key(provider_key)
    can_edit = _role_ge(str(user.get("role") or ""), required="editor")
    is_admin = _role_ge(str(user.get("role") or ""), required="admin")
    next_path = request.url.path

    def _esc(s: Any) -> str:
        return (
            str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )

    sql = _directory_schema(
        """
        SELECT
          p.id,
          p.provider_key,
          p.provider_name,
          p.website_url,
          p.status,
          p.last_reviewed_at,
          p.review_interval_days,
          p.profile_json,
          c.market_orientation,
          c.client_profile_indicators,
          c.educational_market_orientation,
          c.commercial_posture_signal,
          r.analytical_prompt_version,
          r.raw_json,
          r.generated_at,
          (
            SELECT jsonb_object_agg(sl.kind, sl.url)
            FROM "__SCHEMA__".provider_social_links sl
            WHERE sl.provider_id = p.id
          ) AS social_links,
          e.s3_bucket,
          e.s3_key
        FROM "__SCHEMA__".providers p
        LEFT JOIN "__SCHEMA__".provider_classifications c ON c.provider_id = p.id
        LEFT JOIN "__SCHEMA__".provider_analysis_runs r ON r.provider_id = p.id
        LEFT JOIN "__SCHEMA__".provider_evidence e ON e.provider_id = p.id AND e.kind = 'markdown'
        WHERE p.provider_key = %s
        LIMIT 5;
        """
    ).strip()

    row = None
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_directory_tables(cur)
            cur.execute(sql, (provider_key,))
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Provider not found")

    (
        _provider_id,
        key,
        name,
        website_url,
        status,
        last_reviewed_at,
        review_interval_days,
        profile_json,
        market_orientation,
        client_profile_indicators,
        educational_market_orientation,
        commercial_posture_signal,
        analytical_prompt_version,
        raw_json,
        generated_at,
        social_links,
        _s3_bucket,
        s3_key,
    ) = row

    countries_sql = _directory_schema(
        """
        SELECT country_or_territory
        FROM "__SCHEMA__".provider_country
        WHERE provider_key = %s
        ORDER BY country_or_territory ASC;
        """
    ).strip()
    countries: list[str] = []
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_directory_tables(cur)
            cur.execute(countries_sql, (provider_key,))
            countries = [str(r[0] or "").strip() for r in cur.fetchall() if str(r[0] or "").strip()]

    profile = profile_json if isinstance(profile_json, dict) else {}
    mission = (profile.get("mission_or_purpose") or {}).get("value") if isinstance(profile.get("mission_or_purpose"), dict) else ""
    trips = (profile.get("types_of_trips_offered") or {}).get("categories") if isinstance(profile.get("types_of_trips_offered"), dict) else []
    trips_text = ", ".join([str(x) for x in trips]) if isinstance(trips, list) else ""
    operates = (profile.get("where_the_provider_operates") or {}).get("value") if isinstance(profile.get("where_the_provider_operates"), dict) else []
    operates_text = ", ".join([str(x) for x in operates]) if isinstance(operates, list) else ""
    serves = (profile.get("who_the_provider_serves") or {}).get("value") if isinstance(profile.get("who_the_provider_serves"), dict) else []
    serves_text = ", ".join([str(x) for x in serves]) if isinstance(serves, list) else ""

    schools = (profile.get("named_schools_referenced") or {}).get("value") if isinstance(profile.get("named_schools_referenced"), dict) else []
    schools_text = ", ".join([str(x) for x in schools[:12]]) if isinstance(schools, list) else ""
    countries_text = ", ".join(countries[:24])
    countries_note = ""
    if len(countries) > 24:
        countries_note = f" (+{len(countries) - 24} more)"

    last = last_reviewed_at.date().isoformat() if isinstance(last_reviewed_at, datetime) else ""
    due = ""
    if isinstance(last_reviewed_at, datetime):
        now = datetime.now(timezone.utc)
        try:
            interval_days = int(review_interval_days or 365)
        except Exception:
            interval_days = 365
        if (now.date() - last_reviewed_at.date()).days >= max(30, interval_days):
            due = "Yes"
    else:
        due = "Yes"

    website = str(website_url or "").strip()
    website_html = f'<a href="{_esc(website)}" target="_blank" rel="noopener">{_esc(website)}</a>' if website else ""
    social_html = _render_social_links_html(social_links) or ""
    evidence_html = (
        f'<a class="btn" href="/trip_providers_research/{quote(provider_key)}/evidence">View evidence</a>'
        if str(s3_key or "").strip()
        else '<span class="muted">No evidence uploaded</span>'
    )
    actions_html = ""
    if can_edit:
        desired = "excluded" if str(status or "").strip().lower() != "excluded" else "active"
        label = "Exclude" if desired == "excluded" else "Include"
        actions_html += f"""
          <form method="post" action="/trip_providers_research/{_esc(quote(provider_key))}/set_status" style="display:inline;">
            <input type="hidden" name="status" value="{_esc(desired)}" />
            <input type="hidden" name="next_path" value="{_esc(next_path)}" />
            <button class="btn" type="submit">{_esc(label)}</button>
          </form>
        """.strip()
    if is_admin:
        actions_html += (
            " "
            + f"""
          <form method="post" action="/trip_providers_research/{_esc(quote(provider_key))}/delete" style="display:inline;" onsubmit="return confirm('Delete this provider from the database?');">
            <input type="hidden" name="next_path" value="/trip_providers_research" />
            <button class="btn" type="submit">Delete</button>
          </form>
        """.strip()
        )

    raw_pre = ""
    if isinstance(raw_json, (dict, list)):
        raw_pre = json.dumps(raw_json, indent=2)[:40_000]

    body_html = f"""
      <div class="card">
        <div class="muted"><a href="/trip_providers_research">← Back to list</a></div>
        <h1>{_esc(name or key)}</h1>
        <div class="muted"><code>{_esc(key)}</code> · <span class="pill">{_esc(status)}</span></div>
        <div style="margin-top:12px;">{website_html}</div>
        {f'<div class="muted" style="margin-top:10px;">{social_html}</div>' if social_html else ''}
        {f'<div class="btnrow" style="margin-top:12px;">{actions_html}</div>' if actions_html else ''}
      </div>

      <div class="grid-2">
        <div class="card">
          <h2>Classification</h2>
          <div class="section">
            <div class="muted">Market orientation</div>
            <div><strong>{_esc(market_orientation or '')}</strong></div>
            <div class="divider"></div>
            <div class="muted">Client profile indicators</div>
            <div>{_esc(client_profile_indicators or '')}</div>
            <div class="divider"></div>
            <div class="muted">Educational market orientation</div>
            <div>{_esc(educational_market_orientation or '')}</div>
            <div class="divider"></div>
            <div class="muted">Commercial posture signal</div>
            <div>{_esc(commercial_posture_signal or '')}</div>
          </div>
        </div>

        <div class="card">
          <h2>Profile</h2>
          <div class="section">
            <div class="muted">Mission</div>
            <div>{_esc(mission or '')}</div>
            <div class="divider"></div>
            <div class="muted">Trips offered</div>
            <div>{_esc(trips_text or '')}</div>
            <div class="divider"></div>
            <div class="muted">Where they operate</div>
            <div>{_esc(operates_text or '')}</div>
            <div class="divider"></div>
            <div class="muted">Who they serve</div>
            <div>{_esc(serves_text or '')}</div>
            <div class="divider"></div>
            <div class="muted">Named schools referenced</div>
            <div class="muted">{_esc(schools_text or '')}</div>
            <div class="divider"></div>
            <div class="muted">Countries / territories</div>
            <div class="muted">{_esc(countries_text or '—')}{_esc(countries_note)}</div>
          </div>
        </div>
      </div>

      <div class="card">
        <h2>Governance</h2>
        <div class="section">
          <div class="muted">Last reviewed</div>
          <div><strong>{_esc(last)}</strong></div>
          <div class="muted" style="margin-top:6px;">Review due: <strong>{_esc(due)}</strong></div>
          <div class="divider"></div>
          <div>{evidence_html}</div>
        </div>
      </div>

      <div class="card">
        <h2>Analysis (raw)</h2>
        <div class="muted">Version: <code>{_esc(analytical_prompt_version or '')}</code> · Generated: <code>{_esc(generated_at or '')}</code></div>
        <div class="divider"></div>
        <pre class="statusbox" style="max-height: 520px; overflow:auto;">{_esc(raw_pre)}</pre>
      </div>
    """.strip()

    return _ui_shell(title=f"Trip Provider: {name or key}", active="trip_providers", body_html=body_html, max_width_px=1400, user=user)


@app.get("/trip_providers_research/{provider_key}/evidence", response_class=HTMLResponse)
def trip_providers_research_evidence(
    request: Request,
    provider_key: str,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="viewer") or {}
    provider_key = _safe_provider_key(provider_key)

    def _esc(s: Any) -> str:
        return (
            str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )

    sql = _directory_schema(
        """
        SELECT e.s3_bucket, e.s3_key
        FROM "__SCHEMA__".providers p
        JOIN "__SCHEMA__".provider_evidence e ON e.provider_id = p.id AND e.kind = 'markdown'
        WHERE p.provider_key = %s
        LIMIT 1;
        """
    ).strip()

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_directory_tables(cur)
            cur.execute(sql, (provider_key,))
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Evidence not found")

    bucket, key = row
    cfg = get_s3_config()
    b = str(bucket or cfg.bucket)
    k = str(key or "").strip()
    if not k:
        raise HTTPException(status_code=404, detail="Evidence not found")

    try:
        data = get_bytes(region=cfg.region, bucket=b, key=k, max_bytes=2_000_000)
    except Exception as e:
        body_html = f"""
          <div class="card">
            <h1>Evidence</h1>
            <p class="muted">This document can’t be previewed ({_esc(str(e))}).</p>
            <div class="btnrow" style="margin-top:14px;">
              <a class="btn" href="/trip_providers_research/{_esc(quote(provider_key))}">Back to provider</a>
              <a class="btn" href="/trip_providers_research">Back to list</a>
            </div>
          </div>
        """.strip()
        return _ui_shell(title="Trip Provider Evidence", active="trip_providers", body_html=body_html, max_width_px=1100, user=user)

    md_text = data.decode("utf-8", errors="replace")
    rendered = _render_markdown_safe(md_text)

    extra_head = """
    <style>
      .doc-md h1,.doc-md h2,.doc-md h3 { margin-top: 18px; }
      .doc-md pre { background: #F5F6F7; border: 1px solid #F2F2F2; padding: 12px; overflow: auto; border-radius: 10px; }
      .doc-md code { background: #F5F6F7; padding: 0 4px; border-radius: 6px; }
      .doc-md pre code { background: transparent; padding: 0; }
    </style>
    """.strip()

    body_html = f"""
      <div class="card">
        <div class="muted"><a href="/trip_providers_research/{_esc(quote(provider_key))}">← Back to provider</a></div>
        <h1 style="margin-bottom:6px;">Evidence</h1>
        <div class="muted"><code>{_esc(provider_key)}</code></div>
        <div class="btnrow" style="margin-top:14px;">
          <a class="btn" href="/trip_providers_research/{_esc(quote(provider_key))}">Back</a>
          <a class="btn" href="/trip_providers_research">List</a>
        </div>
      </div>

      <div class="card doc-md">
        {rendered}
      </div>
    """.strip()

    return _ui_shell(
        title=f"{provider_key} — Evidence",
        active="trip_providers",
        body_html=body_html,
        max_width_px=1100,
        extra_head=extra_head,
        user=user,
    )


def _safe_next_path(next_path: str) -> str:
    p = (next_path or "").strip()
    if not p.startswith("/"):
        return "/apps"
    if p.startswith("//"):
        return "/apps"
    if "\n" in p or "\r" in p:
        return "/apps"
    return p


@app.post("/trip_providers_research/{provider_key}/set_status")
async def trip_providers_set_status(
    provider_key: str,
    request: Request,
    status: str = Form(...),
    next_path: str = Form(default="/trip_providers_research"),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> Response:
    _require_access(request=request, x_api_key=x_api_key, role="editor")
    provider_key = _safe_provider_key(provider_key)
    status_s = (status or "").strip().lower()
    if status_s not in {"active", "excluded"}:
        raise HTTPException(status_code=400, detail="Invalid status")

    sql = _directory_schema(
        """
        UPDATE "__SCHEMA__".providers
        SET status=%s, updated_at=now()
        WHERE provider_key=%s;
        """
    ).strip()
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_directory_tables(cur)
            cur.execute(sql, (status_s, provider_key))
        conn.commit()

    return RedirectResponse(url=_safe_next_path(next_path), status_code=303)


@app.post("/trip_providers_research/{provider_key}/delete")
async def trip_providers_delete(
    provider_key: str,
    request: Request,
    next_path: str = Form(default="/trip_providers_research"),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> Response:
    _require_access(request=request, x_api_key=x_api_key, role="admin")
    provider_key = _safe_provider_key(provider_key)

    sql_del_country = _directory_schema('DELETE FROM "__SCHEMA__".provider_country WHERE provider_key=%s;').strip()
    sql_del_provider = _directory_schema('DELETE FROM "__SCHEMA__".providers WHERE provider_key=%s;').strip()
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_directory_tables(cur)
            cur.execute(sql_del_country, (provider_key,))
            cur.execute(sql_del_provider, (provider_key,))
        conn.commit()

    return RedirectResponse(url=_safe_next_path(next_path), status_code=303)


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
    app_key: str = ""
    workflow: str = ""
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
                    SELECT prompt_key, app_key, workflow, name, natural_name, description, provider, model, is_active, updated_at
                    FROM "__SCHEMA__".prompts
                    ORDER BY app_key ASC, workflow ASC, prompt_key ASC;
                    """
                )
            )
            rows = cur.fetchall()

    prompts = [
        {
            "prompt_key": str(k),
            "app_key": str(ak or ""),
            "workflow": str(wf or ""),
            "name": str(n or ""),
            "natural_name": str(nn or ""),
            "description": str(d or ""),
            "provider": str(p or ""),
            "model": str(m or ""),
            "is_active": bool(a),
            "updated_at": ua.isoformat() if ua else None,
        }
        for (k, ak, wf, n, nn, d, p, m, a, ua) in rows
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
            "app_key": "weather",
            "workflow": "weather",
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
            "app_key": "weather",
            "workflow": "weather",
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
            "app_key": "weather",
            "workflow": "sunlight",
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
    out = _reconcile_required_prompts(edited_by=actor, change_note="Seed: reconcile required prompts")
    return {"ok": True, "created": out.get("created", []), "updated": out.get("updated", [])}


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
                    SELECT id, prompt_key, app_key, workflow, name, natural_name, description, provider, model, prompt_text, is_active, created_at, updated_at
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

    (pid, k, app_key, workflow, name, natural_name, desc, provider, model, text, is_active, created_at, updated_at) = row
    return {
        "ok": True,
        "prompt": {
            "id": str(pid),
            "prompt_key": str(k),
            "app_key": str(app_key or ""),
            "workflow": str(workflow or ""),
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
                    SELECT id, app_key, workflow, provider, model, prompt_text
                    FROM "__SCHEMA__".prompts
                    WHERE prompt_key=%s
                    LIMIT 1;
                    """
                ),
                (key,),
            )
            existing = cur.fetchone()

            before_app_key = ""
            before_workflow = ""
            before_provider = ""
            before_model = ""
            before_text = ""
            prompt_id = None

            if existing:
                prompt_id, before_app_key, before_workflow, before_provider, before_model, before_text = existing

                cur.execute(
                    _prompts_schema(
                        """
                        UPDATE "__SCHEMA__".prompts
                        SET app_key=%s, workflow=%s, name=%s, natural_name=%s, description=%s, provider=%s, model=%s, prompt_text=%s, updated_at=now()
                        WHERE id=%s;
                        """
                    ),
                    (
                        (body.app_key or "").strip(),
                        (body.workflow or "").strip(),
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
                        INSERT INTO "__SCHEMA__".prompts (prompt_key, app_key, workflow, name, natural_name, description, provider, model, prompt_text)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        RETURNING id;
                        """
                    ),
                    (
                        key,
                        (body.app_key or "").strip(),
                        (body.workflow or "").strip(),
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
                       before_text, after_text,
                       before_app_key, after_app_key, before_workflow, after_workflow,
                       before_provider, after_provider, before_model, after_model)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
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
                    str(before_app_key or ""),
                    str((body.app_key or "").strip()),
                    str(before_workflow or ""),
                    str((body.workflow or "").strip()),
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

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_prompts_tables(cur)
            _ensure_usage_tables(cur)

            required = _required_prompts()
            required_map = {str(r.get("prompt_key") or ""): r for r in required}
            required_keys = [k for k in required_map.keys() if k]

            cur.execute(
                _prompts_schema(
                    """
                    SELECT prompt_key, app_key, workflow, natural_name, provider, model, is_active, updated_at
                    FROM "__SCHEMA__".prompts
                    WHERE prompt_key = ANY(%s)
                    ORDER BY app_key ASC, workflow ASC, prompt_key ASC;
                    """
                ),
                (required_keys,),
            )
            rows_required = cur.fetchall()

            # Legacy/unrequired prompts (for an advanced toggle).
            cur.execute(
                _prompts_schema(
                    """
                    SELECT prompt_key, app_key, workflow, natural_name, provider, model, is_active, updated_at
                    FROM "__SCHEMA__".prompts
                    WHERE prompt_key <> ALL(%s)
                    ORDER BY app_key ASC, workflow ASC, prompt_key ASC;
                    """
                ),
                (required_keys,),
            )
            rows_other = cur.fetchall()

            cur.execute(
                _usage_schema(
                    """
                    SELECT prompt_key,
                           COALESCE(SUM(prompt_tokens),0) AS prompt_tokens,
                           COALESCE(SUM(completion_tokens),0) AS completion_tokens,
                           COALESCE(SUM(total_tokens),0) AS total_tokens,
                           COALESCE(SUM(cost_usd),0) AS cost_usd,
                           MAX(created_at) AS last_used
                    FROM "__SCHEMA__".llm_usage
                    WHERE prompt_key <> ''
                    GROUP BY prompt_key;
                    """
                )
            )
            stats_rows = cur.fetchall()

    stats: dict[str, dict[str, Any]] = {}
    for (pk, pt, ct, tt, cost, last_used) in stats_rows:
        stats[str(pk)] = {
            "prompt_tokens": int(pt or 0),
            "completion_tokens": int(ct or 0),
            "total_tokens": int(tt or 0),
            "cost_usd": float(cost or 0.0),
            "last_used": last_used.isoformat() if last_used else "",
        }

    existing_required = {str(r[0]) for r in rows_required}
    missing_required_keys = [k for k in required_keys if k not in existing_required]

    def _esc(s: Any) -> str:
        return (
            str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )

    def _build_group_html(rows: list[tuple[Any, ...]]) -> str:
        groups: dict[tuple[str, str], list[tuple[Any, ...]]] = {}
        for row in rows:
            (k, app_key, workflow, natural_name, provider, model, is_active, updated_at) = row
            ak = (str(app_key or "").strip() or "ungrouped").lower()
            wf = (str(workflow or "").strip() or "default").lower()
            groups.setdefault((ak, wf), []).append(row)

        out = ""
        for (ak, wf) in sorted(groups.keys(), key=lambda x: (x[0], x[1])):
            table_rows = ""
            for (k, _app_key, _workflow, natural_name, provider, model, is_active, updated_at) in groups[(ak, wf)]:
                key = str(k)
                st = stats.get(key, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "cost_usd": 0.0, "last_used": ""})
                table_rows += (
                    "<tr>"
                    f"<td><code>{_esc(key)}</code></td>"
                    f"<td>{_esc(natural_name or '')}</td>"
                    f"<td class=\"muted\">{_esc(provider or '')}</td>"
                    f"<td class=\"muted\"><code>{_esc(model or '')}</code></td>"
                    f"<td>{'YES' if bool(is_active) else 'NO'}</td>"
                    f"<td class=\"muted\"><code>{_esc(updated_at.isoformat() if updated_at else '')}</code></td>"
                    f"<td class=\"muted\"><code>{_esc(st.get('last_used') or '')}</code></td>"
                    f"<td class=\"right\"><code>{int(st.get('prompt_tokens') or 0)}</code></td>"
                    f"<td class=\"right\"><code>{int(st.get('completion_tokens') or 0)}</code></td>"
                    f"<td class=\"right\"><code>{int(st.get('total_tokens') or 0)}</code></td>"
                    f"<td class=\"right\"><code>${float(st.get('cost_usd') or 0.0):.6f}</code></td>"
                    f"<td><a class=\"btn\" href=\"/prompts/edit?prompt_key={_esc(key)}\">Details</a> <a class=\"btn\" href=\"/prompts/log/ui?prompt_key={_esc(key)}\">Log</a></td>"
                    "</tr>"
                )
            out += f"""
              <div class="divider"></div>
              <div style="display:flex; gap:10px; align-items:baseline; flex-wrap:wrap;">
                <h3 style="margin:0;">{_esc(ak)}</h3>
                <div class="muted">workflow: <code>{_esc(wf)}</code></div>
              </div>
              <div class="tablewrap" style="margin-top:10px;">
                <table>
                  <thead>
                    <tr>
                      <th>Key</th>
                      <th>Natural name</th>
                      <th>Provider</th>
                      <th>Model</th>
                      <th>Active</th>
                      <th>Updated</th>
                      <th>Last used (UTC)</th>
                      <th class="right">In</th>
                      <th class="right">Out</th>
                      <th class="right">Total</th>
                      <th class="right">Cost</th>
                      <th></th>
                    </tr>
                  </thead>
                  <tbody>
                    {table_rows}
                  </tbody>
                </table>
              </div>
            """.strip()
        return out

    required_html = _build_group_html(rows_required)
    other_html = _build_group_html(rows_other)

    missing_html = ""
    if missing_required_keys:
        items = "".join(f"<li><code>{_esc(k)}</code></li>" for k in missing_required_keys)
        missing_html = f"""
        <div class="card">
          <h2>Required prompts missing</h2>
          <div class="muted">These should be auto-created on startup. If this persists, the startup reconcile may have failed.</div>
          <ul style="margin: 10px 0 0 18px; padding: 0;">{items}</ul>
        </div>
        """.strip()

    body_html = f"""
      <div class="card">
        <h1>Prompts</h1>
        <p class="muted">Read-only inventory of prompts used by the system, plus per-prompt token/cost tracking. Prompt edits are handled via the API and logged.</p>
        <div class="btnrow">
          <a class="btn" href="/usage/ui">View usage log</a>
          <a class="btn" href="/prompts/log/ui">View change log</a>
        </div>
      </div>

      {missing_html}

      <div class="card">
        <h2>Used prompts</h2>
        <div class="muted">Grouped by <code>app_key</code> and <code>workflow</code>. Token totals are cumulative across all runs.</div>
        <div class="section">
          {required_html or '<div class="muted">No required prompts found.</div>'}
        </div>
      </div>

      <div class="card">
        <div class="btnrow" style="justify-content:space-between;">
          <h2 style="margin:0;">All prompts (advanced)</h2>
          <button id="btnToggleAll" class="btn" type="button">Show</button>
        </div>
        <div id="allPrompts" style="display:none;">
          <div class="muted">Includes legacy/unused prompts stored in the DB.</div>
          <div class="section">
            {other_html or '<div class="muted">No other prompts.</div>'}
          </div>
        </div>
      </div>
    """.strip()

    script = f"""
    <script>
      const btnToggle = document.getElementById('btnToggleAll');
      const all = document.getElementById('allPrompts');
      btnToggle && btnToggle.addEventListener('click', () => {{
        const open = all.style.display !== 'none';
        all.style.display = open ? 'none' : 'block';
        btnToggle.textContent = open ? 'Show' : 'Hide';
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
    allow_ui_editing = os.environ.get("PROMPTS_UI_EDITING", "").strip().lower() in {"1", "true", "yes", "on", "enabled"}
    can_edit = allow_ui_editing and _role_ge(str(user.get("role") or ""), required="editor")

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
        <p class="muted">This page is read-only by default. To enable editing, set <code>PROMPTS_UI_EDITING=enabled</code> and use an editor/admin account.</p>
        <div class="btnrow">
          <a class="btn" href="/prompts/ui">Back to prompts</a>
          <a class="btn" href="/prompts/log/ui?prompt_key={_esc(p.get('prompt_key') or '')}">View log</a>
        </div>
      </div>

      <div class="card">
        <form id="form">
          <label>App key</label>
          <input id="app_key" type="text" value="{_esc(p.get('app_key') or '')}" placeholder="weather / directory / flights" {'disabled' if not can_edit else ''} />
          <div style="height:12px;"></div>

          <label>Workflow</label>
          <input id="workflow" type="text" value="{_esc(p.get('workflow') or '')}" placeholder="weather / sunlight / prompts" {'disabled' if not can_edit else ''} />
          <div style="height:12px;"></div>

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
          app_key: String(document.getElementById('app_key').value || '').trim(),
          workflow: String(document.getElementById('workflow').value || '').trim(),
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


@app.get("/documents/list")
def documents_list(
    request: Request,
    limit: int = Query(default=500, ge=1, le=10000),
    folder: str = Query(default=""),
    status: str = Query(default=""),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_access(request=request, x_api_key=x_api_key, role="viewer")
    folder_s = _require_docs_folder(folder)
    status_s = (status or "").strip().lower()
    if status_s:
        status_s = _require_docs_status(status_s)

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_documents_tables(cur)
            if folder_s and status_s:
                cur.execute(
                    _docs_schema(
                        """
                        SELECT id, folder, filename, app_key, group_key, content_type, bytes, sha256, status, notes, storage, s3_bucket, s3_key, uploaded_by_username, created_at, updated_at
                        FROM "__SCHEMA__".documents
                        WHERE folder=%s AND status=%s
                        ORDER BY updated_at DESC
                        LIMIT %s;
                        """
                    ),
                    (folder_s, status_s, limit),
                )
            elif folder_s:
                cur.execute(
                    _docs_schema(
                        """
                        SELECT id, folder, filename, app_key, group_key, content_type, bytes, sha256, status, notes, storage, s3_bucket, s3_key, uploaded_by_username, created_at, updated_at
                        FROM "__SCHEMA__".documents
                        WHERE folder=%s
                        ORDER BY updated_at DESC
                        LIMIT %s;
                        """
                    ),
                    (folder_s, limit),
                )
            elif status_s:
                cur.execute(
                    _docs_schema(
                        """
                        SELECT id, folder, filename, app_key, group_key, content_type, bytes, sha256, status, notes, storage, s3_bucket, s3_key, uploaded_by_username, created_at, updated_at
                        FROM "__SCHEMA__".documents
                        WHERE status=%s
                        ORDER BY updated_at DESC
                        LIMIT %s;
                        """
                    ),
                    (status_s, limit),
                )
            else:
                cur.execute(
                    _docs_schema(
                        """
                        SELECT id, folder, filename, app_key, group_key, content_type, bytes, sha256, status, notes, storage, s3_bucket, s3_key, uploaded_by_username, created_at, updated_at
                        FROM "__SCHEMA__".documents
                        ORDER BY updated_at DESC
                        LIMIT %s;
                        """
                    ),
                    (limit,),
                )
            rows = cur.fetchall()

    docs = [
        {
            "id": str(i),
            "folder": str(f or ""),
            "filename": str(fn or ""),
            "app_key": str(ak or ""),
            "group_key": str(gk or ""),
            "content_type": str(ct or ""),
            "bytes": int(b or 0),
            "sha256": str(h or ""),
            "status": str(st or ""),
            "notes": str(n or ""),
            "storage": str(sto or ""),
            "s3_bucket": str(sb or ""),
            "s3_key": str(sk or ""),
            "uploaded_by": str(u or ""),
            "created_at": ca.isoformat() if ca else None,
            "updated_at": ua.isoformat() if ua else None,
        }
        for (i, f, fn, ak, gk, ct, b, h, st, n, sto, sb, sk, u, ca, ua) in rows
    ]
    return {"ok": True, "docs": docs}


@app.get("/documents/download/{doc_id}")
def documents_download(
    doc_id: str,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> Response:
    _require_access(request=request, x_api_key=x_api_key, role="viewer")
    try:
        did = uuid.UUID(doc_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid doc_id")

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_documents_tables(cur)
            cur.execute(
                _docs_schema(
                    """
                    SELECT filename, content_type, content, s3_bucket, s3_key, storage
                    FROM "__SCHEMA__".documents
                    WHERE id=%s
                    LIMIT 1;
                    """
                ),
                (did,),
            )
            row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Not found")

    filename, content_type, content, s3_bucket, s3_key, storage = row
    fn = str(filename or "document.md")
    ct = str(content_type or "application/octet-stream")
    bucket = str(s3_bucket or "").strip()
    key = str(s3_key or "").strip()
    storage_s = str(storage or "").strip().lower()

    if storage_s == "s3" or (bucket and key):
        cfg = get_s3_config()
        view_url = presign_get(region=cfg.region, bucket=bucket or cfg.bucket, key=key, expires_in=3600)
        return RedirectResponse(url=view_url, status_code=302)

    body = bytes(content or b"")
    if not body:
        raise HTTPException(status_code=404, detail="Document content missing")
    safe_fn = quote(fn)
    return Response(content=body, media_type=ct, headers={"Content-Disposition": f"attachment; filename*=UTF-8''{safe_fn}"})


@app.get("/documents/view/{doc_id}")
def documents_view(
    doc_id: str,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> Response:
    _require_access(request=request, x_api_key=x_api_key, role="viewer")
    try:
        did = uuid.UUID(doc_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid doc_id")

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_documents_tables(cur)
            cur.execute(
                _docs_schema(
                    """
                    SELECT filename, content_type, content, s3_bucket, s3_key, storage
                    FROM "__SCHEMA__".documents
                    WHERE id=%s
                    LIMIT 1;
                    """
                ),
                (did,),
            )
            row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Not found")

    filename, content_type, content, s3_bucket, s3_key, storage = row
    fn = str(filename or "document")
    ct_raw = str(content_type or "application/octet-stream").strip()
    bucket = str(s3_bucket or "").strip()
    key = str(s3_key or "").strip()
    storage_s = str(storage or "").strip().lower()

    # Avoid running HTML in-browser from stored docs.
    safe_ct = ct_raw
    if safe_ct.lower() in {"text/html", "application/xhtml+xml"}:
        safe_ct = "text/plain; charset=utf-8"

    if storage_s == "s3" or (bucket and key):
        cfg = get_s3_config()
        view_url = presign_get_inline(
            region=cfg.region,
            bucket=bucket or cfg.bucket,
            key=key,
            filename=fn,
            content_type=safe_ct,
            expires_in=3600,
        )
        return RedirectResponse(url=view_url, status_code=302)

    body = bytes(content or b"")
    if not body:
        raise HTTPException(status_code=404, detail="Document content missing")
    safe_fn = quote(fn)
    return Response(content=body, media_type=safe_ct, headers={"Content-Disposition": f"inline; filename*=UTF-8''{safe_fn}"})


@app.get("/documents/preview/{doc_id}", response_class=HTMLResponse, response_model=None)
def documents_preview(
    doc_id: str,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> Response:
    user = _require_access(request=request, x_api_key=x_api_key, role="viewer") or {}
    try:
        did = uuid.UUID(doc_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid doc_id")

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_documents_tables(cur)
            cur.execute(
                _docs_schema(
                    """
                    SELECT folder, filename, content_type, content, storage, s3_bucket, s3_key, status, notes, bytes, updated_at
                    FROM "__SCHEMA__".documents
                    WHERE id=%s
                    LIMIT 1;
                    """
                ),
                (did,),
            )
            row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Not found")

    folder, filename, content_type, content, storage, s3_bucket, s3_key, status, notes, bytes_n, updated_at = row
    folder_s = str(folder or "")
    fn = str(filename or "document.md")
    ct = str(content_type or "application/octet-stream")
    storage_s = str(storage or "").strip().lower()
    bucket = str(s3_bucket or "").strip()
    key = str(s3_key or "").strip()

    if not _is_markdown(filename=fn, content_type=ct):
        return RedirectResponse(url=f"/documents/view/{doc_id}", status_code=302)

    try:
        if storage_s == "s3" or (bucket and key):
            cfg = get_s3_config()
            data = get_bytes(
                region=cfg.region,
                bucket=bucket or cfg.bucket,
                key=key,
                max_bytes=_docs_max_preview_bytes(),
            )
        else:
            data = bytes(content or b"")
            if len(data) > _docs_max_preview_bytes():
                raise RuntimeError("Document too large to preview")
    except Exception as e:
        body_html = f"""
          <div class="card">
            <h1>Preview</h1>
            <p class="muted">This document can’t be previewed ({str(e)}).</p>
            <div class="btnrow" style="margin-top:14px;">
              <a class="btn" href="/documents/download/{doc_id}">Download</a>
              <a class="btn" href="/documents/ui">Back</a>
            </div>
          </div>
        """.strip()
        return HTMLResponse(_ui_shell(title="ETI360 Document Preview", active="apps", body_html=body_html, max_width_px=1100, user=user))

    md_text = data.decode("utf-8", errors="replace")
    rendered = _render_markdown_safe(md_text)

    def _esc(s: Any) -> str:
        return (
            str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )

    updated_iso = updated_at.isoformat() if updated_at else ""
    status_s = str(status or "")
    notes_s = str(notes or "")
    bytes_s = int(bytes_n or 0)
    title = fn
    if folder_s:
        title = f"{folder_s}/{fn}"

    extra_head = """
    <style>
      .doc-md h1,.doc-md h2,.doc-md h3 { margin-top: 18px; }
      .doc-md pre { background: #F5F6F7; border: 1px solid #F2F2F2; padding: 12px; overflow: auto; border-radius: 10px; }
      .doc-md code { background: #F5F6F7; padding: 0 4px; border-radius: 6px; }
      .doc-md pre code { background: transparent; padding: 0; }
      .doc-meta { display:flex; gap:12px; flex-wrap:wrap; align-items:center; }
    </style>
    """.strip()

    body_html = f"""
      <div class="card">
        <h1 style="margin-bottom:6px;">Preview</h1>
        <div class="doc-meta muted">
          <span><strong>File:</strong> <code>{_esc(title)}</code></span>
          <span><strong>Status:</strong> <span class="pill">{_esc(status_s)}</span></span>
          <span><strong>Size:</strong> <code>{_esc(bytes_s)}</code> bytes</span>
          <span><strong>Updated (Local):</strong> <time class="dt" datetime="{_esc(updated_iso)}"><code>{_esc(updated_iso).replace('T',' ').replace('Z','')}</code></time></span>
        </div>
        {f'<p class="muted" style="margin-top:10px;"><strong>Notes:</strong> {_esc(notes_s)}</p>' if notes_s else ''}
        <div class="btnrow" style="margin-top:14px;">
          <a class="btn" href="/documents/download/{_esc(doc_id)}">Download</a>
          <a class="btn" href="/documents/view/{_esc(doc_id)}" target="_blank" rel="noopener">View raw</a>
          <a class="btn" href="/documents/ui">Back</a>
        </div>
      </div>

      <div class="card doc-md">
        {rendered}
      </div>
    """.strip()

    extra_script = """
    <script>
      (function () {
        const els = document.querySelectorAll('time.dt[datetime]');
        for (const el of els) {
          const iso = el.getAttribute('datetime') || '';
          if (!iso) continue;
          const d = new Date(iso);
          if (!isFinite(d.getTime())) continue;
          el.setAttribute('title', iso);
          el.innerHTML = '<code>' + d.toLocaleString() + '</code>';
        }
      })();
    </script>
    """.strip()

    return HTMLResponse(
        _ui_shell(
            title="ETI360 Document Preview",
            active="apps",
            body_html=body_html,
            max_width_px=1100,
            extra_head=extra_head,
            extra_script=extra_script,
            user=user,
        )
    )


@app.post("/documents/upload")
async def documents_upload(
    request: Request,
    file: UploadFile = File(...),
    app_key: str = Form(default="planning"),
    group_key: str = Form(default=""),
    status: str = Form(default="future"),
    notes: str = Form(default=""),
    overwrite: str = Form(default="true"),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> Response:
    actor = _require_access(request=request, x_api_key=x_api_key, role="editor") or {}
    app_key_s = _require_docs_app_key(app_key)
    group_key_s = _normalize_group_key(group_key)
    folder_s = _docs_folder_for(app_key=app_key_s, group_key=group_key_s)
    status_s = _require_docs_status(status)
    notes_s = (notes or "").strip()
    overwrite_b = (overwrite or "").strip().lower() not in {"0", "false", "no", "off"}

    if not file or not file.filename:
        raise HTTPException(status_code=400, detail="file is required")
    filename = str(file.filename).strip()
    if len(filename) > 255:
        raise HTTPException(status_code=400, detail="filename too long")

    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="empty file")
    if len(data) > _docs_max_upload_bytes():
        raise HTTPException(status_code=413, detail="file too large")

    digest = sha256(data).hexdigest()
    content_type = (file.content_type or "").strip() or "application/octet-stream"
    safe_key_name = _safe_s3_filename(filename)

    username = str(actor.get("username") or "")
    user_id_uuid = None
    try:
        if str(actor.get("id") or "").strip() and str(actor.get("id")) not in {"disabled", "api_key", "startup"}:
            user_id_uuid = uuid.UUID(str(actor.get("id")))
    except Exception:
        user_id_uuid = None

    try:
        s3_cfg = get_s3_config()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 not configured for documents: {e}")

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_documents_tables(cur)
            cur.execute(
                _docs_schema(
                    """
                    SELECT id
                    FROM "__SCHEMA__".documents
                    WHERE folder=%s AND filename=%s
                    LIMIT 1;
                    """
                ),
                (folder_s, filename),
            )
            existing = cur.fetchone()
            if existing and not overwrite_b:
                raise HTTPException(status_code=409, detail="Document already exists (set overwrite=true)")

            if existing:
                (doc_id,) = existing
            else:
                doc_id = uuid.uuid4()

            s3_key = f"{s3_cfg.prefix}{_docs_s3_prefix()}{folder_s}/{doc_id}/{safe_key_name}"
            put_bytes(region=s3_cfg.region, bucket=s3_cfg.bucket, key=s3_key, body=data, content_type=content_type)

            if existing:
                cur.execute(
                    _docs_schema(
                        """
                        UPDATE "__SCHEMA__".documents
                        SET folder=%s, app_key=%s, group_key=%s, content_type=%s, bytes=%s, sha256=%s, status=%s, notes=%s,
                            storage='s3', s3_bucket=%s, s3_key=%s,
                            uploaded_by_user_id=%s, uploaded_by_username=%s, updated_at=now(),
                            content=%s
                        WHERE id=%s;
                        """
                    ),
                    (
                        folder_s,
                        app_key_s,
                        group_key_s,
                        content_type,
                        len(data),
                        digest,
                        status_s,
                        notes_s,
                        s3_cfg.bucket,
                        s3_key,
                        user_id_uuid,
                        username,
                        b"",
                        doc_id,
                    ),
                )
            else:
                cur.execute(
                    _docs_schema(
                        """
                        INSERT INTO "__SCHEMA__".documents
                          (id, folder, app_key, group_key, filename, content_type, bytes, sha256, status, notes, storage, s3_bucket, s3_key,
                           uploaded_by_user_id, uploaded_by_username, content)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'s3',%s,%s,%s,%s,%s);
                        """
                    ),
                    (
                        doc_id,
                        folder_s,
                        app_key_s,
                        group_key_s,
                        filename,
                        content_type,
                        len(data),
                        digest,
                        status_s,
                        notes_s,
                        s3_cfg.bucket,
                        s3_key,
                        user_id_uuid,
                        username,
                        b"",
                    ),
                )
        conn.commit()

    # If the browser submitted the HTML form directly (no JS), redirect back to the UI.
    accept = (request.headers.get("accept") or "").lower()
    if "text/html" in accept:
        return RedirectResponse(url="/documents/ui", status_code=303)

    return JSONResponse(
        {
            "ok": True,
            "id": str(doc_id),
            "folder": folder_s,
            "app_key": app_key_s,
            "group_key": group_key_s,
            "filename": filename,
            "bytes": len(data),
            "sha256": digest,
            "storage": "s3",
            "s3_bucket": s3_cfg.bucket,
            "s3_key": s3_key,
        }
    )


@app.post("/documents/delete/{doc_id}")
def documents_delete(
    doc_id: str,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> Response:
    _require_access(request=request, x_api_key=x_api_key, role="editor")
    try:
        did = uuid.UUID(doc_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid doc_id")

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_documents_tables(cur)
            cur.execute(_docs_schema('DELETE FROM "__SCHEMA__".documents WHERE id=%s;'), (did,))
        conn.commit()
    accept = (request.headers.get("accept") or "").lower()
    if "text/html" in accept:
        return RedirectResponse(url="/documents/ui", status_code=303)
    return JSONResponse({"ok": True, "deleted": doc_id})


@app.get("/documents/ui", response_class=HTMLResponse)
def documents_ui(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="viewer") or {}
    can_write = _role_ge(str(user.get("role") or ""), required="editor")

    # Server-render the list to avoid client-side (JS) failures preventing the table from populating.
    status_q = str(request.query_params.get("status", "") or "")
    status_s = (status_q or "").strip().lower()
    if status_s:
        status_s = _require_docs_status(status_s)

    def _esc(s: Any) -> str:
        return (
            str(s)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )

    def _fmt_bytes(n: int) -> str:
        if n < 1024:
            return f"{n} B"
        if n < 1024 * 1024:
            return f"{n/1024:.1f} KB"
        return f"{n/(1024*1024):.1f} MB"

    def _pretty_segment(s: str) -> str:
        s = (s or "").strip()
        if not s:
            return ""
        s = s.replace("_", " ").replace("-", " ")
        s = re.sub(r"\s+", " ", s).strip()
        return s.title()

    def _pretty_path(folder: str, filename: str) -> str:
        folder = (folder or "").strip().strip("/")
        filename = (filename or "").strip()
        if not folder:
            return _pretty_segment(filename)
        parts = [p for p in folder.split("/") if p.strip()]
        pretty_folder = " / ".join(_pretty_segment(p) for p in parts)
        return f"{pretty_folder} / {_pretty_segment(filename)}"

    def _pretty_status(s: str) -> str:
        s = (s or "").strip().lower()
        if s == "in_progress":
            return "In progress"
        if s == "finished":
            return "Finished"
        return "Future"

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_documents_tables(cur)
            if status_s:
                cur.execute(
                    _docs_schema(
                        """
                        SELECT id, folder, filename, status, bytes, updated_at
                        FROM "__SCHEMA__".documents
                        WHERE status=%s
                        ORDER BY updated_at DESC
                        LIMIT 5000;
                        """
                    ),
                    (status_s,),
                )
            else:
                cur.execute(
                    _docs_schema(
                        """
                        SELECT id, folder, filename, status, bytes, updated_at
                        FROM "__SCHEMA__".documents
                        ORDER BY updated_at DESC
                        LIMIT 5000;
                        """
                    )
                )
            rows = cur.fetchall()

    rows_html = ""
    for (doc_id, f, fn, st, b, ua) in rows:
        is_md = _is_markdown(filename=str(fn or ""), content_type="")
        pretty_name = _pretty_path(str(f or ""), str(fn or ""))
        updated = ua.isoformat() if ua else ""
        delete_html = ""
        if can_write:
            delete_html = f"""
              <form method="post" action="/documents/delete/{_esc(doc_id)}" style="display:inline;">
                <button class="btn" type="submit" onclick="return confirm('Delete this document?')">Delete</button>
              </form>
            """.strip()
        preview_html = ""
        if is_md:
            preview_html = f"<a class=\"btn\" href=\"/documents/preview/{_esc(doc_id)}\" target=\"_blank\" rel=\"noopener\">Preview</a> "
        rows_html += (
            "<tr>"
            f"<td><code>{_esc(pretty_name)}</code></td>"
            f"<td><code>{_esc(_pretty_status(str(st or '')))}</code></td>"
            f"<td class=\"right\"><code>{_esc(_fmt_bytes(int(b or 0)))}</code></td>"
            f"<td><time class=\"dt\" datetime=\"{_esc(updated)}\"><code>{_esc(updated).replace('T',' ').replace('Z','')}</code></time></td>"
            "<td style=\"white-space:nowrap;\">"
            f"{preview_html}"
            f"<a class=\"btn\" href=\"/documents/view/{_esc(doc_id)}\" target=\"_blank\" rel=\"noopener\">View</a> "
            f"<a class=\"btn\" href=\"/documents/download/{_esc(doc_id)}\">Download</a> "
            f"{delete_html}"
            "</td>"
            "</tr>"
        )
    if not rows_html:
        rows_html = '<tr><td colspan="5" class="muted">No documents yet.</td></tr>'

    upload_note = ""
    upload_disabled = ""
    if not can_write:
        upload_note = '<p class="muted">Upload is disabled (requires editor role).</p>'
        upload_disabled = "disabled"

    body_html = f"""
	      <div class="card">
	        <h1>Documents</h1>
	        <p class="muted">Upload and download project notes (stored in S3; metadata in Postgres). Organize by app/group and status.</p>
	      </div>

      <div class="section grid-2">
        <div class="card">
          <h2>Upload</h2>
	          {upload_note}
	          <form method="post" action="/documents/upload" enctype="multipart/form-data">
	            <label>App</label>
	            <input name="app_key" type="text" placeholder="planning" value="planning" {upload_disabled} />
	            <div style="height:12px;"></div>

	            <label>Group (optional)</label>
	            <input name="group_key" type="text" placeholder="2026 or SchoolName" {upload_disabled} />
	            <div style="height:12px;"></div>

	            <label>Status</label>
	            <select name="status" {upload_disabled}>
	              <option value="future">Future</option>
	              <option value="in_progress">In progress</option>
              <option value="finished">Finished</option>
            </select>
            <div style="height:12px;"></div>

            <label>Notes (optional)</label>
            <input name="notes" type="text" placeholder="Short description" {upload_disabled} />
            <div style="height:12px;"></div>

            <label>File</label>
            <input name="file" type="file" {upload_disabled} />
            <div style="height:12px;"></div>

            <label style="display:flex; align-items:center; gap:10px;">
              <input name="overwrite" type="checkbox" checked {upload_disabled} />
              Overwrite if filename exists in this app/group
            </label>

            <div class="btnrow">
              <button class="btn primary" type="submit" {upload_disabled}>Upload</button>
              <a class="btn" href="/apps">Back</a>
            </div>
          </form>
        </div>

        <div class="card">
          <h2>Browse</h2>
          <form method="get" action="/documents/ui" style="display:flex; gap:10px; flex-wrap:wrap; align-items:flex-end;">
            <div style="min-width:160px;">
              <label>Status</label>
              <select name="status">
                <option value="" {"selected" if not status_s else ""}>All</option>
                <option value="future" {"selected" if status_s == "future" else ""}>Future</option>
                <option value="in_progress" {"selected" if status_s == "in_progress" else ""}>In progress</option>
                <option value="finished" {"selected" if status_s == "finished" else ""}>Finished</option>
              </select>
            </div>
            <button class="btn" type="submit">Filter</button>
            <a class="btn" href="/documents/ui">Clear</a>
          </form>
          <div class="divider"></div>
          <div class="tablewrap" style="max-height: 70vh;">
            <table>
              <thead>
                <tr>
                  <th>File</th>
                  <th>Status</th>
                  <th class="right">Size</th>
                  <th>Updated (Local)</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {rows_html}
              </tbody>
            </table>
          </div>
          <div class="muted" style="margin-top:10px;">Rows: {_esc(len(rows))} · Stored in schema <code>{_esc(_require_safe_ident("DOCS_SCHEMA", DOCS_SCHEMA))}</code></div>
        </div>
      </div>
    """.strip()

    script = """
    <script>
      (function () {
        const els = document.querySelectorAll('time.dt[datetime]');
        for (const el of els) {
          const iso = el.getAttribute('datetime') || '';
          if (!iso) continue;
          const d = new Date(iso);
          if (!isFinite(d.getTime())) continue;
          el.setAttribute('title', iso);
          el.innerHTML = '<code>' + d.toLocaleString() + '</code>';
        }
      })();
    </script>
    """.strip()

    return _ui_shell(title="ETI360 Documents", active="apps", body_html=body_html, max_width_px=1400, user=user, extra_script=script)


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
    dir_schema = _require_safe_ident("DIRECTORY_SCHEMA", DIRECTORY_SCHEMA)
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
	          <div class="muted" style="margin-top:6px;">
	            Common: <a href="/db/ui?schema={_esc(WEATHER_SCHEMA)}">weather</a> · <a href="/db/ui?schema=ops">ops</a> · <a href="/db/ui?schema={_esc(dir_schema)}">directory</a>
	          </div>
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

    dir_schema = _require_safe_ident("DIRECTORY_SCHEMA", DIRECTORY_SCHEMA)
    applied: dict[str, list[str]] = {WEATHER_SCHEMA: [], dir_schema: []}
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                for stmt in _SCHEMA_STATEMENTS:
                    cur.execute(stmt)
                    applied[WEATHER_SCHEMA].append(stmt.splitlines()[0][:120])
                for stmt in _DIRECTORY_SCHEMA_STATEMENTS:
                    cur.execute(_directory_schema(stmt))
                    applied[dir_schema].append(stmt.splitlines()[0][:120])
            conn.commit()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Schema init failed: {e}") from e

    return {
        "ok": True,
        "schemas": [WEATHER_SCHEMA, dir_schema],
        "applied": applied,
    }


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
) -> tuple[dict[str, Any], dict[str, int], str, dict[str, int], str, dict[str, int], str]:
    """
    Returns:
      (result,
       perplexity_token_totals, perplexity_model,
       openai_weather_title_tokens, openai_weather_model,
       openai_daylight_title_tokens, openai_daylight_model)
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
    openai_weather_tokens = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    openai_weather_model_used = ""

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
                            openai_weather_tokens = {
                                "prompt_tokens": int(tok.get("prompt_tokens") or 0),
                                "completion_tokens": int(tok.get("completion_tokens") or 0),
                                "total_tokens": int(tok.get("total_tokens") or 0),
                            }
                            openai_weather_model_used = model or openai_weather_model_used
    except Exception:
        pass

    daylight_title = ""
    daylight_subtitle = ""
    openai_daylight_tokens = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    openai_daylight_model_used = ""
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
                    openai_daylight_tokens = {
                        "prompt_tokens": int(tok.get("prompt_tokens") or 0),
                        "completion_tokens": int(tok.get("completion_tokens") or 0),
                        "total_tokens": int(tok.get("total_tokens") or 0),
                    }
                    openai_daylight_model_used = model or openai_daylight_model_used
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
    return (
        result,
        perplexity_tokens,
        perplexity_model,
        openai_weather_tokens,
        openai_weather_model_used,
        openai_daylight_tokens,
        openai_daylight_model_used,
    )


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
    openai_weather_prompt = 0
    openai_weather_completion = 0
    openai_weather_total = 0
    openai_weather_model = ""
    openai_daylight_prompt = 0
    openai_daylight_completion = 0
    openai_daylight_total = 0
    openai_daylight_model = ""

    for q in locations:
        try:
            res, tok, model, wtok, wmodel, dtok, dmodel = _auto_generate_one(location_query=q, force_refresh=body.force_refresh)
            results.append(res)
            perplexity_prompt += int(tok.get("prompt_tokens") or 0)
            perplexity_completion += int(tok.get("completion_tokens") or 0)
            perplexity_total += int(tok.get("total_tokens") or 0)
            perplexity_model = model or perplexity_model
            openai_weather_prompt += int(wtok.get("prompt_tokens") or 0)
            openai_weather_completion += int(wtok.get("completion_tokens") or 0)
            openai_weather_total += int(wtok.get("total_tokens") or 0)
            openai_weather_model = wmodel or openai_weather_model
            openai_daylight_prompt += int(dtok.get("prompt_tokens") or 0)
            openai_daylight_completion += int(dtok.get("completion_tokens") or 0)
            openai_daylight_total += int(dtok.get("total_tokens") or 0)
            openai_daylight_model = dmodel or openai_daylight_model
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
            prompt_key="weather_normals_perplexity_v1",
            app_key="weather",
            prompt_workflow="weather",
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
            prompt_key="weather_titles_openai_v1",
            app_key="weather",
            prompt_workflow="weather",
            model=openai_weather_model or os.environ.get("OPENAI_MODEL", "").strip() or "gpt-5-mini",
            prompt_tokens=openai_weather_prompt,
            completion_tokens=openai_weather_completion,
            total_tokens=openai_weather_total,
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
            prompt_key="daylight_titles_openai_v1",
            app_key="weather",
            prompt_workflow="sunlight",
            model=openai_daylight_model or os.environ.get("OPENAI_MODEL", "").strip() or "gpt-5-mini",
            prompt_tokens=openai_daylight_prompt,
            completion_tokens=openai_daylight_completion,
            total_tokens=openai_daylight_total,
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
