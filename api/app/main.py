from __future__ import annotations

import csv
import json
import os
import re
import tempfile
import threading
import time
import uuid
from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import datetime, timedelta, timezone
from hashlib import pbkdf2_hmac, sha256
from io import StringIO
from pathlib import Path
from secrets import token_bytes
from typing import Any
from urllib.parse import urlencode, urlparse, quote
from urllib.request import urlopen

import bleach
import markdown as mdlib
import psycopg
import requests
from fastapi import FastAPI, File, Form, Header, HTTPException, Query, Request, Response, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app.arp_pipeline import (
    ARP_EXTRACT_SCHEMA,
    ARP_EXTRACT_SYSTEM,
    ARP_WRITE_SYSTEM,
    BM25Index,
    arp_extract_user_prompt,
    chunks_from_document,
    guess_content_type,
    parse_html_bytes,
    parse_pdf_bytes,
    render_arp_json_to_markdown,
    sha256_hex,
    tokenize,
    validate_arp_json,
)
from app.geo import CONTINENT_ORDER, continent_for_country
from app.weather.perplexity import fetch_monthly_weather_normals
from app.weather.daylight_chart import DaylightInputs, compute_daylight_summary, render_daylight_chart
from app.weather.llm_usage import estimate_cost_usd
from app.weather.openai_chat import OpenAIResult, chat_json, chat_text
from app.weather.s3 import get_bytes, get_s3_config, presign_get, presign_get_inline, put_bytes, put_png
from app.weather.weather_chart import MONTHS, MonthlyWeather, render_weather_chart

app = FastAPI(title="ETI360 Internal API", docs_url="/docs", redoc_url=None)
_STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

WEATHER_SCHEMA = "weather"
OPS_SCHEMA = os.environ.get("OPS_SCHEMA", "ops").strip() or "ops"
ARP_SCHEMA = os.environ.get("ARP_SCHEMA", "arp").strip() or "arp"
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
                _apply_ops_migrations(cur)
                _ensure_usage_tables(cur)
                _ensure_prompts_tables(cur)
                _ensure_documents_tables(cur)
            conn.commit()
        _bootstrap_schools_from_static()
        _reconcile_required_prompts(edited_by={"id": "startup", "username": "startup", "role": "admin"}, change_note="Startup reconcile")
        _maybe_start_jobs_worker()
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


def _ops_schema(sql: str) -> str:
    schema = _require_safe_ident("OPS_SCHEMA", OPS_SCHEMA)
    return sql.replace("__OPS_SCHEMA__", schema)


def _arp_schema(sql: str) -> str:
    schema = _require_safe_ident("ARP_SCHEMA", ARP_SCHEMA)
    return sql.replace("__ARP_SCHEMA__", schema)


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


def _stable_slug(*parts: str, max_len: int = 80) -> str:
    raw = " ".join(p.strip() for p in parts if p and p.strip())
    raw = re.sub(r"\s+", " ", raw).strip()
    base = re.sub(r"[^a-zA-Z0-9]+", "-", raw).strip("-").lower()
    if not base:
        base = "source"
    if len(base) <= max_len:
        return base
    digest = sha256(raw.encode("utf-8")).hexdigest()[:12]
    return f"{base[: max_len - 13]}-{digest}"


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


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _migrations_dir() -> Path:
    return Path(__file__).resolve().parent / "migrations"


def _list_sql_migrations() -> list[Path]:
    d = _migrations_dir()
    if not d.exists():
        return []
    files = [p for p in d.iterdir() if p.is_file() and p.name.endswith(".sql")]
    files.sort(key=lambda p: p.name)
    return files


def _apply_ops_migrations(cur: psycopg.Cursor) -> None:
    """
    Lightweight SQL migration runner.

    - Applies `api/app/migrations/*.sql` in filename order.
    - Tracks applied migrations in `OPS_SCHEMA.schema_migrations`.
    - Intended for small, internal systems where Docker/migration tooling is overkill.
    """
    schema = _require_safe_ident("OPS_SCHEMA", OPS_SCHEMA)
    migrations = _list_sql_migrations()
    if not migrations:
        return

    cur.execute("SELECT to_regclass(%s);", (f"{schema}.schema_migrations",))
    has_table = cur.fetchone()[0] is not None  # type: ignore[index]

    applied: set[str] = set()
    if has_table:
        cur.execute(f'SELECT version FROM "{schema}".schema_migrations;')
        applied = {str(r[0]) for r in (cur.fetchall() or [])}  # type: ignore[index]

    for p in migrations:
        version = p.stem
        if version in applied:
            continue
        sql = _read_text(p)
        sql = _ops_schema(sql)
        sql = _arp_schema(sql)
        cur.execute(sql)
        cur.execute(f'INSERT INTO "{schema}".schema_migrations(version) VALUES (%s) ON CONFLICT (version) DO NOTHING;', (version,))
        applied.add(version)


def _ensure_arp_tables(cur: psycopg.Cursor) -> None:
    # ARP schema/tables are created via migrations.
    _apply_ops_migrations(cur)


_JOBS_THREAD: threading.Thread | None = None


def _jobs_worker_mode() -> str:
    return (os.environ.get("JOBS_WORKER_MODE", "thread") or "thread").strip().lower()


def _maybe_start_jobs_worker() -> None:
    global _JOBS_THREAD
    mode = _jobs_worker_mode()
    if mode in {"0", "false", "no", "off", "disabled"}:
        return
    if _JOBS_THREAD and _JOBS_THREAD.is_alive():
        return

    t = threading.Thread(target=_jobs_worker_loop, name="eti360-jobs-worker", daemon=True)
    t.start()
    _JOBS_THREAD = t


def _jobs_poll_seconds() -> float:
    try:
        return float(os.environ.get("JOBS_POLL_SECONDS", "2").strip() or "2")
    except Exception:
        return 2.0


def _jobs_schema_name() -> str:
    return _require_safe_ident("OPS_SCHEMA", OPS_SCHEMA)


def _enqueue_job(*, kind: str, payload: dict[str, Any], created_by: str = "") -> str:
    kind = (kind or "").strip()
    if not kind:
        raise HTTPException(status_code=400, detail="Missing job kind")
    with _connect() as conn:
        with conn.cursor() as cur:
            _apply_ops_migrations(cur)
            schema = _jobs_schema_name()
            cur.execute(
                f'INSERT INTO "{schema}".jobs(kind, payload, created_by) VALUES (%s, %s::jsonb, %s) RETURNING id;',
                (kind, json.dumps(payload), created_by),
            )
            (job_id,) = cur.fetchone()
        conn.commit()
    return str(job_id)


def _get_job(job_id: str) -> dict[str, Any] | None:
    job_id = (job_id or "").strip()
    if not job_id:
        return None
    with _connect() as conn:
        with conn.cursor() as cur:
            _apply_ops_migrations(cur)
            schema = _jobs_schema_name()
            cur.execute(
                f'SELECT id, kind, status, payload, result, error, log, created_by, created_at, started_at, finished_at, heartbeat_at '
                f'FROM "{schema}".jobs WHERE id=%s;',
                (job_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            (
                jid,
                kind,
                status,
                payload,
                result,
                err,
                log,
                created_by,
                created_at,
                started_at,
                finished_at,
                heartbeat_at,
            ) = row
            return {
                "id": str(jid),
                "kind": kind,
                "status": status,
                "payload": payload,
                "result": result,
                "error": err,
                "log": log,
                "created_by": created_by,
                "created_at": created_at.isoformat() if created_at else None,
                "started_at": started_at.isoformat() if started_at else None,
                "finished_at": finished_at.isoformat() if finished_at else None,
                "heartbeat_at": heartbeat_at.isoformat() if heartbeat_at else None,
            }


def _list_jobs(*, limit: int = 100) -> list[dict[str, Any]]:
    limit = max(1, min(int(limit or 100), 500))
    with _connect() as conn:
        with conn.cursor() as cur:
            _apply_ops_migrations(cur)
            schema = _jobs_schema_name()
            cur.execute(
                f'SELECT id, kind, status, created_at, started_at, finished_at FROM "{schema}".jobs ORDER BY created_at DESC LIMIT %s;',
                (limit,),
            )
            out: list[dict[str, Any]] = []
            for jid, kind, status, created_at, started_at, finished_at in (cur.fetchall() or []):
                out.append(
                    {
                        "id": str(jid),
                        "kind": kind,
                        "status": status,
                        "created_at": created_at.isoformat() if created_at else None,
                        "started_at": started_at.isoformat() if started_at else None,
                        "finished_at": finished_at.isoformat() if finished_at else None,
                    }
                )
            return out


def _jobs_worker_loop() -> None:
    while True:
        try:
            job = _claim_next_job()
            if not job:
                time.sleep(_jobs_poll_seconds())
                continue
            _run_job(job_id=job["id"], kind=job["kind"], payload=job["payload"])
        except Exception as e:
            print(f"[jobs] worker loop error: {e}")
            time.sleep(2.0)


def _claim_next_job() -> dict[str, Any] | None:
    with _connect() as conn:
        with conn.cursor() as cur:
            _apply_ops_migrations(cur)
            schema = _jobs_schema_name()
            cur.execute(
                f"""
                WITH picked AS (
                  SELECT id
                  FROM "{schema}".jobs
                  WHERE status='queued'
                  ORDER BY created_at ASC
                  LIMIT 1
                  FOR UPDATE SKIP LOCKED
                )
                UPDATE "{schema}".jobs j
                SET status='running', started_at=COALESCE(started_at, now()), heartbeat_at=now()
                FROM picked
                WHERE j.id=picked.id
                RETURNING j.id, j.kind, j.payload;
                """.strip()
            )
            row = cur.fetchone()
            if not row:
                conn.commit()
                return None
            jid, kind, payload = row
            conn.commit()
            return {"id": str(jid), "kind": kind, "payload": payload or {}}


def _job_append_log(cur: psycopg.Cursor, *, job_id: str, line: str) -> None:
    schema = _jobs_schema_name()
    cur.execute(
        f'UPDATE "{schema}".jobs SET log = log || %s, heartbeat_at=now() WHERE id=%s;',
        ((line.rstrip() + "\n"), job_id),
    )


def _job_finish_ok(cur: psycopg.Cursor, *, job_id: str, result: dict[str, Any]) -> None:
    schema = _jobs_schema_name()
    cur.execute(
        f'UPDATE "{schema}".jobs SET status=%s, result=%s::jsonb, finished_at=now(), heartbeat_at=now() WHERE id=%s;',
        ("ok", json.dumps(result), job_id),
    )


def _job_finish_error(cur: psycopg.Cursor, *, job_id: str, error: str, log: str = "") -> None:
    schema = _jobs_schema_name()
    cur.execute(
        f'UPDATE "{schema}".jobs SET status=%s, error=%s, log = log || %s, finished_at=now(), heartbeat_at=now() WHERE id=%s;',
        ("error", (error or "")[:20000], (log or ""), job_id),
    )


def _arp_s3_key(*, prefix: str, source_id: str, content_type: str) -> tuple[str, str]:
    ext = "bin"
    ct = (content_type or "").strip().lower()
    if ct == "pdf":
        ext = "pdf"
        mime = "application/pdf"
    elif ct == "html":
        ext = "html"
        mime = "text/html"
    else:
        mime = "application/octet-stream"
    key = f"{prefix}arp/raw/{source_id}/original.{ext}"
    key = re.sub(r"//+", "/", key)
    return key, mime


def _arp_fetch_and_store_source(
    cur: psycopg.Cursor,
    *,
    source_id: str,
    url: str,
    job_id: str,
    s3_prefix: str,
) -> tuple[str, str, str, int]:
    """
    Returns (content_type, s3_bucket, s3_key, bytes_size)
    """
    cur.execute(
        _arp_schema('SELECT status, s3_bucket, s3_key, content_type FROM "__ARP_SCHEMA__".documents WHERE source_id=%s;'),
        (source_id,),
    )
    row = cur.fetchone()
    if row and str(row[0] or "") == "fetched" and str(row[1] or "") and str(row[2] or ""):
        return str(row[3] or ""), str(row[1] or ""), str(row[2] or ""), 0

    _job_append_log(cur, job_id=job_id, line=f"Fetch: {source_id}")

    u = (url or "").strip()
    parsed = urlparse(u) if u else None
    if not parsed or parsed.scheme not in {"http", "https"} or not parsed.netloc:
        msg = f"Invalid source URL (must start with http:// or https://): {u or '(empty)'}"
        cur.execute(
            _arp_schema(
                """
                UPDATE "__ARP_SCHEMA__".documents
                SET status='error', error=%s, fetched_at=now()
                WHERE source_id=%s;
                """
            ).strip(),
            (msg, source_id),
        )
        raise RuntimeError(msg)

    try:
        resp = requests.get(u, timeout=45, headers={"User-Agent": "ETI360/1.0"})
        resp.raise_for_status()
        raw = resp.content
    except Exception as e:
        cur.execute(
            _arp_schema(
                """
                UPDATE "__ARP_SCHEMA__".documents
                SET status='error', error=%s, fetched_at=now()
                WHERE source_id=%s;
                """
            ).strip(),
            (str(e), source_id),
        )
        raise

    header_ct = str(resp.headers.get("Content-Type") or "")
    ctype = guess_content_type(url=url, header_content_type=header_ct)
    s3cfg = get_s3_config()
    key, mime = _arp_s3_key(prefix=s3cfg.prefix or s3_prefix, source_id=source_id, content_type=ctype)
    put_bytes(region=s3cfg.region, bucket=s3cfg.bucket, key=key, body=raw, content_type=mime)
    digest = sha256_hex(raw)

    cur.execute(
        _arp_schema(
            """
            UPDATE "__ARP_SCHEMA__".documents
            SET status='fetched',
                content_type=%s,
                fetched_at=now(),
                sha256=%s,
                bytes_size=%s,
                s3_bucket=%s,
                s3_key=%s,
                error=''
            WHERE source_id=%s;
            """
        ).strip(),
        (ctype, digest, int(len(raw)), s3cfg.bucket, key, source_id),
    )
    return ctype, s3cfg.bucket, key, int(len(raw))


def _arp_prepare_activity(*, activity_id: int, job_id: str, only_missing: bool = True) -> dict[str, Any]:
    s3cfg = get_s3_config()
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_arp_tables(cur)
            cur.execute(_arp_schema('SELECT activity_name FROM "__ARP_SCHEMA__".activities WHERE activity_id=%s;'), (activity_id,))
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Unknown activity")
            (activity_name,) = row

            cur.execute(
                _arp_schema(
                    """
                    SELECT s.source_id, s.url, s.jurisdiction, s.authority_class, s.publication_date, s.source_type,
                           d.status, d.content_type, d.s3_bucket, d.s3_key
                    FROM "__ARP_SCHEMA__".sources s
                    LEFT JOIN "__ARP_SCHEMA__".documents d ON d.source_id = s.source_id
                    WHERE s.activity_id=%s
                    ORDER BY s.source_id ASC;
                    """
                ).strip(),
                (activity_id,),
            )
            sources = list(cur.fetchall())

            cur.execute(
                _arp_schema(
                    """
                    SELECT source_id, COUNT(*) AS n
                    FROM "__ARP_SCHEMA__".chunks
                    WHERE activity_id=%s
                    GROUP BY source_id;
                    """
                ).strip(),
                (activity_id,),
            )
            chunks_by_source = {str(sid): int(n or 0) for sid, n in (cur.fetchall() or [])}

        conn.commit()

    prepared = 0
    chunks_added = 0
    skipped = 0
    errors: list[str] = []

    for idx, (source_id, url, jurisdiction, authority_class, publication_date, source_type, d_status, d_ctype, d_bucket, d_key) in enumerate(
        sources, start=1
    ):
        with _connect() as conn:
            with conn.cursor() as cur:
                _ensure_arp_tables(cur)
                _job_append_log(cur, job_id=job_id, line=f"[{idx}/{len(sources)}] {source_id}")
                try:
                    sid = str(source_id)
                    existing_chunks = int(chunks_by_source.get(sid, 0))
                    if only_missing and str(d_status or "") == "fetched" and existing_chunks > 0:
                        skipped += 1
                        _job_append_log(cur, job_id=job_id, line=f"skip (already fetched; chunks={existing_chunks})")
                        conn.commit()
                        continue

                    # Prefer re-parsing from existing S3 object if already fetched, to avoid repeated external downloads.
                    ctype = str(d_ctype or "")
                    bucket = str(d_bucket or "")
                    key = str(d_key or "")
                    if str(d_status or "") == "fetched" and bucket and key:
                        raw = get_bytes(region=s3cfg.region, bucket=bucket, key=key, max_bytes=15 * 1024 * 1024)
                        if not ctype:
                            ctype = guess_content_type(url=str(url), header_content_type="")
                    else:
                        ctype, bucket, key, _ = _arp_fetch_and_store_source(
                            cur, source_id=sid, url=str(url), job_id=job_id, s3_prefix=s3cfg.prefix
                        )
                        raw = get_bytes(region=s3cfg.region, bucket=bucket, key=key, max_bytes=15 * 1024 * 1024)

                    if ctype == "pdf":
                        doc = parse_pdf_bytes(sid, raw)
                    else:
                        doc = parse_html_bytes(sid, raw)

                    chunks = chunks_from_document(
                        source_id=sid,
                        activity_id=int(activity_id),
                        jurisdiction=str(jurisdiction or ""),
                        authority_class=str(authority_class or ""),
                        publication_date=str(publication_date or ""),
                        doc=doc,
                    )

                    for c in chunks:
                        cur.execute(
                            _arp_schema(
                                """
                                INSERT INTO "__ARP_SCHEMA__".chunks
                                  (chunk_id, activity_id, source_id, heading, text, jurisdiction, authority_class, publication_date, loc)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (chunk_id) DO UPDATE SET
                                  text=EXCLUDED.text,
                                  heading=EXCLUDED.heading,
                                  jurisdiction=EXCLUDED.jurisdiction,
                                  authority_class=EXCLUDED.authority_class,
                                  publication_date=EXCLUDED.publication_date;
                                """
                            ).strip(),
                            (
                                c["chunk_id"],
                                c["activity_id"],
                                c["source_id"],
                                c["heading"],
                                c["text"],
                                c["jurisdiction"],
                                c["authority_class"],
                                c["publication_date"],
                                c["loc"],
                            ),
                        )
                    prepared += 1
                    chunks_added += len(chunks)
                except Exception as e:
                    errors.append(f"{source_id}: {e}")
                    _job_append_log(cur, job_id=job_id, line=f"ERROR: {source_id}: {e}")
                conn.commit()

    return {
        "activity_id": int(activity_id),
        "activity_name": str(activity_name),
        "sources_total": len(sources),
        "sources_prepared": prepared,
        "sources_skipped": skipped,
        "chunks_added": chunks_added,
        "errors": errors,
    }


def _arp_writer_input_md(activity_name: str, extracted: dict[str, list[dict[str, str]]]) -> str:
    def to_list(items: list[dict[str, str]]) -> str:
        lines = []
        for it in items:
            text = (it.get("text") or "").strip()
            if not text:
                continue
            jur = it.get("jurisdiction") or ""
            cls = it.get("authority_class") or ""
            pub = it.get("publication_date") or ""
            meta = ", ".join([x for x in [f"class {cls}" if cls else "", jur, pub] if x])
            suffix = f" ({meta})" if meta else ""
            lines.append(f"- {text}{suffix}")
        return "\n".join(lines) if lines else "- (no extracted items)"

    parts = [f"# Extracted evidence (activity: {activity_name})", ""]
    for title, key in [
        ("Environment assumptions", "environment_assumptions"),
        ("Participant assumptions", "participant_assumptions"),
        ("Supervision assumptions", "supervision_assumptions"),
        ("Common failure modes", "common_failure_modes"),
        ("Explicit cautions / abort criteria", "explicit_cautions_abort_criteria"),
        ("Explicit limitations stated by sources", "explicit_limitations_from_source"),
    ]:
        parts.append(f"## {title}")
        parts.append(to_list(extracted.get(key) or []))
        parts.append("")
    return "\n".join(parts).strip() + "\n"


def _arp_generate_activity(*, activity_id: int, top_k: int, job_id: str) -> dict[str, Any]:
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_arp_tables(cur)
            cur.execute(
                _arp_schema('SELECT activity_slug, activity_name FROM "__ARP_SCHEMA__".activities WHERE activity_id=%s;'),
                (activity_id,),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError("Unknown activity")
            activity_slug, activity_name = row
            cur.execute(
                _arp_schema(
                    """
                    SELECT scope_notes
                    FROM "__ARP_SCHEMA__".activities
                    WHERE activity_id=%s;
                    """
                ).strip(),
                (activity_id,),
            )
            row2 = cur.fetchone()
            scope_notes = str(row2[0] or "") if row2 else ""

            cur.execute(
                _arp_schema(
                    """
                    SELECT chunk_id, heading, text, jurisdiction, authority_class, publication_date
                    FROM "__ARP_SCHEMA__".chunks
                    WHERE activity_id=%s;
                    """
                ).strip(),
                (activity_id,),
            )
            chunks = list(cur.fetchall())
        conn.commit()

    if not chunks:
        with _connect() as conn:
            with conn.cursor() as cur:
                _job_append_log(cur, job_id=job_id, line="No chunks found; run Prepare first.")
            conn.commit()
        raise RuntimeError("No chunks found (prepare evidence first).")

    idx = BM25Index()
    by_id: dict[str, dict[str, Any]] = {}
    for chunk_id, heading, text, jurisdiction, authority_class, publication_date in chunks:
        cid = str(chunk_id)
        by_id[cid] = {
            "chunk_id": cid,
            "heading": str(heading or ""),
            "text": str(text or ""),
            "jurisdiction": str(jurisdiction or ""),
            "authority_class": str(authority_class or ""),
            "publication_date": str(publication_date or ""),
        }
        idx.add(cid, str(text or ""))

    results = idx.query(str(activity_name), top_k=int(top_k))
    selected_ids = [str(r.get("id")) for r in results if r.get("id")]
    selected = [by_id[cid] for cid in selected_ids if cid in by_id]

    model_extract = os.environ.get("OPENAI_MODEL_ARP_EXTRACT", "").strip() or os.environ.get("OPENAI_MODEL", "").strip() or "gpt-5-mini"
    model_write = os.environ.get("OPENAI_MODEL_ARP_WRITE", "").strip() or os.environ.get("OPENAI_MODEL", "").strip() or "gpt-5-mini"

    extracted: dict[str, list[dict[str, str]]] = {
        "environment_assumptions": [],
        "participant_assumptions": [],
        "supervision_assumptions": [],
        "common_failure_modes": [],
        "explicit_cautions_abort_criteria": [],
        "explicit_limitations_from_source": [],
    }

    run_id = _create_run_id()
    total_prompt = 0
    total_completion = 0
    total_tokens = 0

    for i, c in enumerate(selected, start=1):
        with _connect() as conn:
            with conn.cursor() as cur:
                _job_append_log(cur, job_id=job_id, line=f"Extract [{i}/{len(selected)}]: {c['chunk_id']}")
            conn.commit()
        try:
            res = chat_json(
                model=model_extract,
                system=ARP_EXTRACT_SYSTEM,
                user=arp_extract_user_prompt(activity=str(activity_name), heading=str(c["heading"]), excerpt=str(c["text"])),
                temperature=0.1,
            )
            total_prompt += res.prompt_tokens
            total_completion += res.completion_tokens
            total_tokens += res.total_tokens
            payload = res.payload or {}
            for k in extracted.keys():
                vals = payload.get(k) if isinstance(payload, dict) else None
                if isinstance(vals, list):
                    for v in vals:
                        if isinstance(v, str) and v.strip():
                            extracted[k].append(
                                {
                                    "text": v.strip(),
                                    "jurisdiction": str(c["jurisdiction"]),
                                    "authority_class": str(c["authority_class"]),
                                    "publication_date": str(c["publication_date"]),
                                }
                            )
        except Exception as e:
            with _connect() as conn:
                with conn.cursor() as cur:
                    _job_append_log(cur, job_id=job_id, line=f"Extract error: {e}")
                conn.commit()

    _record_llm_usage(
        run_id=run_id,
        workflow="arp",
        kind="extract",
        prompt_key="arp_extract_v1",
        app_key="arp",
        prompt_workflow="arp",
        provider="openai",
        model=model_extract,
        prompt_tokens=total_prompt,
        completion_tokens=total_completion,
        total_tokens=total_tokens,
        locations_count=0,
        ok_count=0,
        fail_count=0,
    )

    writer_md = _arp_writer_input_md(str(activity_name), extracted)

    write = chat_json(model=model_write, system=ARP_WRITE_SYSTEM, user=writer_md, temperature=0.2)
    arp_json = write.payload or {}
    ok, err = validate_arp_json(arp_json)
    if not ok:
        # One retry to fix structure.
        fix_prompt = writer_md + "\n\nFix output to valid JSON with required keys. Error: " + err
        write = chat_json(model=model_write, system=ARP_WRITE_SYSTEM, user=fix_prompt, temperature=0.2)
        arp_json = write.payload or {}
        ok, err = validate_arp_json(arp_json)
        if not ok:
            raise RuntimeError(f"Writer output invalid: {err}")

    report_md = render_arp_json_to_markdown(str(activity_name), arp_json)

    _record_llm_usage(
        run_id=run_id,
        workflow="arp",
        kind="write",
        prompt_key="arp_write_v1",
        app_key="arp",
        prompt_workflow="arp",
        provider="openai",
        model=write.model,
        prompt_tokens=write.prompt_tokens,
        completion_tokens=write.completion_tokens,
        total_tokens=write.total_tokens,
        locations_count=0,
        ok_count=0,
        fail_count=0,
    )

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_arp_tables(cur)
            cur.execute(
                _arp_schema(
                    """
                    INSERT INTO "__ARP_SCHEMA__".reports (activity_id, activity_slug, status, report_json, report_md, model, error)
                    VALUES (%s, %s, 'draft', %s::jsonb, %s, %s, '')
                    ON CONFLICT (activity_id) DO UPDATE SET
                      activity_slug=EXCLUDED.activity_slug,
                      report_json=EXCLUDED.report_json,
                      report_md=EXCLUDED.report_md,
                      model=EXCLUDED.model,
                      error='',
                      updated_at=now();
                    """
                ).strip(),
                (int(activity_id), str(activity_slug), json.dumps(arp_json), report_md, str(write.model)),
            )
        conn.commit()

    # Icon classification + deterministic SVG render (non-fatal).
    try:
        _arp_upsert_activity_icon(
            activity_id=int(activity_id),
            activity_slug=str(activity_slug),
            activity_name=str(activity_name),
            scope_notes=str(scope_notes or ""),
            report_md=str(report_md or ""),
            job_id=job_id,
        )
    except Exception as e:
        with _connect() as conn:
            with conn.cursor() as cur:
                _job_append_log(cur, job_id=job_id, line=f"Icon skipped: {e}")
            conn.commit()

    return {"activity_id": int(activity_id), "activity_slug": str(activity_slug), "report_url": f"/arp/report/{quote(str(activity_slug))}"}


def _arp_upsert_activity_icon(
    *,
    activity_id: int,
    activity_slug: str,
    activity_name: str,
    scope_notes: str,
    report_md: str,
    job_id: str,
) -> None:
    overview = extract_activity_overview(report_md) or (scope_notes or "")
    ih = icon_input_hash(activity_name=activity_name, overview=overview)

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_arp_tables(cur)
            cur.execute(
                _arp_schema(
                    """
                    SELECT activity_id, activity_slug, input_hash, renderer_version, spec_json, svg
                    FROM "__ARP_SCHEMA__".activity_icons
                    WHERE activity_id=%s;
                    """
                ).strip(),
                (int(activity_id),),
            )
            row = cur.fetchone()
            if row:
                rec = icon_record_from_row(row)
                if rec.svg and rec.input_hash == ih and rec.renderer_version == ICON_RENDERER_VERSION:
                    _job_append_log(cur, job_id=job_id, line="Icon: cached")
                    conn.commit()
                    return
        conn.commit()

    model_icon = os.environ.get("OPENAI_MODEL_ARP_ICON", "").strip() or os.environ.get("OPENAI_MODEL", "").strip() or "gpt-5-mini"
    user_prompt = (
        "Activity name:\n"
        f"{activity_name}\n\n"
        "Activity overview:\n"
        f"{overview}\n"
    )

    _job_append_log_safe(job_id=job_id, line=f"Icon: classify (model={model_icon})")
    run_id = _create_run_id()
    res = chat_json(model=model_icon, system=ICON_CLASSIFY_SYSTEM, user=user_prompt, temperature=0.0)

    spec_raw = res.payload or {}
    spec, err = validate_icon_spec(spec_raw)
    if err:
        spec = fallback_icon_spec(activity_name=activity_name, overview=overview)

    svg = render_icon_svg(spec, stroke_mode="primary")

    _record_llm_usage(
        run_id=run_id,
        workflow="arp",
        kind="icon_classify",
        prompt_key="arp_icon_v1",
        app_key="arp",
        prompt_workflow="arp",
        provider="openai",
        model=res.model,
        prompt_tokens=res.prompt_tokens,
        completion_tokens=res.completion_tokens,
        total_tokens=res.total_tokens,
        locations_count=0,
        ok_count=0,
        fail_count=0,
    )

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_arp_tables(cur)
            cur.execute(
                _arp_schema(
                    """
                    INSERT INTO "__ARP_SCHEMA__".activity_icons
                      (activity_id, activity_slug, input_hash, renderer_version, spec_json, svg, updated_at)
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s, now())
                    ON CONFLICT (activity_id) DO UPDATE SET
                      activity_slug=EXCLUDED.activity_slug,
                      input_hash=EXCLUDED.input_hash,
                      renderer_version=EXCLUDED.renderer_version,
                      spec_json=EXCLUDED.spec_json,
                      svg=EXCLUDED.svg,
                      updated_at=now();
                    """
                ).strip(),
                (int(activity_id), str(activity_slug), str(ih), str(ICON_RENDERER_VERSION), json.dumps(spec), str(svg)),
            )
        conn.commit()

    _job_append_log_safe(job_id=job_id, line="Icon: saved")


def _job_append_log_safe(*, job_id: str, line: str) -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            _job_append_log(cur, job_id=job_id, line=line)
        conn.commit()


def _run_job(*, job_id: str, kind: str, payload: dict[str, Any]) -> None:
    if kind == "weather_auto_batch":
        locations = payload.get("locations") if isinstance(payload, dict) else None
        force_refresh = bool(payload.get("force_refresh")) if isinstance(payload, dict) else False
        locs = [str(x).strip() for x in (locations or []) if str(x).strip()]
        if not locs:
            with _connect() as conn:
                with conn.cursor() as cur:
                    _job_finish_error(cur, job_id=job_id, error="No locations in job payload")
                conn.commit()
            return

        with _connect() as conn:
            with conn.cursor() as cur:
                _job_append_log(cur, job_id=job_id, line=f"Starting weather_auto_batch: {len(locs)} locations")
            conn.commit()

        try:
            result = _run_weather_auto_batch(locations=locs, force_refresh=force_refresh, job_id=job_id)
            with _connect() as conn:
                with conn.cursor() as cur:
                    _job_finish_ok(cur, job_id=job_id, result=result)
                conn.commit()
        except Exception as e:
            with _connect() as conn:
                with conn.cursor() as cur:
                    _job_finish_error(cur, job_id=job_id, error=str(getattr(e, "detail", e)))
                conn.commit()
        return

    if kind == "arp_prepare":
        ids = payload.get("activity_ids") if isinstance(payload, dict) else None
        activity_ids = [int(x) for x in (ids or []) if str(x).strip().isdigit()]
        if not activity_ids:
            with _connect() as conn:
                with conn.cursor() as cur:
                    _job_finish_error(cur, job_id=job_id, error="No activity_ids in job payload")
                conn.commit()
            return

        with _connect() as conn:
            with conn.cursor() as cur:
                _job_append_log(cur, job_id=job_id, line=f"Starting arp_prepare: {len(activity_ids)} activities")
            conn.commit()

        try:
            results = []
            for i, aid in enumerate(activity_ids, start=1):
                with _connect() as conn:
                    with conn.cursor() as cur:
                        _job_append_log(cur, job_id=job_id, line=f"[{i}/{len(activity_ids)}] activity_id={aid}")
                    conn.commit()
                results.append(_arp_prepare_activity(activity_id=int(aid), job_id=job_id, only_missing=True))
            with _connect() as conn:
                with conn.cursor() as cur:
                    _job_finish_ok(cur, job_id=job_id, result={"ok": True, "results": results})
                conn.commit()
        except Exception as e:
            with _connect() as conn:
                with conn.cursor() as cur:
                    _job_finish_error(cur, job_id=job_id, error=str(getattr(e, "detail", e)))
                conn.commit()
        return

    if kind == "arp_prepare_generate":
        ids = payload.get("activity_ids") if isinstance(payload, dict) else None
        top_k = int(payload.get("top_k") or 12) if isinstance(payload, dict) else 12
        top_k = max(1, min(top_k, 50))
        activity_ids = [int(x) for x in (ids or []) if str(x).strip().isdigit()]
        if not activity_ids:
            with _connect() as conn:
                with conn.cursor() as cur:
                    _job_finish_error(cur, job_id=job_id, error="No activity_ids in job payload")
                conn.commit()
            return

        with _connect() as conn:
            with conn.cursor() as cur:
                _job_append_log(
                    cur,
                    job_id=job_id,
                    line=f"Starting arp_prepare_generate: {len(activity_ids)} activities (top_k={top_k})",
                )
            conn.commit()

        prepare_results: list[dict[str, Any]] = []
        generate_results: list[dict[str, Any]] = []
        errors: list[str] = []

        try:
            # 1) Prepare evidence (missing-only, reuses existing S3)
            for i, aid in enumerate(activity_ids, start=1):
                with _connect() as conn:
                    with conn.cursor() as cur:
                        _job_append_log(cur, job_id=job_id, line=f"[{i}/{len(activity_ids)}] prepare activity_id={aid}")
                    conn.commit()
                prepare_results.append(_arp_prepare_activity(activity_id=int(aid), job_id=job_id, only_missing=True))

            # 2) Generate reports
            for i, aid in enumerate(activity_ids, start=1):
                with _connect() as conn:
                    with conn.cursor() as cur:
                        _job_append_log(cur, job_id=job_id, line=f"[{i}/{len(activity_ids)}] generate activity_id={aid}")
                    conn.commit()
                try:
                    generate_results.append(_arp_generate_activity(activity_id=int(aid), top_k=top_k, job_id=job_id))
                except Exception as e:
                    msg = str(getattr(e, "detail", e))
                    errors.append(f"{aid}: {msg}")
                    with _connect() as conn:
                        with conn.cursor() as cur:
                            _job_append_log(cur, job_id=job_id, line=f"ERROR: generate activity_id={aid}: {msg}")
                        conn.commit()

            with _connect() as conn:
                with conn.cursor() as cur:
                    _job_finish_ok(
                        cur,
                        job_id=job_id,
                        result={
                            "ok": True,
                            "prepare_results": prepare_results,
                            "generate_results": generate_results,
                            "errors": errors,
                        },
                    )
                conn.commit()
        except Exception as e:
            with _connect() as conn:
                with conn.cursor() as cur:
                    _job_finish_error(cur, job_id=job_id, error=str(getattr(e, "detail", e)))
                conn.commit()
        return

    if kind == "arp_generate":
        ids = payload.get("activity_ids") if isinstance(payload, dict) else None
        top_k = int(payload.get("top_k") or 12) if isinstance(payload, dict) else 12
        top_k = max(1, min(top_k, 50))
        activity_ids = [int(x) for x in (ids or []) if str(x).strip().isdigit()]
        if not activity_ids:
            with _connect() as conn:
                with conn.cursor() as cur:
                    _job_finish_error(cur, job_id=job_id, error="No activity_ids in job payload")
                conn.commit()
            return

        with _connect() as conn:
            with conn.cursor() as cur:
                _job_append_log(cur, job_id=job_id, line=f"Starting arp_generate: {len(activity_ids)} activities (top_k={top_k})")
            conn.commit()

        try:
            results = []
            for i, aid in enumerate(activity_ids, start=1):
                with _connect() as conn:
                    with conn.cursor() as cur:
                        _job_append_log(cur, job_id=job_id, line=f"[{i}/{len(activity_ids)}] activity_id={aid}")
                    conn.commit()
                results.append(_arp_generate_activity(activity_id=int(aid), top_k=top_k, job_id=job_id))
            with _connect() as conn:
                with conn.cursor() as cur:
                    _job_finish_ok(cur, job_id=job_id, result={"ok": True, "results": results})
                conn.commit()
        except Exception as e:
            with _connect() as conn:
                with conn.cursor() as cur:
                    _job_finish_error(cur, job_id=job_id, error=str(getattr(e, "detail", e)))
                conn.commit()
        return

    with _connect() as conn:
        with conn.cursor() as cur:
            _job_finish_error(cur, job_id=job_id, error=f"Unknown job kind: {kind}")
        conn.commit()


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


def _allow_unauth_write() -> bool:
    v = os.environ.get("ALLOW_UNAUTH_WRITE", "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _require_write_access(
    *,
    request: Request,
    x_api_key: str | None,
    role: str = "editor",
) -> dict[str, Any] | None:
    """
    Write/cost-incurring access check.

    Unlike `_require_access`, this does NOT automatically bypass when auth is disabled,
    unless `ALLOW_UNAUTH_WRITE=true` is set explicitly.
    """
    if _allow_unauth_write():
        return {"id": "disabled", "username": "disabled", "display_name": "Unauth writes allowed", "role": "admin"}

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
      logo_url TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'active',
      last_reviewed_at TIMESTAMPTZ,
      review_interval_days INTEGER NOT NULL DEFAULT 365,
      profile_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      CONSTRAINT providers_status_chk CHECK (status IN ('active','excluded'))
    );
    """.strip(),
    'ALTER TABLE "__SCHEMA__".providers ADD COLUMN IF NOT EXISTS logo_url TEXT NOT NULL DEFAULT \'\';',
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


_UI_CSS = ""  # legacy (replaced by /static/eti360.css)


def _ui_nav(*, active: str) -> str:
    items = [
        ("Apps", "/apps", "apps"),
        ("ARP", "/arp/ui", "arp"),
        ("Trip Providers", "/trip_providers_research", "trip_providers"),
        ("Countries", "/trip_providers/countries", "trip_providers_countries"),
        ("Weather", "/weather/ui", "weather"),
        ("Usage", "/usage/ui", "usage"),
        ("Jobs", "/jobs/ui", "jobs"),
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
    <link rel="stylesheet" href="/static/eti360.css" />
    {extra_head}
  </head>
  <body class="eti-app">
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
        <h1>Projects</h1>
        <p class="muted">One place to track internal tools, prototypes, and deployed surfaces. Edit <code>api/app/static/projects.json</code> and redeploy to update this list.</p>
      </div>

      <div class="card">
        <div style="display:flex; gap:12px; align-items:baseline; justify-content:space-between; flex-wrap:wrap;">
          <h2>Directory</h2>
          <div class="btnrow" style="margin-top:0;">
            <input id="q" type="text" placeholder="Search (name / description / tags)" style="max-width:420px;" />
            <select id="category" style="max-width:220px;">
              <option value="">All categories</option>
            </select>
            <button id="reset" class="btn" type="button">Clear</button>
          </div>
        </div>

        <div class="section tablewrap">
          <table>
            <thead>
              <tr>
                <th class="sortable" data-key="name">Project</th>
                <th class="sortable" data-key="category">Category</th>
                <th class="sortable" data-key="status">Status</th>
                <th>Description</th>
                <th>Links</th>
              </tr>
            </thead>
            <tbody id="rows"></tbody>
          </table>
        </div>
        <div class="muted" id="meta" style="margin-top:10px;">Loading…</div>
      </div>
    """.strip()

    script = """
    <script>
      const rowsEl = document.getElementById('rows');
      const metaEl = document.getElementById('meta');
      const qEl = document.getElementById('q');
      const resetEl = document.getElementById('reset');
      const categoryEl = document.getElementById('category');

      let items = [];
      let sortKey = localStorage.getItem('eti_projects_sortKey') || 'name';
      let sortDir = localStorage.getItem('eti_projects_sortDir') || 'asc';

      function esc(s) {
        return String(s ?? '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('\"','&quot;');
      }

      function keyValue(it, key) { return String(it?.[key] ?? '').toLowerCase(); }

      function compare(a, b) {
        const dir = sortDir === 'desc' ? -1 : 1;
        const av = keyValue(a, sortKey);
        const bv = keyValue(b, sortKey);
        return av.localeCompare(bv) * dir;
      }

      function normalizeTags(it) {
        const t = it?.tags;
        if (Array.isArray(t)) return t.map(x => String(x||'').trim()).filter(Boolean);
        return [];
      }

      function render() {
        const q = String(qEl.value || '').trim().toLowerCase();
        const cat = String(categoryEl.value || '').trim().toLowerCase();

        const filtered = items.filter((it) => {
          if (cat && String(it.category || '').toLowerCase() !== cat) return false;
          if (!q) return true;
          const tags = normalizeTags(it).join(' ');
          const hay = [it.name, it.description, it.category, it.status, tags, it.notes].map(x => String(x||'').toLowerCase()).join(' | ');
          return hay.includes(q);
        }).slice().sort(compare);

        rowsEl.innerHTML = '';
        for (const it of filtered) {
          const url = String(it.url || '').trim();
          const repo = String(it.repo_url || '').trim();
          const docs = String(it.docs_url || '').trim();
          const links = [];
          links.push(url ? `<a href="${esc(url)}">Open</a>` : `<span class="muted">No UI</span>`);
          if (repo) links.push(`<a href="${esc(repo)}" target="_blank" rel="noopener">Repo</a>`);
          if (docs) links.push(`<a href="${esc(docs)}" target="_blank" rel="noopener">Docs</a>`);
          const tags = normalizeTags(it);
          const tagsHtml = tags.length ? `<div class="muted" style="margin-top:2px;">${tags.map(t => `<span class="pill">${esc(t)}</span>`).join(' ')}</div>` : '';
          const notesHtml = it.notes ? `<div class="muted" style="margin-top:4px;">${esc(it.notes)}</div>` : '';

          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td><div style="font-weight:600; color:var(--text-secondary);">${esc(it.name || '')}</div>${tagsHtml}</td>
            <td>${esc(it.category || '')}</td>
            <td><span class="pill">${esc(it.status || '')}</span></td>
            <td>${esc(it.description || '')}${notesHtml}</td>
            <td>${links.join(' · ')}</td>
          `;
          rowsEl.appendChild(tr);
        }
        if (filtered.length === 0) {
          rowsEl.innerHTML = '<tr><td colspan="5" class="muted">No matching projects.</td></tr>';
        }
        metaEl.textContent = `Projects: ${filtered.length}/${items.length} • Sort: ${sortKey} (${sortDir})`;
      }

      function fillCategories() {
        const cats = new Set(items.map(it => String(it.category || '').trim()).filter(Boolean));
        const sorted = Array.from(cats).sort((a,b) => a.localeCompare(b));
        categoryEl.innerHTML = '<option value=\"\">All categories</option>' + sorted.map(c => `<option value=\"${esc(c)}\">${esc(c)}</option>`).join('');
      }

      async function load() {
        try {
          const res = await fetch('/static/projects.json', { cache: 'no-store' });
          const body = await res.json().catch(() => ([]));
          items = Array.isArray(body) ? body : [];
          fillCategories();
          metaEl.textContent = `Projects: ${items.length}`;
          render();
        } catch (e) {
          metaEl.textContent = 'Failed to load projects.json';
        }
      }

      qEl.value = localStorage.getItem('eti_projects_q') || '';
      qEl.addEventListener('input', () => { localStorage.setItem('eti_projects_q', qEl.value); render(); });
      categoryEl.value = localStorage.getItem('eti_projects_category') || '';
      categoryEl.addEventListener('change', () => { localStorage.setItem('eti_projects_category', categoryEl.value); render(); });
      resetEl.addEventListener('click', () => {
        qEl.value = '';
        categoryEl.value = '';
        localStorage.setItem('eti_projects_q', '');
        localStorage.setItem('eti_projects_category', '');
        render();
      });

      document.querySelectorAll('th.sortable').forEach((th) => {
        th.addEventListener('click', () => {
          const key = th.getAttribute('data-key');
          if (!key) return;
          if (sortKey === key) sortDir = sortDir === 'asc' ? 'desc' : 'asc';
          else { sortKey = key; sortDir = 'asc'; }
          localStorage.setItem('eti_projects_sortKey', sortKey);
          localStorage.setItem('eti_projects_sortDir', sortDir);
          render();
        });
      });

      load();
    </script>
    """.strip()

    return _ui_shell(title="ETI360 Projects", active="apps", body_html=body_html, max_width_px=1200, extra_script=script, user=user)


@app.get("/jobs/ui", response_class=HTMLResponse)
def jobs_ui(
    request: Request,
    job_id: str = Query(default=""),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="viewer") or {}

    jid = (job_id or "").strip()
    if jid:
        body_html = f"""
          <div class="card">
            <h1>Job</h1>
            <p class="muted mono">{jid}</p>
          </div>

          <div class="card">
            <div class="muted" id="status">Loading…</div>
            <pre class="statusbox mono" id="payload" style="margin-top:12px; max-height: 220px; overflow:auto;"></pre>
            <pre class="statusbox mono" id="log" style="margin-top:12px; max-height: 360px; overflow:auto;"></pre>
          </div>
        """.strip()

        script = f"""
        <script>
          const statusEl = document.getElementById('status');
          const payloadEl = document.getElementById('payload');
          const logEl = document.getElementById('log');

          async function tick() {{
            const res = await fetch('/jobs/api/{jid}', {{ cache: 'no-store' }});
            const body = await res.json().catch(() => ({{}}));
            if (!res.ok || !body.ok) {{
              statusEl.textContent = body.detail || body.error || `HTTP ${{res.status}}`;
              return;
            }}
            const j = body.job || {{}};
            statusEl.textContent = `Status: ${{j.status}} • Kind: ${{j.kind}} • Created: ${{j.created_at}}`;
            payloadEl.textContent = JSON.stringify(j.payload || {{}}, null, 2);
            logEl.textContent = String(j.log || '');
            if (j.status === 'queued' || j.status === 'running') setTimeout(tick, 1200);
          }}
          tick();
        </script>
        """.strip()

        return _ui_shell(title="ETI360 Jobs", active="jobs", body_html=body_html, extra_script=script, user=user)

    jobs = _list_jobs(limit=200)
    rows = []
    for j in jobs:
        jid = str(j.get("id") or "")
        kind = str(j.get("kind") or "")
        status = str(j.get("status") or "")
        created_at = str(j.get("created_at") or "")
        rows.append(
            f"<tr><td class=\"mono\"><a href=\"/jobs/ui?job_id={quote(jid)}\">{jid}</a></td><td>{kind}</td><td><span class=\"pill\">{status}</span></td><td class=\"muted\">{created_at}</td></tr>"
        )

    body_html = f"""
      <div class="card">
        <h1>Jobs</h1>
        <p class="muted">Background jobs (long-running workflows).</p>
      </div>

      <div class="card">
        <div class="section tablewrap">
          <table>
            <thead><tr><th>Job</th><th>Kind</th><th>Status</th><th>Created</th></tr></thead>
            <tbody>{''.join(rows) if rows else '<tr><td colspan=\"4\" class=\"muted\">No jobs yet.</td></tr>'}</tbody>
          </table>
        </div>
      </div>
    """.strip()
    return _ui_shell(title="ETI360 Jobs", active="jobs", body_html=body_html, max_width_px=1200, user=user)


@app.get("/jobs/api/{job_id}")
def jobs_api_item(
    job_id: str,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_access(request=request, x_api_key=x_api_key, role="viewer")
    job = _get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Unknown job")
    return {"ok": True, "job": job}


class ArpRunIn(BaseModel):
    activity_ids: list[int] = Field(default_factory=list)
    top_k: int = 12


class ArpCreateIn(BaseModel):
    activity_name: str
    activity_category: str | None = ""
    scope_notes: str | None = ""
    status: str | None = "active"
    resource_urls: str | None = ""


@app.get("/arp/ui", response_class=HTMLResponse)
def arp_ui(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="viewer") or {}
    body_html = """
      <div class="card">
        <h1>Activity Risk Profiles (ARP)</h1>
        <p class="muted">Activities + resources live in Postgres under the ARP schema. Prepare evidence (S3) and generate reports (OpenAI) via background jobs.</p>
      </div>

      <style>
        dialog.eti-modal {
          border: 1px solid rgba(0,0,0,0.08);
          border-radius: 14px;
          padding: 0;
          width: min(900px, calc(100vw - 40px));
          box-shadow: 0 18px 60px rgba(0,0,0,0.18);
        }
        dialog.eti-modal::backdrop { background: rgba(15, 23, 42, 0.55); }
        .eti-modal__header { padding: 18px 18px 0 18px; display:flex; justify-content:space-between; align-items:baseline; gap:12px; }
        .eti-modal__body { padding: 10px 18px 18px 18px; }
        .eti-modal__close { border: 0; background: transparent; font-size: 22px; line-height: 1; cursor: pointer; color: var(--text-muted); }
      </style>

      <dialog id="dlgApiKey" class="eti-modal">
        <div class="eti-modal__header">
          <div>
            <h2 style="margin:0;">API key</h2>
            <div class="muted">Stored in your browser localStorage for this site.</div>
          </div>
          <button id="btnCloseApiKey" class="eti-modal__close" type="button" aria-label="Close">×</button>
        </div>
        <div class="eti-modal__body">
          <label>X-API-Key (for writes)</label>
          <input id="apiKey" type="password" placeholder="Paste ETI360_API_KEY here" />
          <div class="muted" style="margin-top:6px;">
            Required for create/prepare/generate unless <code>ALLOW_UNAUTH_WRITE=true</code>.
          </div>
          <div class="btnrow" style="margin-top:12px;">
            <button class="btn primary" id="btnSaveApiKey" type="button">Save</button>
            <button class="btn" id="btnCancelApiKey" type="button">Cancel</button>
          </div>
        </div>
      </dialog>

      <dialog id="dlgCreate" class="eti-modal">
        <div class="eti-modal__header">
          <div>
            <h2 style="margin:0;">Add activity</h2>
            <div class="muted">Writes directly to Postgres.</div>
          </div>
          <button id="btnCloseCreate" class="eti-modal__close" type="button" aria-label="Close">×</button>
        </div>
        <div class="eti-modal__body">
          <form id="createForm">
            <div class="grid-2">
              <div>
                <label>Activity name</label>
                <input type="text" name="activity_name" placeholder="e.g., Stand-up paddleboarding (SUP)" required />
              </div>
              <div>
                <label>Category</label>
                <input type="text" name="activity_category" placeholder="e.g., Water-Based" />
              </div>
            </div>
            <div class="grid-2">
              <div>
                <label>Status</label>
                <input type="text" name="status" value="active" />
              </div>
              <div>
                <label>Resources (comma-separated URLs)</label>
                <input type="text" name="resource_urls" placeholder="https://… , https://… , https://…" />
                <div class="muted">Tip: you can paste multiple URLs separated by commas or new lines.</div>
              </div>
            </div>
            <div>
              <label>Context / scope notes</label>
              <textarea name="scope_notes" rows="3" placeholder="Optional notes to show in the table"></textarea>
            </div>
            <div class="btnrow">
              <button class="btn primary" id="btnCreate" type="button">Create activity</button>
              <button class="btn" id="btnCancelCreate" type="button">Cancel</button>
            </div>
          </form>
        </div>
      </dialog>

      <div class="card">
        <div style="display:flex; gap:12px; align-items:baseline; justify-content:space-between; flex-wrap:wrap;">
          <h2>Activities</h2>
          <div class="btnrow" style="margin-top:0;">
            <button id="btnOpenCreate" class="btn" type="button">Add activities</button>
            <input id="q" type="text" placeholder="Search (name / category / scope)" style="max-width:420px;" />
            <select id="category" style="max-width:220px;">
              <option value="">All categories</option>
            </select>
            <button id="reset" class="btn" type="button">Clear</button>
          </div>
        </div>

        <div class="section" style="display:flex; gap:10px; align-items:center; flex-wrap:wrap;">
          <button id="btnGenerate" class="btn primary" type="button">Prepare evidence &amp; generate report</button>
          <label class="muted" style="margin:0;">Top-k</label>
          <input id="topk" type="text" value="12" style="max-width:90px;" />
          <span class="muted" id="meta">Loading…</span>
        </div>
        <div class="muted" style="margin-top:-4px;">This runs Prepare first (missing-only), then writes the report.</div>

        <div class="section tablewrap">
          <table>
            <thead>
              <tr>
                <th style="width:40px;"></th>
                <th class="sortable" data-key="activity_name">Activity</th>
                <th class="sortable" data-key="activity_category">Category</th>
                <th>Resources</th>
                <th>Evidence</th>
                <th>Chunks</th>
                <th>Report</th>
              </tr>
            </thead>
            <tbody id="rows"></tbody>
          </table>
        </div>
      </div>
    """.strip()

    script = """
    <script>
      const rowsEl = document.getElementById('rows');
      const metaEl = document.getElementById('meta');
      const qEl = document.getElementById('q');
      const resetEl = document.getElementById('reset');
      const categoryEl = document.getElementById('category');
	      const topkEl = document.getElementById('topk');
	      const btnGenerate = document.getElementById('btnGenerate');
	      const btnCreate = document.getElementById('btnCreate');
	      const dlgApiKey = document.getElementById('dlgApiKey');
	      const btnCloseApiKey = document.getElementById('btnCloseApiKey');
	      const btnCancelApiKey = document.getElementById('btnCancelApiKey');
	      const btnSaveApiKey = document.getElementById('btnSaveApiKey');
	      const apiKeyEl = document.getElementById('apiKey');

      const dlgCreate = document.getElementById('dlgCreate');
      const btnOpenCreate = document.getElementById('btnOpenCreate');
      const btnCloseCreate = document.getElementById('btnCloseCreate');
      const btnCancelCreate = document.getElementById('btnCancelCreate');

      let items = [];
      let sortKey = localStorage.getItem('eti_arpweb_sortKey') || 'activity_name';
      let sortDir = localStorage.getItem('eti_arpweb_sortDir') || 'asc';

      function esc(s) {
        return String(s ?? '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('\"','&quot;');
      }
      function keyValue(it, key) { return String(it?.[key] ?? '').toLowerCase(); }
      function compare(a, b) {
        const dir = sortDir === 'desc' ? -1 : 1;
        return keyValue(a, sortKey).localeCompare(keyValue(b, sortKey)) * dir;
      }
      function fillCategories() {
        const cats = new Set(items.map(it => String(it.activity_category || '').trim()).filter(Boolean));
        const sorted = Array.from(cats).sort((a,b) => a.localeCompare(b));
        categoryEl.innerHTML = '<option value=\"\">All categories</option>' + sorted.map(c => `<option value=\"${esc(c)}\">${esc(c)}</option>`).join('');
      }
      function render() {
        const q = String(qEl.value || '').trim().toLowerCase();
        const cat = String(categoryEl.value || '').trim().toLowerCase();
        const filtered = items.filter((it) => {
          if (cat && String(it.activity_category||'').toLowerCase() !== cat) return false;
          if (!q) return true;
          const hay = [it.activity_name, it.activity_category, it.scope_notes].map(x => String(x||'').toLowerCase()).join(' | ');
          return hay.includes(q);
        }).slice().sort(compare);

        rowsEl.innerHTML = '';
        for (const it of filtered) {
          const rid = String(it.activity_id || '');
          const resUrl = `/arp/resources/${encodeURIComponent(rid)}`;
          const repUrl = it.has_report ? `/arp/report/${encodeURIComponent(it.activity_slug)}` : '';
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td><input class="pick" type="checkbox" data-id="${esc(rid)}" /></td>
            <td><div style="font-weight:600; color:var(--text-secondary);">${esc(it.activity_name||'')}</div><div class="muted">${esc(it.scope_notes||'')}</div></td>
            <td>${esc(it.activity_category||'')}</td>
            <td><a href="${resUrl}">Resources (${Number(it.sources_count||0)})</a></td>
            <td><span class="pill">${esc(it.docs_status||'')}</span></td>
            <td class="mono">${Number(it.chunks_count||0)}</td>
            <td>${repUrl ? `<a href="${repUrl}">View</a>` : '<span class="muted">—</span>'}</td>
          `;
          rowsEl.appendChild(tr);
        }
        if (filtered.length === 0) rowsEl.innerHTML = '<tr><td colspan="7" class="muted">No matching activities.</td></tr>';
        metaEl.textContent = `Activities: ${filtered.length}/${items.length} • Sort: ${sortKey} (${sortDir})`;
      }
      function selected() {
        return Array.from(document.querySelectorAll('input.pick:checked')).map(x => Number(x.getAttribute('data-id'))).filter(Boolean);
      }
      async function load() {
        const res = await fetch('/arp/api/activities', { cache:'no-store' });
        const body = await res.json().catch(() => ({}));
        items = Array.isArray(body.activities) ? body.activities : [];
        fillCategories();
        render();
      }
      async function enqueue(url, payload) {
        const apiKey = String(localStorage.getItem('eti_x_api_key') || '').trim();
        const headers = { 'Content-Type':'application/json' };
        if (apiKey) headers['X-API-Key'] = apiKey;
        const res = await fetch(url, { method:'POST', headers, body: JSON.stringify(payload||{}) });
        const body = await res.json().catch(() => ({}));
        if (res.status === 401) { openApiKey(); throw new Error(body.detail || body.error || 'Missing/invalid API key'); }
        if (!res.ok || !body.ok) throw new Error(body.detail || body.error || `HTTP ${res.status}`);
        window.location.href = `/jobs/ui?job_id=${encodeURIComponent(body.job_id)}`;
      }
      btnGenerate.addEventListener('click', async () => {
        const ids = selected();
        if (!ids.length) { metaEl.textContent = 'Select at least one activity.'; return; }
        const topk = Number(topkEl.value || '12');
        btnGenerate.disabled = true;
        try { await enqueue('/arp/api/generate', { activity_ids: ids, top_k: topk }); } finally { btnGenerate.disabled = false; }
      });
	      function openApiKey() {
	        if (!dlgApiKey) return;
	        apiKeyEl.value = localStorage.getItem('eti_x_api_key') || '';
	        try { dlgApiKey.showModal(); } catch (e) { dlgApiKey.setAttribute('open', 'open'); }
	        setTimeout(() => apiKeyEl.focus(), 0);
	      }
	      function closeApiKey() { if (dlgApiKey && dlgApiKey.open) dlgApiKey.close(); }
	      if (btnCloseApiKey) btnCloseApiKey.addEventListener('click', closeApiKey);
	      if (btnCancelApiKey) btnCancelApiKey.addEventListener('click', closeApiKey);
	      if (btnSaveApiKey) btnSaveApiKey.addEventListener('click', () => {
	        const v = String(apiKeyEl.value || '').trim();
	        if (v) localStorage.setItem('eti_x_api_key', v);
	        else localStorage.removeItem('eti_x_api_key');
	        closeApiKey();
	      });
	      if (dlgApiKey) dlgApiKey.addEventListener('click', (e) => { if (e.target === dlgApiKey) closeApiKey(); });

      function splitUrls(s) {
        const parts = String(s||'')
          .replaceAll('\\n', ',')
          .replaceAll('\\r', ',')
          .split(',')
          .map(x => x.trim())
          .filter(Boolean);
        // de-dupe, keep order
        const seen = new Set();
        const out = [];
        for (const p of parts) { if (!seen.has(p)) { seen.add(p); out.push(p); } }
        return out;
      }

      btnCreate.addEventListener('click', async () => {
        const form = document.getElementById('createForm');
        const fd = new FormData(form);
        const payload = {
          activity_name: String(fd.get('activity_name')||'').trim(),
          activity_category: String(fd.get('activity_category')||'').trim(),
          scope_notes: String(fd.get('scope_notes')||'').trim(),
          status: String(fd.get('status')||'').trim(),
          resource_urls: splitUrls(String(fd.get('resource_urls')||'')).join(', '),
        };
        if (!payload.activity_name) { metaEl.textContent = 'Enter an activity name.'; return; }

        const apiKey = String(localStorage.getItem('eti_x_api_key') || '').trim();
        const headers = { 'Content-Type':'application/json' };
        if (apiKey) headers['X-API-Key'] = apiKey;

        metaEl.textContent = 'Creating…';
        btnCreate.disabled = true;
        try {
          const res = await fetch('/arp/api/create', { method:'POST', headers, body: JSON.stringify(payload) });
          const body = await res.json().catch(() => ({}));
          if (res.status === 401) { openApiKey(); throw new Error(body.detail || body.error || 'Missing/invalid API key'); }
          if (!res.ok || !body.ok) throw new Error(body.detail || body.error || `HTTP ${res.status}`);
          metaEl.textContent = `Created activity ${body.activity_id} with ${body.sources || 0} resources.`;
          form.reset();
          form.querySelector('input[name=\"status\"]').value = 'active';
          if (dlgCreate && dlgCreate.open) dlgCreate.close();
          await load();
        } catch (e) {
          metaEl.textContent = 'Error: ' + String(e?.message || e);
        } finally {
          btnCreate.disabled = false;
        }
      });

      function openCreate() {
        if (!dlgCreate) return;
        try { dlgCreate.showModal(); } catch (e) { dlgCreate.setAttribute('open', 'open'); }
        const first = document.querySelector('#createForm input[name=\"activity_name\"]');
        if (first) setTimeout(() => first.focus(), 0);
      }
      function closeCreate() { if (dlgCreate && dlgCreate.open) dlgCreate.close(); }

      btnOpenCreate.addEventListener('click', openCreate);
      btnCloseCreate.addEventListener('click', closeCreate);
      btnCancelCreate.addEventListener('click', closeCreate);
      dlgCreate.addEventListener('click', (e) => { if (e.target === dlgCreate) closeCreate(); });

      qEl.value = localStorage.getItem('eti_arpweb_q') || '';
      qEl.addEventListener('input', () => { localStorage.setItem('eti_arpweb_q', qEl.value); render(); });
      categoryEl.value = localStorage.getItem('eti_arpweb_category') || '';
      categoryEl.addEventListener('change', () => { localStorage.setItem('eti_arpweb_category', categoryEl.value); render(); });
      resetEl.addEventListener('click', () => { qEl.value=''; categoryEl.value=''; localStorage.setItem('eti_arpweb_q',''); localStorage.setItem('eti_arpweb_category',''); render(); });
      document.querySelectorAll('th.sortable').forEach((th) => {
        th.addEventListener('click', () => {
          const key = th.getAttribute('data-key');
          if (!key) return;
          if (sortKey === key) sortDir = sortDir === 'asc' ? 'desc' : 'asc';
          else { sortKey = key; sortDir = 'asc'; }
          localStorage.setItem('eti_arpweb_sortKey', sortKey);
          localStorage.setItem('eti_arpweb_sortDir', sortDir);
          render();
        });
      });

      load();
    </script>
    """.strip()

    return _ui_shell(title="ETI360 ARP", active="arp", body_html=body_html, max_width_px=1200, extra_script=script, user=user)


@app.get("/arp/schools", response_class=HTMLResponse)
def arp_schools_ui_redirect(request: Request) -> Response:
    _ = request
    return RedirectResponse(url="/schools", status_code=307)


@app.get("/schools", response_class=HTMLResponse)
def schools_ui(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="viewer") or {}

    body_html = """
      <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:baseline; gap:12px; flex-wrap:wrap;">
          <h1>Schools</h1>
          <div class="btnrow" style="margin-top:0;">
            <a class="btn" href="/apps">Back to Apps</a>
          </div>
        </div>
        <p class="muted">Browse schools with trip/travel evidence (official websites only), extracted program names, and overview narratives.</p>
      </div>

      <style>
        /* Fit table to viewport without horizontal scroll (prefer wrapping + responsive column drops). */
        table.schools-table { width: 100%; table-layout: fixed; }
        table.schools-table th, table.schools-table td { white-space: normal; word-break: break-word; }
        .schools-wrap { overflow-x: hidden; }

        .clamp-2 {
          display: -webkit-box;
          -webkit-box-orient: vertical;
          -webkit-line-clamp: 2;
          overflow: hidden;
        }
        .clamp-3 {
          display: -webkit-box;
          -webkit-box-orient: vertical;
          -webkit-line-clamp: 3;
          overflow: hidden;
        }

	        @media (max-width: 1100px) {
	          /* Hide Programs first on smaller screens. */
	          table.schools-table th:nth-child(4),
	          table.schools-table td:nth-child(4) { display:none; }
	        }
	        @media (max-width: 920px) {
	          /* Then hide Score. */
	          table.schools-table th:nth-child(3),
          table.schools-table td:nth-child(3) { display:none; }
        }
      </style>

      <div class="card">
        <div style="display:flex; gap:12px; align-items:baseline; justify-content:space-between; flex-wrap:wrap;">
          <h2>Directory</h2>
          <div class="btnrow" style="margin-top:0;">
            <input id="q" type="text" placeholder="Search (school / domain / program)" style="max-width:480px;" />
            <label class="muted" style="display:flex; align-items:center; gap:8px; margin:0;">
              <input id="includeAll" type="checkbox" />
              Include all tiers
            </label>
          </div>
        </div>
        <div class="section tablewrap schools-wrap">
          <table class="schools-table">
	            <thead>
	              <tr>
	                <th style="width:32%;">School</th>
	                <th style="width:9%;">Tier</th>
	                <th class="mono" style="width:6%;">Score</th>
	                <th style="width:18%;">Programs</th>
	                <th style="width:35%;">LLM review</th>
	              </tr>
	            </thead>
            <tbody id="rows"></tbody>
          </table>
        </div>
        <div class="muted" id="meta" style="margin-top:10px;">Loading…</div>
      </div>

    """.strip()

    script = """
    <script>
      const rowsEl = document.getElementById('rows');
      const metaEl = document.getElementById('meta');
      const qEl = document.getElementById('q');
      const includeAllEl = document.getElementById('includeAll');

      let items = [];

      function esc(s) {
        return String(s ?? '').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('\"','&quot;');
      }
	      function pickPrograms(programs) {
        const arr = Array.isArray(programs) ? programs.map(x => String(x||'').trim()).filter(Boolean) : [];
        const first = arr.slice(0, 3);
        const more = arr.length > 3 ? ` +${arr.length - 3} more` : '';
        return first.map(esc).join(', ') + (more ? `<span class="muted">${esc(more)}</span>` : '');
      }
	      function matches(it, q) {
	        if (!q) return true;
	        const hay = [
	          it.name, it.primary_domain, it.school_key, it.tier,
	          it.review,
	          ...(Array.isArray(it.programs) ? it.programs : [])
	        ].map(x => String(x||'').toLowerCase()).join(' | ');
	        return hay.includes(q);
	      }

		      function normalizeSocial(obj) {
	        const out = {};
	        if (!obj || typeof obj !== 'object') return out;
	        for (const [k,v] of Object.entries(obj)) {
	          const key = String(k||'').trim().toLowerCase();
	          const url = String(v||'').trim();
	          if (!key || !url) continue;
	          out[key] = url;
	        }
	        return out;
		      }

		      function hasSocial(obj) {
		        const sl = normalizeSocial(obj);
		        return Object.keys(sl).length > 0;
		      }

		      function renderSocial(obj) {
		        const sl = normalizeSocial(obj);
	        const order = [
	          ['instagram','IG'],
	          ['facebook','FB'],
	          ['x','X'],
	          ['twitter','X'],
	          ['linkedin','IN'],
	          ['youtube','YT'],
	          ['tiktok','TT'],
	        ];
	        const seen = new Set();
	        const parts = [];
	        for (const [k,label] of order) {
	          const url = sl[k];
	          if (!url || seen.has(url)) continue;
	          seen.add(url);
	          parts.push(`<a href="${esc(url)}" target="_blank" rel="noopener">${esc(label)}</a>`);
	        }
	        // Any other platforms
	        for (const [k,url] of Object.entries(sl)) {
	          if (!url || seen.has(url)) continue;
	          if (order.some(([ok]) => ok === k)) continue;
	          seen.add(url);
	          parts.push(`<a href="${esc(url)}" target="_blank" rel="noopener">${esc(k)}</a>`);
	        }
	        return parts.length ? parts.join(' · ') : '<span class="muted">—</span>';
	      }
		      function render() {
	        const q = String(qEl.value || '').trim().toLowerCase();
	        const filtered = items.filter((it) => matches(it, q));
	        rowsEl.innerHTML = '';
		        for (const it of filtered) {
	          const key = String(it.school_key || '');
	          const home = String(it.homepage_url || '');
	          const detailUrl = `/schools/${encodeURIComponent(key)}`;
	          const llmUrl = `/schools/${encodeURIComponent(key)}/llm`;
		          const links = [
		            it.has_evidence ? `<a href="${detailUrl}">Evidence</a>` : '<span class="muted">Evidence —</span>',
		            it.has_llm ? `<a href="${llmUrl}">LLM</a>` : '<span class="muted">LLM —</span>',
		          ].join(' · ');
		          const socialLine = hasSocial(it.social_links) ? `<div class="muted" style="margin-top:2px;">${renderSocial(it.social_links)}</div>` : '';
		          const nameText = esc(it.name||'(missing)');
	          const nameHtml = home ? `<a href="${esc(home)}" target="_blank" rel="noopener">${nameText}</a>` : nameText;
	          const schoolCell = `
	            <div style="font-weight:600; color:var(--text-secondary);" class="clamp-2">${nameHtml}</div>
	            <div class="muted clamp-2">
	              ${home ? `${esc(it.primary_domain||home)}` : esc(it.primary_domain||'')}
	            </div>
	            <div class="muted" style="margin-top:2px;">${links}</div>
	            ${socialLine}
	          `;
	          const tr = document.createElement('tr');
	          tr.innerHTML = `
	            <td>${schoolCell}</td>
	            <td><span class="pill">${esc(it.tier || '')}</span></td>
	            <td class="mono">${Number(it.health_score||0)}</td>
	            <td><div class="clamp-2">${pickPrograms(it.programs||[]) || '<span class="muted">—</span>'}</div></td>
	            <td><div>${esc(it.review || '') || '<span class="muted">—</span>'}</div></td>
	          `;
	          rowsEl.appendChild(tr);
	        }
	        if (filtered.length === 0) rowsEl.innerHTML = '<tr><td colspan="5" class="muted">No matching schools.</td></tr>';
	        metaEl.textContent = `Schools: ${filtered.length}/${items.length}`;
	      }
	      async function load() {
	        const includeAll = includeAllEl.checked ? '1' : '0';
	        try {
	          const res = await fetch(`/schools/api/list?include_all=${encodeURIComponent(includeAll)}`, { cache:'no-store' });
	          const body = await res.json().catch(() => ({}));
	          if (!res.ok || !body.ok) {
	            metaEl.textContent = body.detail || body.error || `Failed to load schools (HTTP ${res.status})`;
	            rowsEl.innerHTML = '<tr><td colspan="5" class="muted">Failed to load.</td></tr>';
	            return;
	          }
	          if (body.fallback_used) metaEl.textContent = 'Showing all tiers (no Healthy/Partial matches found).';
	          items = Array.isArray(body.schools) ? body.schools : [];
	          render();
	        } catch (e) {
	          metaEl.textContent = 'Failed to load: ' + String(e?.message || e);
	          rowsEl.innerHTML = '<tr><td colspan="5" class="muted">Failed to load.</td></tr>';
	        }
	      }
      qEl.value = localStorage.getItem('eti_schools_q') || '';
      qEl.addEventListener('input', () => { localStorage.setItem('eti_schools_q', qEl.value); render(); });
      includeAllEl.checked = (localStorage.getItem('eti_schools_includeAll') || '') === '1';
      includeAllEl.addEventListener('change', () => { localStorage.setItem('eti_schools_includeAll', includeAllEl.checked ? '1' : '0'); load(); });

      load();
    </script>
    """.strip()

    return _ui_shell(title="Schools", active="apps", body_html=body_html, max_width_px=1200, extra_script=script, user=user)


def _escape_html(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


_URL_RE = re.compile(r"(https?://[^\s<>\"]+)", flags=re.IGNORECASE)


def _escape_and_linkify(s: str) -> str:
    """
    Convert plaintext URLs to clickable links while preserving HTML escaping.
    """
    text = str(s or "")
    if not text:
        return ""
    out: list[str] = []
    last = 0
    for m in _URL_RE.finditer(text):
        if m.start() > last:
            out.append(_escape_html(text[last : m.start()]))
        url = text[m.start() : m.end()]
        safe = _escape_html(url)
        out.append(f'<a href="{safe}" target="_blank" rel="noopener">{safe}</a>')
        last = m.end()
    if last < len(text):
        out.append(_escape_html(text[last:]))
    return "".join(out)


def _pretty_label(s: str) -> str:
    raw = str(s or "").strip()
    if not raw:
        return ""
    # Common normalization for snake_case keys/values.
    raw = raw.replace("_", " ").replace("-", " ")
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw[:1].upper() + raw[1:]


def _render_llm_json_readable(llm_json: Any) -> str:
    """
    Render the stored `school_overviews.llm_json` into a readable HTML summary.

    Expected shape is either:
    - a `result` dict from the pipeline, or
    - a dict with a nested `result` dict.
    """
    if not isinstance(llm_json, dict):
        return '<p class="muted">No structured output available.</p>'

    obj: dict[str, Any] = dict(llm_json)
    res = obj.get("result")
    if isinstance(res, dict):
        obj = res

    parts: list[str] = []

    overview = obj.get("trip_overview_narrative")
    if isinstance(overview, str) and overview.strip():
        parts.append(f"<h3>Trip overview</h3><p>{_escape_and_linkify(overview.strip())}</p>")

    programs = obj.get("programs")
    if isinstance(programs, list) and programs:
        rows: list[str] = []
        for p in programs:
            if not isinstance(p, dict):
                continue
            name = str(p.get("program_name") or "").strip() or "(Unnamed program)"
            fmt = str(p.get("program_format") or "").strip()
            trip_types = p.get("trip_types") if isinstance(p.get("trip_types"), list) else []
            grade_bands = p.get("grade_bands") if isinstance(p.get("grade_bands"), list) else []
            frequency = str(p.get("frequency") or "").strip()
            duration = str(p.get("duration") or "").strip()
            locations = p.get("locations") if isinstance(p.get("locations"), list) else []

            chips: list[str] = []
            for t in trip_types:
                lab = _pretty_label(str(t))
                if lab:
                    chips.append(f'<span class="pill">{_escape_html(lab)}</span>')
            for g in grade_bands:
                lab = str(g or "").strip()
                if lab:
                    chips.append(f'<span class="pill">{_escape_html(lab)}</span>')

            meta_bits: list[str] = []
            if fmt:
                meta_bits.append(_escape_html(fmt))
            if frequency:
                meta_bits.append(_escape_html(frequency))
            if duration:
                meta_bits.append(_escape_html(duration))
            if locations:
                locs = ", ".join([str(x or "").strip() for x in locations if str(x or "").strip()])
                if locs:
                    meta_bits.append(_escape_html(locs))
            meta_line = " · ".join(meta_bits)

            evidence_html = ""
            evidence = p.get("evidence")
            if isinstance(evidence, list) and evidence:
                ev_items: list[str] = []
                for ev in evidence:
                    if not isinstance(ev, dict):
                        continue
                    src = str(ev.get("source_url") or "").strip()
                    quote = str(ev.get("quote") or "").strip()
                    src_html = f'<a href="{_escape_html(src)}" target="_blank" rel="noopener">Source</a>' if src else ""
                    quote_html = _escape_and_linkify(quote) if quote else ""
                    if src_html or quote_html:
                        ev_items.append(f"<li>{src_html}{(' · ' if src_html and quote_html else '')}{quote_html}</li>")
                if ev_items:
                    evidence_html = "<div class=\"section\"><div class=\"muted\" style=\"margin-bottom:6px;\">Evidence</div><ul>" + "".join(ev_items) + "</ul></div>"

            rows.append(
                "<div class=\"section\" style=\"padding:12px 12px; border:1px solid var(--eti-border); border-radius:14px; background:var(--eti-bg);\">"
                f"<div style=\"font-weight:650; color:var(--text-secondary);\">{_escape_html(name)}</div>"
                + (f"<div class=\"muted\" style=\"margin-top:2px;\">{meta_line}</div>" if meta_line else "")
                + (f"<div style=\"margin-top:8px; display:flex; gap:6px; flex-wrap:wrap;\">{''.join(chips)}</div>" if chips else "")
                + evidence_html
                + "</div>"
            )

        if rows:
            parts.append("<h3>Programs</h3>" + "".join(rows))

    locations_all = obj.get("locations_all")
    if isinstance(locations_all, list) and locations_all:
        locs = [str(x or "").strip() for x in locations_all if str(x or "").strip()]
        if locs:
            parts.append("<h3>Locations mentioned</h3><p>" + _escape_html(", ".join(sorted(set(locs)))) + "</p>")

    unknowns = obj.get("unknowns")
    if isinstance(unknowns, list) and unknowns:
        unks = [str(x or "").strip() for x in unknowns if str(x or "").strip()]
        if unks:
            parts.append("<h3>Unknowns</h3><ul>" + "".join([f"<li>{_escape_and_linkify(x)}</li>" for x in unks]) + "</ul>")

    if not parts:
        return '<p class="muted">No readable fields found in the stored LLM output.</p>'
    return "".join(parts)


def _schools_static_dir() -> Path:
    return _STATIC_DIR / "schools_research"


def _bootstrap_schools_from_static() -> None:
    """
    One-time bootstrap import into Postgres from bundled pipeline outputs.

    Intended behavior:
    - If `__ARP_SCHEMA__.schools` is empty and bundled files exist, import them.
    - Otherwise, no-op.
    """
    base = _schools_static_dir()
    csv_path = base / "Schools.csv"
    if not csv_path.exists():
        return

    evidence_dir = base / "evidence_markdown"
    extracted_dir = base / "extracted"
    llm_dir = base / "llm_trip_programs"
    social_dir = base / "social_links"

    evidence_by_key: dict[str, str] = {}
    if evidence_dir.exists():
        for p in evidence_dir.glob("*.md"):
            key = p.stem.strip()
            if not key:
                continue
            evidence_by_key[key] = p.read_text(encoding="utf-8", errors="replace")

    extracted_by_key: dict[str, dict[str, Any]] = {}
    if extracted_dir.exists():
        for p in extracted_dir.glob("*.json"):
            key = p.stem.strip()
            if not key:
                continue
            try:
                obj = json.loads(p.read_text(encoding="utf-8", errors="replace"))
            except Exception:
                continue
            if isinstance(obj, dict):
                extracted_by_key[key] = obj

    llm_by_key: dict[str, dict[str, Any]] = {}
    if llm_dir.exists():
        for p in llm_dir.glob("*.json"):
            key = p.stem.strip()
            if not key:
                continue
            try:
                obj = json.loads(p.read_text(encoding="utf-8", errors="replace"))
            except Exception:
                continue
            if isinstance(obj, dict):
                llm_by_key[key] = obj

    social_by_key: dict[str, dict[str, str]] = {}
    if social_dir.exists():
        for p in social_dir.glob("*.json"):
            key = p.stem.strip()
            if not key:
                continue
            try:
                obj = json.loads(p.read_text(encoding="utf-8", errors="replace"))
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue
            # The pipeline social link snapshots have used either `links` or `social_links`.
            links = obj.get("links")
            if not isinstance(links, dict):
                links = obj.get("social_links")
            if not isinstance(links, dict):
                continue
            cleaned: dict[str, str] = {}
            for k, v in links.items():
                kk = str(k or "").strip().lower()
                vv = str(v or "").strip()
                if not kk or not vv:
                    continue
                cleaned[kk] = vv
            if cleaned:
                social_by_key[key] = cleaned

    schools_raw = csv_path.read_bytes()
    _, rows = _parse_csv_bytes(schools_raw)
    if not rows:
        return

    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_arp_tables(cur)
            cur.execute(_arp_schema('SELECT COUNT(*) FROM "__ARP_SCHEMA__".schools;'))
            (existing,) = cur.fetchone()
            has_any = int(existing or 0) > 0

            cur.execute(
                _arp_schema(
                    """
                    SELECT
                      s.school_key,
                      (COALESCE(s.social_links,'{}'::jsonb) <> '{}'::jsonb) AS has_social,
                      (e.school_key IS NOT NULL AND COALESCE(e.evidence_markdown,'') <> '') AS has_evidence,
                      (o.school_key IS NOT NULL AND (COALESCE(o.llm_json,'{}'::jsonb) <> '{}'::jsonb OR COALESCE(o.narrative,'') <> '' OR COALESCE(o.overview_75w,'') <> '')) AS has_llm
                    FROM "__ARP_SCHEMA__".schools s
                    LEFT JOIN "__ARP_SCHEMA__".school_evidence e ON e.school_key = s.school_key
                    LEFT JOIN "__ARP_SCHEMA__".school_overviews o ON o.school_key = s.school_key;
                    """
                ).strip()
            )
            existing_state: dict[str, dict[str, bool]] = {}
            for k, has_social, has_evidence, has_llm in cur.fetchall() or []:
                existing_state[str(k)] = {
                    "has_social": bool(has_social),
                    "has_evidence": bool(has_evidence),
                    "has_llm": bool(has_llm),
                }

            schools_count = 0
            evidence_count = 0
            json_count = 0

            for r in rows:
                nr = _norm_row(r)
                school_key = _row_get(nr, "school_key", "schoolkey", "key")
                name = _row_get(nr, "School", "school", "name")
                homepage_url = _row_get(nr, "URL", "url", "homepage_url", "homepage")
                primary_domain = _row_get(nr, "primary_domain", "primarydomain", "domain")
                last_crawled_at = _parse_dt_maybe(_row_get(nr, "last_crawled_at", "lastcrawledat", "last_crawled"))
                tier = _row_get(nr, "trip_crawl_tier", "tier")
                hs_raw = _row_get(nr, "trip_crawl_health_score", "health_score", "score")
                extracted_at = _parse_dt_maybe(_row_get(nr, "extracted_at", "extractedat"))
                logo_url = _row_get(nr, "logo_url", "logourl")

                try:
                    health_score = int(float(hs_raw)) if hs_raw else 0
                except Exception:
                    health_score = 0

                emails = {
                    "general_email": _row_get(nr, "general_email"),
                    "admissions_email": _row_get(nr, "admissions_email"),
                    "communications_email": _row_get(nr, "communications_email"),
                }
                emails = {k: v for k, v in emails.items() if v}

                if not school_key:
                    if homepage_url or name:
                        school_key = _stable_slug(name or homepage_url, max_len=90)
                    else:
                        continue
                try:
                    school_key = _safe_school_key(school_key)
                except Exception:
                    continue

                social_links: dict[str, str] = {}
                if school_key in social_by_key:
                    social_links = dict(social_by_key.get(school_key) or {})
                else:
                    extracted_obj = extracted_by_key.get(school_key) or {}
                    if isinstance(extracted_obj, dict):
                        sl = extracted_obj.get("social_links")
                        if not isinstance(sl, dict):
                            sl = extracted_obj.get("links")
                        if isinstance(sl, dict):
                            for k3, v3 in sl.items():
                                if not k3 or not v3:
                                    continue
                                social_links[str(k3).strip().lower()] = str(v3).strip()

                # Upsert core school row.
                cur.execute(
                    _arp_schema(
                        """
                        INSERT INTO "__ARP_SCHEMA__".schools
                          (school_key, name, homepage_url, primary_domain, last_crawled_at, tier, health_score, logo_url, emails, social_links, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, now())
                        ON CONFLICT (school_key) DO UPDATE SET
                          name=EXCLUDED.name,
                          homepage_url=EXCLUDED.homepage_url,
                          primary_domain=EXCLUDED.primary_domain,
                          last_crawled_at=EXCLUDED.last_crawled_at,
                          tier=EXCLUDED.tier,
                          health_score=EXCLUDED.health_score,
                          logo_url=EXCLUDED.logo_url,
                          emails=EXCLUDED.emails,
                          social_links=CASE
                            WHEN COALESCE("__ARP_SCHEMA__".schools.social_links,'{}'::jsonb)='{}'::jsonb
                              THEN EXCLUDED.social_links
                            ELSE "__ARP_SCHEMA__".schools.social_links
                          END,
                          updated_at=now();
                        """
                    ).strip(),
                    (
                        school_key,
                        name,
                        homepage_url,
                        primary_domain,
                        last_crawled_at,
                        tier,
                        int(health_score),
                        logo_url,
                        json.dumps(emails),
                        json.dumps(social_links),
                    ),
                )
                schools_count += 1

                md_text = evidence_by_key.get(school_key)
                st = existing_state.get(school_key) or {}
                if md_text and (not has_any or not st.get("has_evidence")):
                    cur.execute(
                        _arp_schema(
                            """
                            INSERT INTO "__ARP_SCHEMA__".school_evidence (school_key, evidence_markdown, evidence_generated_at)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (school_key) DO UPDATE SET
                              evidence_markdown=EXCLUDED.evidence_markdown,
                              evidence_generated_at=EXCLUDED.evidence_generated_at;
                            """
                        ).strip(),
                        (school_key, md_text, last_crawled_at),
                    )
                    evidence_count += 1

                llm_obj = llm_by_key.get(school_key) or {}
                if isinstance(llm_obj, dict) and llm_obj and (not has_any or not st.get("has_llm")):
                    result = llm_obj.get("result") if isinstance(llm_obj.get("result"), dict) else {}
                    narrative = str((result or {}).get("trip_overview_narrative") or "").strip()
                    overview_75w = str((result or {}).get("overview_75w") or "").strip()
                    if not overview_75w and narrative:
                        overview_75w = _truncate_words(narrative, 75)

                    model = str(llm_obj.get("model") or "").strip()
                    run_id = str(llm_obj.get("run_id") or llm_obj.get("runId") or "").strip()

                    def _to_int(v) -> int:
                        try:
                            return int(v or 0)
                        except Exception:
                            try:
                                return int(float(v or 0))
                            except Exception:
                                return 0

                    def _to_float(v) -> float:
                        try:
                            return float(v or 0)
                        except Exception:
                            return 0.0

                    tokens_in = _to_int(llm_obj.get("tokens_in") or llm_obj.get("tokensIn") or llm_obj.get("prompt_tokens"))
                    tokens_out = _to_int(llm_obj.get("tokens_out") or llm_obj.get("tokensOut") or llm_obj.get("completion_tokens"))
                    tokens_total = _to_int(llm_obj.get("tokens_total") or llm_obj.get("tokensTotal") or llm_obj.get("total_tokens"))
                    cost_usd = _to_float(llm_obj.get("cost_usd") or llm_obj.get("costUsd"))

                    cur.execute(
                        _arp_schema(
                            """
                            INSERT INTO "__ARP_SCHEMA__".school_overviews
                              (school_key, overview_75w, narrative, model, run_id, tokens_in, tokens_out, tokens_total, cost_usd, extracted_at, llm_json)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                            ON CONFLICT (school_key) DO UPDATE SET
                              overview_75w=EXCLUDED.overview_75w,
                              narrative=EXCLUDED.narrative,
                              model=EXCLUDED.model,
                              run_id=EXCLUDED.run_id,
                              tokens_in=EXCLUDED.tokens_in,
                              tokens_out=EXCLUDED.tokens_out,
                              tokens_total=EXCLUDED.tokens_total,
                              cost_usd=EXCLUDED.cost_usd,
                              extracted_at=EXCLUDED.extracted_at,
                              llm_json=EXCLUDED.llm_json;
                            """
                        ).strip(),
                        (
                            school_key,
                            overview_75w,
                            narrative,
                            model,
                            run_id,
                            tokens_in,
                            tokens_out,
                            tokens_total,
                            cost_usd,
                            extracted_at,
                            json.dumps(llm_obj),
                        ),
                    )

                    progs_raw = (result or {}).get("programs") or []
                    program_names: list[str] = []
                    if isinstance(progs_raw, list):
                        for p in progs_raw:
                            if isinstance(p, str) and p.strip():
                                program_names.append(p.strip())
                            elif isinstance(p, dict):
                                nm = str(p.get("program_name") or p.get("name") or "").strip()
                                if nm:
                                    program_names.append(nm)
                    program_names = sorted({x for x in program_names if x})

                    if program_names:
                        for nm in program_names:
                            cur.execute(
                                _arp_schema(
                                    """
                                    INSERT INTO "__ARP_SCHEMA__".school_trip_programs (school_key, program_name, source, extracted_at)
                                    VALUES (%s, %s, 'llm', %s)
                                    ON CONFLICT (school_key, program_name) DO NOTHING;
                                    """
                                ).strip(),
                                (school_key, nm, extracted_at),
                            )

                    json_count += 1

            conn.commit()
    print(f"[schools] bootstrapped: schools={schools_count} evidence={evidence_count} json={json_count}")


def _safe_school_key(school_key: str) -> str:
    school_key = (school_key or "").strip()
    if not school_key:
        raise HTTPException(status_code=400, detail="Missing school_key")
    if "/" in school_key or "\\" in school_key:
        raise HTTPException(status_code=400, detail="Invalid school_key")
    if len(school_key) > 200:
        raise HTTPException(status_code=400, detail="school_key too long")
    return school_key


def _parse_dt_maybe(s: str) -> datetime | None:
    s = (s or "").strip()
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _truncate_words(s: str, max_words: int) -> str:
    s = str(s or "").strip()
    if not s:
        return ""
    words = s.split()
    if len(words) <= max_words:
        return s
    return " ".join(words[:max_words]).strip()


def _has_column(cur: psycopg.Cursor, *, schema: str, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s AND column_name=%s
        LIMIT 1;
        """.strip(),
        (schema, table, column),
    )
    return cur.fetchone() is not None


@app.get("/arp/api/schools")
def arp_api_schools_redirect(request: Request) -> Response:
    _ = request
    return RedirectResponse(url="/schools/api/list", status_code=307)


@app.get("/schools/api/list")
def schools_api_list(
    request: Request,
    include_all: bool = Query(default=False),
) -> Response:
    _ = request
    schools: list[dict[str, Any]] = []
    fallback_used = False
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                _ensure_arp_tables(cur)
                schema = _require_safe_ident("ARP_SCHEMA", ARP_SCHEMA)
                social_dir = _schools_static_dir() / "social_links"
                social_cache: dict[str, dict[str, str]] = {}
                cur.execute("SELECT to_regclass(%s);", (f"{schema}.schools",))
                has_schools = cur.fetchone()[0] is not None  # type: ignore[index]
                if not has_schools:
                    return JSONResponse(
                        status_code=500,
                        headers={"Cache-Control": "no-store"},
                        content={
                            "ok": False,
                            "error": "Schools tables are missing in the ARP schema.",
                            "hint": "This usually means migrations did not apply on Render. Check /health/db output and Render deploy status.",
                        },
                    )
                has_llm_json = _has_column(cur, schema=schema, table="school_overviews", column="llm_json")
                llm_json_expr = "COALESCE(o.llm_json,'{}'::jsonb)" if has_llm_json else "'{}'::jsonb"

                where = ""
                if not include_all:
                    where = "WHERE s.school_key <> 'american_international_school_vienna' AND lower(trim(s.tier)) IN ('healthy','partial')"
                else:
                    where = "WHERE s.school_key <> 'american_international_school_vienna'"

                def run_query(where_sql: str) -> list[tuple[Any, ...]]:
                    cur.execute(
                        _arp_schema(
                            f"""
                            SELECT
                              s.school_key,
                              s.name,
                              s.homepage_url,
                              s.primary_domain,
                              s.last_crawled_at,
                              s.tier,
                              s.health_score,
                              COALESCE(s.social_links, '{{}}'::jsonb) AS social_links,
                              COALESCE(o.overview_75w, '') AS overview_75w,
                              COALESCE(o.narrative, '') AS narrative,
                              (o.school_key IS NOT NULL AND ({llm_json_expr} <> '{{}}'::jsonb OR COALESCE(o.narrative,'') <> '' OR COALESCE(o.overview_75w,'') <> '')) AS has_llm,
                              COALESCE(p.programs, ARRAY[]::text[]) AS programs,
                              (e.school_key IS NOT NULL AND COALESCE(e.evidence_markdown,'') <> '') AS has_evidence
                            FROM "__ARP_SCHEMA__".schools s
                            LEFT JOIN "__ARP_SCHEMA__".school_overviews o ON o.school_key = s.school_key
                            LEFT JOIN (
                              SELECT school_key, array_agg(program_name ORDER BY program_name) AS programs
                              FROM "__ARP_SCHEMA__".school_trip_programs
                              GROUP BY school_key
                            ) p ON p.school_key = s.school_key
                            LEFT JOIN "__ARP_SCHEMA__".school_evidence e ON e.school_key = s.school_key
                            {where_sql}
                            ORDER BY s.health_score DESC, s.name ASC;
                            """
                        ).strip()
                    )
                    return list(cur.fetchall() or [])

                rows = run_query(where)
                if (not include_all) and not rows:
                    fallback_used = True
                    rows = run_query("WHERE s.school_key <> 'american_international_school_vienna'")

                for row in rows:
                    (
                        school_key,
                        name,
                        homepage_url,
                        primary_domain,
                        last_crawled_at,
                        tier,
                        health_score,
                        social_links,
                        overview_75w,
                        narrative,
                        has_llm,
                        programs,
                        has_evidence,
                    ) = row

                    # Opportunistic backfill: if social_links are missing in DB, read from bundled JSON and persist.
                    social_obj: dict[str, str] = social_links if isinstance(social_links, dict) else {}
                    if not social_obj and social_dir.exists():
                        sk = str(school_key or "").strip()
                        if sk:
                            if sk not in social_cache:
                                p = social_dir / f"{sk}.json"
                                links: dict[str, str] = {}
                                if p.exists():
                                    try:
                                        raw = json.loads(p.read_text(encoding="utf-8", errors="replace"))
                                        if isinstance(raw, dict):
                                            raw_links = raw.get("links")
                                            if not isinstance(raw_links, dict):
                                                raw_links = raw.get("social_links")
                                            if isinstance(raw_links, dict):
                                                for k, v in (raw_links or {}).items():
                                                    kk = str(k or "").strip().lower()
                                                    vv = str(v or "").strip()
                                                    if kk and vv:
                                                        links[kk] = vv
                                    except Exception:
                                        links = {}
                                social_cache[sk] = links
                            links2 = social_cache.get(sk) or {}
                            if links2:
                                social_obj = links2
                                cur.execute(
                                    _arp_schema(
                                        """
                                        UPDATE "__ARP_SCHEMA__".schools
                                        SET social_links=%s::jsonb, updated_at=now()
                                        WHERE school_key=%s AND (COALESCE(social_links,'{}'::jsonb)='{}'::jsonb);
                                        """
                                    ).strip(),
                                    (json.dumps(links2), sk),
                                )

                    review = str(overview_75w or "").strip()
                    if not review:
                        review = _truncate_words(str(narrative or "").strip(), 75)
                    schools.append(
                        {
                            "school_key": str(school_key),
                            "name": str(name or ""),
                            "homepage_url": str(homepage_url or ""),
                            "primary_domain": str(primary_domain or ""),
                            "last_crawled_at": last_crawled_at.isoformat() if last_crawled_at else None,
                            "tier": str(tier or ""),
                            "health_score": int(health_score or 0),
                            "social_links": social_obj,
                            "review": review,
                            "programs": list(programs or []),
                            "has_evidence": bool(has_evidence),
                            "has_llm": bool(has_llm),
                        }
                    )
            conn.commit()
        return JSONResponse(
            headers={"Cache-Control": "no-store"},
            content={"ok": True, "schools": schools, "fallback_used": fallback_used},
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            headers={"Cache-Control": "no-store"},
            content={
                "ok": False,
                "error": "Failed to load schools.",
                "detail": str(getattr(e, "detail", e)),
                "hint": "If this mentions missing relations, migrations may not have applied. Check /health/db.",
            },
        )


@app.get("/arp/schools/{school_key}", response_class=HTMLResponse)
def arp_school_detail_ui_redirect(school_key: str, request: Request) -> Response:
    _ = request
    return RedirectResponse(url=f"/schools/{quote(_safe_school_key(school_key))}", status_code=307)


@app.get("/schools/{school_key}", response_class=HTMLResponse)
def school_detail_ui(
    school_key: str,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="viewer") or {}
    key = _safe_school_key(school_key)
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_arp_tables(cur)
            cur.execute(
                _arp_schema(
                    """
                    SELECT name, homepage_url, primary_domain, last_crawled_at, tier, health_score, emails, social_links
                    FROM "__ARP_SCHEMA__".schools
                    WHERE school_key=%s;
                    """
                ).strip(),
                (key,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Unknown school")
            name, homepage_url, primary_domain, last_crawled_at, tier, health_score, emails, social_links = row

            cur.execute(
                _arp_schema('SELECT program_name FROM "__ARP_SCHEMA__".school_trip_programs WHERE school_key=%s ORDER BY program_name ASC;'),
                (key,),
            )
            programs = [str(r[0]) for r in (cur.fetchall() or []) if r and r[0]]

            cur.execute(
                _arp_schema(
                    """
                    SELECT overview_75w, narrative, model, run_id, tokens_in, tokens_out, tokens_total, cost_usd, extracted_at
                    FROM "__ARP_SCHEMA__".school_overviews
                    WHERE school_key=%s;
                    """
                ).strip(),
                (key,),
            )
            o = cur.fetchone()
            overview_75w = ""
            narrative = ""
            model = ""
            run_id = ""
            tokens_in = 0
            tokens_out = 0
            tokens_total = 0
            cost_usd = 0.0
            extracted_at = None
            if o:
                (
                    overview_75w,
                    narrative,
                    model,
                    run_id,
                    tokens_in,
                    tokens_out,
                    tokens_total,
                    cost_usd,
                    extracted_at,
                ) = o

            cur.execute(
                _arp_schema('SELECT evidence_markdown, evidence_generated_at FROM "__ARP_SCHEMA__".school_evidence WHERE school_key=%s;'),
                (key,),
            )
            e = cur.fetchone()
            evidence_md = ""
            evidence_generated_at = None
            if e:
                evidence_md, evidence_generated_at = e
        conn.commit()

    prog_html = "<span class=\"muted\">—</span>"
    if programs:
        prog_html = "<ul style=\"margin: 8px 0 0 18px; padding: 0;\">" + "".join(
            f"<li>{_escape_html(p)}</li>" for p in programs
        ) + "</ul>"

    emails_obj = emails if isinstance(emails, dict) else {}
    emails_items = []
    for k2, label in [
        ("general_email", "General"),
        ("admissions_email", "Admissions"),
        ("communications_email", "Communications"),
    ]:
        v = (emails_obj.get(k2) or "").strip() if isinstance(emails_obj, dict) else ""
        if v:
            emails_items.append(f"<div><span class=\"pill\">{_escape_html(label)}</span> <span class=\"mono\">{_escape_html(v)}</span></div>")
    emails_html = "".join(emails_items) if emails_items else "<span class=\"muted\">—</span>"

    social_obj = social_links if isinstance(social_links, dict) else {}
    social_items = []
    for k2, v2 in (social_obj or {}).items() if isinstance(social_obj, dict) else []:
        k2s = str(k2 or "").strip()
        v2s = str(v2 or "").strip()
        if not k2s or not v2s:
            continue
        social_items.append(
            f'<div><span class="pill">{_escape_html(k2s)}</span> <a href="{_escape_html(v2s)}" target="_blank" rel="noopener">{_escape_html(v2s)}</a></div>'
        )
    social_html = "".join(social_items) if social_items else "<span class=\"muted\">—</span>"

    home_link = ""
    if homepage_url:
        home_link = f'<a class="btn" href="{_escape_html(str(homepage_url))}" target="_blank" rel="noopener">Homepage</a>'
    llm_link = f'<a class="btn" href="/schools/{quote(key)}/llm">LLM</a>'

    evidence_html = ""
    if evidence_md:
        evidence_html = _render_markdown_safe(str(evidence_md or ""))
    else:
        evidence_html = '<p class="muted">No evidence markdown imported yet.</p>'

    updated_parts = []
    if last_crawled_at:
        updated_parts.append(f"Last crawled: {last_crawled_at}")
    if extracted_at:
        updated_parts.append(f"LLM extracted: {extracted_at}")
    if evidence_generated_at:
        updated_parts.append(f"Evidence generated: {evidence_generated_at}")
    updated_html = " · ".join(updated_parts) if updated_parts else ""

    usage_html = ""
    if int(tokens_total or 0) > 0 or float(cost_usd or 0) > 0:
        usage_html = (
            f"<div class=\"muted\">Model: <span class=\"mono\">{_escape_html(str(model or ''))}</span>"
            f"{(' · Run: <span class=\"mono\">' + _escape_html(str(run_id or '')) + '</span>') if run_id else ''}"
            f" · Tokens: <span class=\"mono\">{int(tokens_in or 0)}/{int(tokens_out or 0)}/{int(tokens_total or 0)}</span>"
            f" · Cost: <span class=\"mono\">${float(cost_usd or 0):.4f}</span></div>"
        )

    key_html = f' · <span class="mono">{_escape_html(key)}</span>' if key else ""
    overview_html = _escape_html(str(overview_75w or "")) if overview_75w else '<span class="muted">—</span>'
    narrative_html = (
        f'<div class="section"><h2>Narrative</h2><pre class="log">{_escape_html(str(narrative or ""))}</pre></div>'
        if narrative
        else ""
    )

    body_html = f"""
      <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:baseline; gap:12px; flex-wrap:wrap;">
          <h1>{_escape_html(str(name or key))}</h1>
          <div class="btnrow" style="margin-top:0;">
            <a class="btn" href="/schools">Back</a>
            {home_link}
            {llm_link}
          </div>
        </div>
        <p class="muted">
          <span class="pill">{_escape_html(str(tier or ''))}</span>
          <span class="mono">score {int(health_score or 0)}</span>
          {(' · ' + _escape_html(str(primary_domain or ''))) if primary_domain else ''}
          {key_html}
        </p>
        <p class="muted">{_escape_html(updated_html) if updated_html else ''}</p>
      </div>

      <div class="card">
        <h2>Programs</h2>
        {prog_html}
      </div>

      <div class="card">
        <h2>Overview</h2>
        <p>{overview_html}</p>
        {usage_html}
        {narrative_html}
      </div>

      <div class="card">
        <h2>Emails</h2>
        {emails_html}
      </div>

      <div class="card">
        <h2>Social</h2>
        {social_html}
      </div>

      <div class="card">
        <h2>Evidence</h2>
        {evidence_html}
      </div>
    """.strip()
    return _ui_shell(title=f"School — {str(name or key)}", active="apps", body_html=body_html, max_width_px=1100, user=user)


@app.get("/schools/{school_key}/llm", response_class=HTMLResponse)
def school_llm_ui(
    school_key: str,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="viewer") or {}
    key = _safe_school_key(school_key)
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_arp_tables(cur)
            schema = _require_safe_ident("ARP_SCHEMA", ARP_SCHEMA)
            has_llm_json = _has_column(cur, schema=schema, table="school_overviews", column="llm_json")

            cur.execute(
                _arp_schema('SELECT name FROM "__ARP_SCHEMA__".schools WHERE school_key=%s;'),
                (key,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Unknown school")
            (name,) = row

            llm_col = "llm_json" if has_llm_json else "'{}'::jsonb AS llm_json"
            cur.execute(
                _arp_schema(
                    f"""
                    SELECT overview_75w, narrative, model, run_id, tokens_in, tokens_out, tokens_total, cost_usd, extracted_at, {llm_col}
                    FROM "__ARP_SCHEMA__".school_overviews
                    WHERE school_key=%s;
                    """
                ).strip(),
                (key,),
            )
            row2 = cur.fetchone()
            if not row2:
                raise HTTPException(status_code=404, detail="No LLM output found for this school")
            (
                overview_75w,
                narrative,
                model,
                run_id,
                tokens_in,
                tokens_out,
                tokens_total,
                cost_usd,
                extracted_at,
                llm_json,
            ) = row2
        conn.commit()

    usage_bits = []
    if model:
        usage_bits.append(f"Model: {model}")
    if run_id:
        usage_bits.append(f"Run: {run_id}")
    if extracted_at:
        usage_bits.append(f"Extracted: {extracted_at}")
    if int(tokens_total or 0) > 0:
        usage_bits.append(f"Tokens: {int(tokens_in or 0)}/{int(tokens_out or 0)}/{int(tokens_total or 0)}")
    if float(cost_usd or 0) > 0:
        usage_bits.append(f"Cost: ${float(cost_usd or 0):.4f}")
    usage_line = " · ".join(usage_bits)

    overview_html = _escape_html(str(overview_75w or "")) if overview_75w else "<span class=\"muted\">—</span>"
    narrative_html = _escape_and_linkify(str(narrative or "")) if narrative else ""
    extracted_html = _render_llm_json_readable(llm_json or {})

    body_html = f"""
      <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:baseline; gap:12px; flex-wrap:wrap;">
          <h1>LLM output</h1>
          <div class="btnrow" style="margin-top:0;">
            <a class="btn" href="/schools/{quote(key)}">Back</a>
          </div>
        </div>
        <p class="muted">{_escape_html(str(name or key))} · <span class="mono">{_escape_html(key)}</span></p>
        <p class="muted">{_escape_html(usage_line)}</p>
      </div>

      <style>
        /* Avoid horizontal scroll on this page: prefer wrapping everywhere. */
        .llm-wrap, .llm-wrap * {{ word-break: break-word; }}
        .llm-wrap ul {{ padding-left: 18px; margin: 6px 0; }}
        .llm-wrap li {{ margin: 4px 0; }}
        .llm-wrap h3 {{ margin: 12px 0 6px; font-size: 14px; }}
      </style>

      <div class="card">
        <h2>Review</h2>
        <p>{overview_html}</p>
        {f'<div class=\"section llm-wrap\"><h2>Narrative</h2><div style=\"white-space:pre-wrap;\">{narrative_html}</div></div>' if narrative_html else ''}
      </div>

      <div class="card">
        <h2>Extracted details</h2>
        <div class="llm-wrap">{extracted_html}</div>
      </div>
    """.strip()
    return _ui_shell(title=f"LLM — {str(name or key)}", active="apps", body_html=body_html, max_width_px=1100, user=user)


def _parse_csv_bytes(raw: bytes) -> tuple[list[str], list[dict[str, str]]]:
    text = (raw or b"").decode("utf-8-sig", errors="replace")
    f = StringIO(text)
    reader = csv.DictReader(f)
    fieldnames = list(reader.fieldnames or [])
    rows: list[dict[str, str]] = []
    for r in reader:
        if not isinstance(r, dict):
            continue
        rows.append({str(k): ("" if v is None else str(v)).strip() for k, v in r.items() if k})
    return fieldnames, rows


_CSV_KEY_CLEAN_RE = re.compile(r"[^a-z0-9]+")
_NAME_CLEAN_RE = re.compile(r"[^a-z0-9]+")


def _norm_csv_key(key: str) -> str:
    return _CSV_KEY_CLEAN_RE.sub("", str(key or "").strip().lower())


def _norm_row(row: dict[str, str]) -> dict[str, str]:
    return {_norm_csv_key(k): ("" if v is None else str(v)).strip() for k, v in (row or {}).items() if k}


def _row_get(nrow: dict[str, str], *keys: str) -> str:
    for k in keys:
        v = (nrow.get(_norm_csv_key(k)) or "").strip()
        if v:
            return v
    return ""


def _norm_name(s: str) -> str:
    return _NAME_CLEAN_RE.sub(" ", str(s or "").strip().lower()).strip()


def _arp_import_rows(
    *,
    activity_rows: list[dict[str, str]],
    research_headers: list[str],
    research_rows: list[dict[str, str]],
) -> tuple[int, int]:
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_arp_tables(cur)

            # Activities
            activities_count = 0
            for r in activity_rows:
                nr = _norm_row(r)
                aid_raw = _row_get(nr, "ActivityID", "Activity Id", "Activity ID", "ActivityId", "ID", "Activity #")
                if not aid_raw:
                    continue
                try:
                    aid = int(float(aid_raw))
                except Exception:
                    continue
                name = _row_get(nr, "Activity Name", "Activity", "Name", "ActivityName")
                if not name:
                    continue
                slug = _stable_slug(name, max_len=64)
                category = _row_get(nr, "Activity Category", "Category", "ActivityCategory")
                scope = _row_get(nr, "Context / Scope Notes", "Context/Scope Notes", "Scope Notes", "Context", "Scope")
                status = _row_get(nr, "Status", "Activity Status")

                cur.execute(
                    _arp_schema(
                        """
                        INSERT INTO "__ARP_SCHEMA__".activities
                          (activity_id, activity_slug, activity_name, activity_category, scope_notes, status)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (activity_id) DO UPDATE SET
                          activity_slug=EXCLUDED.activity_slug,
                          activity_name=EXCLUDED.activity_name,
                          activity_category=EXCLUDED.activity_category,
                          scope_notes=EXCLUDED.scope_notes,
                          status=EXCLUDED.status,
                          updated_at=now();
                        """
                    ).strip(),
                    (aid, slug, name, category, scope, status),
                )
                activities_count += 1

            # Research / sources
            sources_count = 0
            cur.execute(_arp_schema('SELECT activity_id, activity_name FROM "__ARP_SCHEMA__".activities;'))
            activity_name_to_id: dict[str, int] = {}
            for aid, nm in cur.fetchall() or []:
                if aid is None or not nm:
                    continue
                activity_name_to_id[_norm_name(str(nm))] = int(aid)

            for r in research_rows:
                nr = _norm_row(r)

                aid_raw = _row_get(nr, "ActivityID", "Activity Id", "Activity ID", "ActivityId", "ID", "Activity #")
                activity_name = _row_get(nr, "Activity", "Activity Name", "ActivityName")
                if not aid_raw and not activity_name:
                    continue
                try:
                    aid = int(float(aid_raw)) if aid_raw else None
                except Exception:
                    aid = None
                url = _row_get(nr, "URL", "Url", "Link", "Resource URL", "Source URL", "Resource")
                if not url:
                    continue

                if aid is None and activity_name:
                    aid = activity_name_to_id.get(_norm_name(activity_name))
                if aid is None:
                    continue

                # Ensure activity exists (some imports may only include research CSV).
                cur.execute(_arp_schema('SELECT 1 FROM "__ARP_SCHEMA__".activities WHERE activity_id=%s;'), (aid,))
                exists = cur.fetchone() is not None
                if not exists:
                    name = activity_name or f"Activity {aid}"
                    slug = _stable_slug(name, max_len=64)
                    cur.execute(
                        _arp_schema(
                            """
                            INSERT INTO "__ARP_SCHEMA__".activities
                              (activity_id, activity_slug, activity_name)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (activity_id) DO NOTHING;
                            """
                        ).strip(),
                        (aid, slug, name),
                    )

                title = _row_get(nr, "Title", "Resource Title", "Document Title")
                org = _row_get(nr, "Organization / Publisher", "Organization", "Publisher")
                jurisdiction = _row_get(nr, "Country / Jurisdiction", "Jurisdiction", "Country")
                source_type = _row_get(nr, "Source type", "Source Type", "Type")
                activities_covered_raw = _row_get(nr, "Activities covered", "Activities Covered")

                brief = _row_get(
                    nr,
                    "Brief focus (1–2 lines)",
                    "Brief focus (1-2 lines)",
                    "Brief focus",
                    "Brief Focus",
                )
                if not brief:
                    # Handle encoding/variant headers by scanning raw headers for "brief focus"
                    for k in research_headers:
                        if _norm_csv_key(k).startswith("brieffocus"):
                            brief = (r.get(k) or "").strip()
                            if brief:
                                break

                authority_class = _row_get(nr, "Authority class (A/B/C)", "Authority class", "Authority Class")
                publication_date = _row_get(
                    nr,
                    "Publication date (YYYY-MM-DD or YYYY)",
                    "Publication date",
                    "Publication Date",
                    "Date",
                )

                src_id = _stable_slug(activity_name or f"activity-{aid}", org, title, max_len=90)

                cur.execute(
                    _arp_schema(
                        """
                        INSERT INTO "__ARP_SCHEMA__".sources
                          (source_id, activity_id, activity_name, title, organization, jurisdiction, url, source_type,
                           activities_covered_raw, brief_focus, authority_class, publication_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source_id) DO UPDATE SET
                          activity_id=EXCLUDED.activity_id,
                          activity_name=EXCLUDED.activity_name,
                          title=EXCLUDED.title,
                          organization=EXCLUDED.organization,
                          jurisdiction=EXCLUDED.jurisdiction,
                          url=EXCLUDED.url,
                          source_type=EXCLUDED.source_type,
                          activities_covered_raw=EXCLUDED.activities_covered_raw,
                          brief_focus=EXCLUDED.brief_focus,
                          authority_class=EXCLUDED.authority_class,
                          publication_date=EXCLUDED.publication_date;
                        """
                    ).strip(),
                    (
                        src_id,
                        aid,
                        activity_name,
                        title,
                        org,
                        jurisdiction,
                        url,
                        source_type,
                        activities_covered_raw,
                        brief,
                        authority_class,
                        publication_date,
                    ),
                )
                cur.execute(
                    _arp_schema(
                        """
                        INSERT INTO "__ARP_SCHEMA__".documents (source_id)
                        VALUES (%s)
                        ON CONFLICT (source_id) DO NOTHING;
                        """
                    ).strip(),
                    (src_id,),
                )
                sources_count += 1

        conn.commit()
    return activities_count, sources_count


@app.post("/arp/api/create")
def arp_api_create(
    body: ArpCreateIn,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_write_access(request=request, x_api_key=x_api_key, role="editor")
    activity_name = (body.activity_name or "").strip()
    if not activity_name:
        raise HTTPException(status_code=400, detail="Missing activity_name")

    category = (body.activity_category or "").strip()
    scope = (body.scope_notes or "").strip()
    status = (body.status or "active").strip()

    urls_raw = (body.resource_urls or "").strip()
    parts = [p.strip() for p in re.split(r"[,\n\r]+", urls_raw) if p and p.strip()]
    # de-dupe while preserving order
    urls: list[str] = []
    seen = set()
    for u in parts:
        if u in seen:
            continue
        seen.add(u)
        urls.append(u)

    slug = _stable_slug(activity_name, max_len=64)
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_arp_tables(cur)
            cur.execute(_arp_schema('SELECT COALESCE(MAX(activity_id), 0) + 1 FROM "__ARP_SCHEMA__".activities;'))
            (new_id,) = cur.fetchone() or (1,)
            activity_id = int(new_id or 1)

            cur.execute(
                _arp_schema(
                    """
                    INSERT INTO "__ARP_SCHEMA__".activities
                      (activity_id, activity_slug, activity_name, activity_category, scope_notes, status)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (activity_id) DO UPDATE SET
                      activity_slug=EXCLUDED.activity_slug,
                      activity_name=EXCLUDED.activity_name,
                      activity_category=EXCLUDED.activity_category,
                      scope_notes=EXCLUDED.scope_notes,
                      status=EXCLUDED.status,
                      updated_at=now();
                    """
                ).strip(),
                (activity_id, slug, activity_name, category, scope, status),
            )

            sources_count = 0
            for url in urls:
                if not url:
                    continue
                src_id = _stable_slug(activity_name, url, max_len=90)
                cur.execute(
                    _arp_schema(
                        """
                        INSERT INTO "__ARP_SCHEMA__".sources
                          (source_id, activity_id, activity_name, title, organization, jurisdiction, url, source_type,
                           activities_covered_raw, brief_focus, authority_class, publication_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source_id) DO UPDATE SET
                          activity_id=EXCLUDED.activity_id,
                          activity_name=EXCLUDED.activity_name,
                          url=EXCLUDED.url,
                          source_type=EXCLUDED.source_type;
                        """
                    ).strip(),
                    (src_id, activity_id, activity_name, "", "", "", url, "manual", "", "", "", ""),
                )
                cur.execute(
                    _arp_schema(
                        """
                        INSERT INTO "__ARP_SCHEMA__".documents (source_id)
                        VALUES (%s)
                        ON CONFLICT (source_id) DO NOTHING;
                        """
                    ).strip(),
                    (src_id,),
                )
                sources_count += 1

        conn.commit()

    return {"ok": True, "activity_id": activity_id, "activity_slug": slug, "sources": sources_count}


@app.post("/arp/import")
def arp_import(
    request: Request,
    activities_csv: UploadFile = File(...),
    research_csv: UploadFile | None = File(default=None),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_write_access(request=request, x_api_key=x_api_key, role="editor")

    activities_raw = activities_csv.file.read()
    research_raw = b""
    if research_csv is not None:
        research_raw = research_csv.file.read()

    _, activity_rows = _parse_csv_bytes(activities_raw)
    headers, research_rows = _parse_csv_bytes(research_raw)

    activities_count, sources_count = _arp_import_rows(
        activity_rows=activity_rows,
        research_headers=headers,
        research_rows=research_rows,
    )

    return {"ok": True, "activities": activities_count, "sources": sources_count}


@app.post("/arp/import_from_repo")
def arp_import_from_repo(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    """
    Convenience import for the bundled CSVs in the repo/container:
      api/app/static/arp_data/activities.csv
      api/app/static/arp_data/research.csv (optional)
    """
    _require_write_access(request=request, x_api_key=x_api_key, role="editor")

    base = _STATIC_DIR / "arp_data"
    activities_path = base / "activities.csv"
    research_path = base / "research.csv"
    if not activities_path.exists():
        raise HTTPException(status_code=404, detail="Missing bundled activities.csv")

    activities_raw = activities_path.read_bytes()
    research_raw = research_path.read_bytes() if research_path.exists() else b""

    _, activity_rows = _parse_csv_bytes(activities_raw)
    headers, research_rows = _parse_csv_bytes(research_raw)
    activities_count, sources_count = _arp_import_rows(
        activity_rows=activity_rows,
        research_headers=headers,
        research_rows=research_rows,
    )
    return {
        "ok": True,
        "activities": activities_count,
        "sources": sources_count,
        "source": "repo",
        "paths": {"activities": str(activities_path), "research": str(research_path)},
    }


@app.get("/arp/api/activities")
def arp_api_activities(request: Request) -> dict[str, Any]:
    _ = request
    out: list[dict[str, Any]] = []
    bootstrapped = False
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_arp_tables(cur)
            # Optional bootstrap: if DB is empty, import the bundled repo CSVs once.
            # This avoids confusion where the UI shows 0–2 items because no import was run yet.
            v = os.environ.get("ARP_BOOTSTRAP_FROM_REPO", "").strip().lower()
            if v in {"1", "true", "yes", "on"}:
                cur.execute(_arp_schema('SELECT COUNT(*) FROM "__ARP_SCHEMA__".activities;'))
                (cnt,) = cur.fetchone() or (0,)
                if int(cnt or 0) == 0:
                    base = _STATIC_DIR / "arp_data"
                    activities_path = base / "activities.csv"
                    research_path = base / "research.csv"
                    if activities_path.exists():
                        activities_raw = activities_path.read_bytes()
                        research_raw = research_path.read_bytes() if research_path.exists() else b""
                        _, activity_rows = _parse_csv_bytes(activities_raw)
                        headers, research_rows = _parse_csv_bytes(research_raw)
                        _arp_import_rows(
                            activity_rows=activity_rows,
                            research_headers=headers,
                            research_rows=research_rows,
                        )
                        bootstrapped = True

            cur.execute(
                _arp_schema(
                    """
                    SELECT
                      a.activity_id,
                      a.activity_slug,
                      a.activity_name,
                      a.activity_category,
                      a.scope_notes,
                      a.status,
                      COALESCE(s.sources_count, 0) AS sources_count,
                      COALESCE(d.docs_fetched, 0) AS docs_fetched,
                      COALESCE(d.docs_total, 0) AS docs_total,
                      COALESCE(c.chunks_count, 0) AS chunks_count,
                      (r.activity_id IS NOT NULL) AS has_report
                    FROM "__ARP_SCHEMA__".activities a
                    LEFT JOIN (
                      SELECT activity_id, COUNT(*) AS sources_count
                      FROM "__ARP_SCHEMA__".sources
                      GROUP BY activity_id
                    ) s ON s.activity_id = a.activity_id
                    LEFT JOIN (
                      SELECT
                        s.activity_id,
                        SUM(CASE WHEN d.status='fetched' THEN 1 ELSE 0 END) AS docs_fetched,
                        COUNT(*) AS docs_total
                      FROM "__ARP_SCHEMA__".sources s
                      LEFT JOIN "__ARP_SCHEMA__".documents d ON d.source_id = s.source_id
                      GROUP BY s.activity_id
                    ) d ON d.activity_id = a.activity_id
                    LEFT JOIN (
                      SELECT activity_id, COUNT(*) AS chunks_count
                      FROM "__ARP_SCHEMA__".chunks
                      GROUP BY activity_id
                    ) c ON c.activity_id = a.activity_id
                    LEFT JOIN "__ARP_SCHEMA__".reports r ON r.activity_id = a.activity_id
                    ORDER BY a.activity_name ASC;
                    """
                ).strip()
            )
            for row in cur.fetchall():
                (
                    activity_id,
                    activity_slug,
                    activity_name,
                    activity_category,
                    scope_notes,
                    status,
                    sources_count,
                    docs_fetched,
                    docs_total,
                    chunks_count,
                    has_report,
                ) = row
                docs_status = "missing"
                if int(docs_total or 0) == 0:
                    docs_status = "no_sources"
                elif int(docs_fetched or 0) == int(docs_total or 0):
                    docs_status = "ready"
                elif int(docs_fetched or 0) > 0:
                    docs_status = "partial"
                out.append(
                    {
                        "activity_id": int(activity_id),
                        "activity_slug": str(activity_slug),
                        "activity_name": str(activity_name),
                        "activity_category": str(activity_category or ""),
                        "scope_notes": str(scope_notes or ""),
                        "status": str(status or ""),
                        "sources_count": int(sources_count or 0),
                        "docs_status": docs_status,
                        "chunks_count": int(chunks_count or 0),
                        "has_report": bool(has_report),
                    }
                )
    return {"ok": True, "activities": out, "bootstrapped": bootstrapped}


@app.get("/arp/resources/{activity_id}", response_class=HTMLResponse)
def arp_resources_ui(
    activity_id: int,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="viewer") or {}
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_arp_tables(cur)
            cur.execute(_arp_schema('SELECT activity_name FROM "__ARP_SCHEMA__".activities WHERE activity_id=%s;'), (activity_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Unknown activity")
            (activity_name,) = row

            cur.execute(
                _arp_schema(
                    """
                    SELECT s.source_id, s.title, s.organization, s.jurisdiction, s.url, s.source_type, s.brief_focus,
                           d.status, d.s3_bucket, d.s3_key, d.content_type
                    FROM "__ARP_SCHEMA__".sources s
                    LEFT JOIN "__ARP_SCHEMA__".documents d ON d.source_id = s.source_id
                    WHERE s.activity_id=%s
                    ORDER BY s.title ASC;
                    """
                ).strip(),
                (activity_id,),
            )
            rows = list(cur.fetchall())

    s3cfg = None
    try:
        s3cfg = get_s3_config()
    except Exception:
        s3cfg = None

    trs = []
    for source_id, title, org, jur, url, source_type, brief, d_status, s3_bucket, s3_key, content_type in rows:
        raw_link = ""
        if s3cfg and s3_bucket and s3_key and str(d_status) == "fetched":
            try:
                raw_link = presign_get_inline(
                    region=s3cfg.region,
                    bucket=str(s3_bucket),
                    key=str(s3_key),
                    filename=f"{source_id}",
                    content_type=str(content_type or ""),
                    expires_in=3600,
                )
            except Exception:
                raw_link = ""
        raw_html = f'<a href="{raw_link}" target="_blank" rel="noopener">S3</a>' if raw_link else '<span class="muted">—</span>'
        src_html = f'<a href="{url}" target="_blank" rel="noopener">Source</a>' if url else '<span class="muted">—</span>'
        trs.append(
            "<tr>"
            f"<td class=\"mono\">{source_id}</td>"
            f"<td>{title}</td>"
            f"<td class=\"muted\">{org}</td>"
            f"<td class=\"muted\">{jur}</td>"
            f"<td><span class=\"pill\">{d_status}</span></td>"
            f"<td>{src_html} · {raw_html}</td>"
            "</tr>"
        )

    body_html = f"""
      <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:baseline; gap:12px; flex-wrap:wrap;">
          <h1>Resources</h1>
          <a class="btn" href="/arp/ui">Back</a>
        </div>
        <p class="muted">{activity_name}</p>
      </div>
      <div class="card">
        <div class="section tablewrap">
          <table>
            <thead><tr><th>Source ID</th><th>Title</th><th>Publisher</th><th>Jurisdiction</th><th>Fetched</th><th>Links</th></tr></thead>
            <tbody>{''.join(trs) if trs else '<tr><td colspan=\"6\" class=\"muted\">No sources.</td></tr>'}</tbody>
          </table>
        </div>
      </div>
    """.strip()
    return _ui_shell(title="ARP Resources", active="arp", body_html=body_html, max_width_px=1200, user=user)


@app.post("/arp/api/prepare")
def arp_prepare(
    body: ArpRunIn,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_write_access(request=request, x_api_key=x_api_key, role="editor")
    ids = [int(x) for x in (body.activity_ids or []) if int(x) > 0]
    if not ids:
        raise HTTPException(status_code=400, detail="Select at least one activity")
    job_id = _enqueue_job(kind="arp_prepare", payload={"activity_ids": ids})
    return {"ok": True, "job_id": job_id}


@app.post("/arp/api/generate")
def arp_generate(
    body: ArpRunIn,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_write_access(request=request, x_api_key=x_api_key, role="editor")
    ids = [int(x) for x in (body.activity_ids or []) if int(x) > 0]
    if not ids:
        raise HTTPException(status_code=400, detail="Select at least one activity")
    top_k = max(1, min(int(body.top_k or 12), 50))
    job_id = _enqueue_job(kind="arp_prepare_generate", payload={"activity_ids": ids, "top_k": top_k})
    return {"ok": True, "job_id": job_id}


@app.get("/arp/report/{activity_slug}", response_class=HTMLResponse)
def arp_report_ui(
    activity_slug: str,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="viewer") or {}
    slug = _slugify(activity_slug)
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_arp_tables(cur)
            cur.execute(
                _arp_schema(
                    """
                    SELECT a.activity_name, r.report_md, r.updated_at
                    FROM "__ARP_SCHEMA__".reports r
                    JOIN "__ARP_SCHEMA__".activities a ON a.activity_id = r.activity_id
                    WHERE r.activity_slug=%s;
                    """
                ).strip(),
                (slug,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Report not found")
            activity_name, report_md, updated_at = row
            cur.execute(
                _arp_schema(
                    """
                    SELECT svg
                    FROM "__ARP_SCHEMA__".activity_icons
                    WHERE activity_slug=%s;
                    """
                ).strip(),
                (slug,),
            )
            row2 = cur.fetchone()
            icon_svg = str(row2[0] or "") if row2 else ""

    html = _render_markdown_safe(str(report_md or ""))
    icon_html = icon_svg if icon_svg.strip().startswith("<svg") else ""
    body_html = f"""
      <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:baseline; gap:12px; flex-wrap:wrap;">
          <div style="display:flex; gap:10px; align-items:center;">
            <div style="width:28px; height:28px; display:flex; align-items:center; justify-content:center;">{icon_html}</div>
            <h1 style="margin:0;">ARP Report</h1>
          </div>
          <div class="btnrow" style="margin-top:0;">
            <a class="btn" href="/arp/ui">Activities</a>
            <a class="btn" href="/arp/report/{quote(slug)}/edit">Edit</a>
          </div>
        </div>
        <p class="muted">{activity_name} · Updated: {updated_at}</p>
      </div>
      <div class="card">
        {html}
      </div>
    """.strip()
    return _ui_shell(title="ARP Report", active="arp", body_html=body_html, max_width_px=900, user=user)


@app.get("/arp/report/{activity_slug}/edit", response_class=HTMLResponse)
def arp_report_edit_ui(
    activity_slug: str,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str:
    user = _require_access(request=request, x_api_key=x_api_key, role="viewer") or {}
    slug = _slugify(activity_slug)
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_arp_tables(cur)
            cur.execute(_arp_schema('SELECT report_md FROM "__ARP_SCHEMA__".reports WHERE activity_slug=%s;'), (slug,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Report not found")
            (report_md,) = row

    md_esc = (
        str(report_md or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )
    body_html = f"""
      <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:baseline; gap:12px; flex-wrap:wrap;">
          <h1>Edit ARP</h1>
          <div class="btnrow" style="margin-top:0;">
            <a class="btn" href="/arp/report/{quote(slug)}">View</a>
            <a class="btn" href="/arp/ui">Activities</a>
          </div>
        </div>
        <p class="muted">Saves update the report markdown stored in Postgres.</p>
      </div>
      <div class="card">
        <textarea id="md" class="mono">{md_esc}</textarea>
        <div class="btnrow">
          <button id="save" class="btn primary" type="button">Save</button>
          <span class="muted" id="status">Ready.</span>
        </div>
      </div>
    """.strip()
    script = f"""
    <script>
      const mdEl = document.getElementById('md');
      const statusEl = document.getElementById('status');
      document.getElementById('save').addEventListener('click', async () => {{
        statusEl.textContent = 'Saving…';
        const apiKey = String(localStorage.getItem('eti_x_api_key') || '').trim();
        const headers = {{ 'Content-Type':'application/json' }};
        if (apiKey) headers['X-API-Key'] = apiKey;
        const res = await fetch('/arp/report/{quote(slug)}', {{ method:'POST', headers, body: JSON.stringify({{ report_md: mdEl.value || '' }}) }});
        const body = await res.json().catch(() => ({{}}));
        if (!res.ok || !body.ok) {{ statusEl.textContent = body.detail || body.error || `HTTP ${{res.status}}`; return; }}
        statusEl.textContent = 'Saved.';
      }});
    </script>
    """.strip()
    return _ui_shell(title="Edit ARP", active="arp", body_html=body_html, max_width_px=900, extra_script=script, user=user)


@app.post("/arp/report/{activity_slug}")
def arp_report_save(
    activity_slug: str,
    body: dict[str, Any],
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> dict[str, Any]:
    _require_write_access(request=request, x_api_key=x_api_key, role="editor")
    slug = _slugify(activity_slug)
    report_md = str(body.get("report_md") or "")
    with _connect() as conn:
        with conn.cursor() as cur:
            _ensure_arp_tables(cur)
            cur.execute(
                _arp_schema(
                    """
                    UPDATE "__ARP_SCHEMA__".reports
                    SET report_md=%s, updated_at=now()
                    WHERE activity_slug=%s;
                    """
                ).strip(),
                (report_md, slug),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Report not found")
        conn.commit()
    return {"ok": True}


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
        raw = str(social_links.get(kind) or "").strip()
        if not raw:
            continue
        # Some source files store social links as markdown bullets like:
        # "* [Facebook](https://facebook.com/...)". Extract the actual URL.
        url = _extract_url(raw)
        if not (url.startswith("http://") or url.startswith("https://")):
            continue
        parts.append(
            f'<a href="{_esc(url)}" target="_blank" rel="noopener noreferrer">{_esc(labels.get(kind, kind))}</a>'
        )
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

    continent_to_items: dict[str, list[str]] = {}
    total_items = 0
    for country, provider_count in rows:
        c = str(country or "").strip()
        if not c:
            continue
        slug = _slugify(c)
        href = f"/trip_providers/countries/{quote(slug)}"
        continent = continent_for_country(c)
        item_html = f'<li><a href="{href}">{_esc(c)}</a> <span class="count">({int(provider_count or 0)})</span></li>'
        continent_to_items.setdefault(continent, []).append(item_html)
        total_items += 1

    cards: list[str] = []
    for continent in CONTINENT_ORDER:
        items = continent_to_items.get(continent) or []
        if not items:
            continue
        cards.append(
            f"""
            <div class="continent-card">
              <div class="continent-head">
                <div class="continent-title">{_esc(continent)}</div>
                <div class="muted">{len(items)} country(ies)</div>
              </div>
              <ul class="country-list">{"".join(items)}</ul>
            </div>
            """.strip()
        )

    list_html = (
        '<div class="continent-grid">' + "".join(cards) + "</div>"
        if cards
        else "<p class=\"muted\">No countries found.</p>"
    )

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
        <div class="muted">Showing {total_items} result(s).</div>
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
          p.logo_url,
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
    for provider_key, provider_name, website_url, logo_url, _market_orientation, client_profile, social_links in rows:
        key = str(provider_key or "").strip()
        name = str(provider_name or "").strip() or key
        website = str(website_url or "").strip()
        website_html = f'<a href="{_esc(website)}" target="_blank" rel="noopener">Website</a>' if website else ""
        client = str(client_profile or "").strip()
        social_html = _render_social_links_html(social_links) or ""
        logo = str(logo_url or "").strip()
        logo_html = (
            f'<img src="{_esc(logo)}" alt="" style="height:24px; width:auto; max-width:96px; object-fit:contain; display:block;" />'
            if logo
            else ""
        )
        tr_rows.append(
            f"""
            <tr>
              <td><div style="display:flex; gap:12px; align-items:center;">{logo_html}<div><a href="/trip_providers_research/{_esc(quote(key))}">{_esc(name)}</a><div class="muted" style="margin-top:4px;"><code>{_esc(key)}</code></div></div></div></td>
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
    missing_logos: bool = Query(default=False),
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
    if missing_logos:
        filters.append("NULLIF(TRIM(p.logo_url), '') IS NULL")
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
          p.logo_url,
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
        logo_url,
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
        logo = str(logo_url or "").strip()
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
        logo_html = (
            f'<img src="{_esc(logo)}" alt="" style="height:28px; width:auto; max-width:110px; object-fit:contain; display:block;" />'
            if logo
            else ""
        )
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
              <td>{logo_html}</td>
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
    missing_logos_checked = "checked" if missing_logos else ""
    tbody_html = "".join(tr_rows) if tr_rows else '<tr><td colspan="11" class="muted">No results.</td></tr>'

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
              <label style="margin-top:10px;">
                <input type="checkbox" name="missing_logos" value="true" {missing_logos_checked} />
                Only missing logos
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
	                <th>Logo</th>
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
          p.logo_url,
          p.profile_json,
          COALESCE(NULLIF(c.market_orientation, ''), 'Not stated') AS market_orientation,
          (
            SELECT jsonb_object_agg(sl.kind, sl.url)
            FROM "__SCHEMA__".provider_social_links sl
            WHERE sl.provider_id = p.id
          ) AS social_links
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
    for provider_id, provider_key, provider_name, website_url, logo_url, profile_json, market_orientation, social_links in rows:
        key = str(provider_key or "").strip()
        if not key:
            continue
        name = str(provider_name or "").strip() or key
        website = str(website_url or "").strip()
        website_html = f'<a href="{_esc(website)}" target="_blank" rel="noopener">Website</a>' if website else '<span class="muted">—</span>'
        logo = str(logo_url or "").strip()
        logo_html = (
            f'<img src="{_esc(logo)}" alt="" style="height:28px; width:auto; max-width:110px; object-fit:contain; display:block;" />'
            if logo
            else ""
        )
        social_html = _render_social_links_html(social_links) or '<span class="muted">—</span>'
        profile = profile_json if isinstance(profile_json, dict) else {}
        overview = ""
        try:
            v = profile.get("company_overview")
            if isinstance(v, dict):
                overview = str(v.get("value") or "").strip()
            elif isinstance(v, str):
                overview = v.strip()
        except Exception:
            overview = ""

        detail_href = f"/trip_providers_research/{quote(key)}"
        action_name = f"action__{key}"
        tr_rows.append(
            f"""
            <tr>
              <td>{logo_html}</td>
              <td><a href="{detail_href}">{_esc(name)}</a><div class="muted" style="margin-top:4px;"><code>{_esc(key)}</code></div></td>
              <td>{website_html}</td>
              <td class="muted" style="white-space:normal; word-break:break-word;">{_esc(overview)}</td>
              <td class="muted">{_esc(market_orientation)}</td>
              <td>
                <div style="display:flex; flex-direction:column; gap:10px;">
                  <label style="display:inline-flex; gap:6px; align-items:center;">
                    <input type="radio" name="{_esc(action_name)}" value="keep" checked />
                    Keep
                  </label>
                  <label style="display:inline-flex; gap:6px; align-items:center;">
                    <input type="radio" name="{_esc(action_name)}" value="education_focused" />
                    Education-focused
                  </label>
                  <label style="display:inline-flex; gap:6px; align-items:center;">
                    <input type="radio" name="{_esc(action_name)}" value="delete" />
                    Delete
                  </label>
                </div>
              </td>
              <td class="muted">{social_html}</td>
            </tr>
            """.strip()
        )

    tbody_html = "".join(tr_rows) if tr_rows else '<tr><td colspan="7" class="muted">No results.</td></tr>'
    done_html = ""
    if (done or "").strip():
        done_html = '<div class="statusbox" style="margin-top:12px;">Saved.</div>'

    body_html = f"""
      <div class="card">
        <div class="muted"><a href="/trip_providers_research">← Back to Trip Providers</a></div>
        <h1>Review Trip Providers</h1>
        <p class="muted">Providers with market orientation = <code>Not stated</code>. Default action is <strong>Keep</strong> (no changes) for every row.</p>
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
            <table style="table-layout:fixed; width:100%;">
              <colgroup>
                <col style="width:6%;" />
                <col style="width:22%;" />
                <col style="width:8%;" />
                <col style="width:18%;" />
                <col style="width:8%;" />
                <col style="width:28%;" />
                <col style="width:10%;" />
              </colgroup>
              <thead>
                <tr>
                  <th>Logo</th>
                  <th>Provider</th>
                  <th>Website</th>
                  <th>Overview</th>
                  <th>Market</th>
                  <th>Decision</th>
                  <th>Social</th>
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
                if d in {"", "keep", "no_change", "none"}:
                    continue
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
	          p.logo_url,
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
        logo_url,
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
    logo = str(logo_url or "").strip()
    logo_html = (
        f'<img src="{_esc(logo)}" alt="" style="height:44px; width:auto; max-width:200px; object-fit:contain; display:block;" />'
        if logo
        else ""
    )
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
	        <div style="display:flex; gap:16px; align-items:center; margin-top:6px;">
	          {logo_html}
	          <div>
	            <h1 style="margin:0;">{_esc(name or key)}</h1>
	            <div class="muted" style="margin-top:8px;"><code>{_esc(key)}</code> · <span class="pill">{_esc(status)}</span></div>
	          </div>
	        </div>
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
def health() -> JSONResponse:
    return JSONResponse({"ok": True}, headers={"Cache-Control": "no-store"})


@app.get("/health/db")
def health_db() -> JSONResponse:
    """
    DB diagnostics (safe to share):
    - verifies DB connectivity
    - shows which ARP tables exist + row counts
    - shows applied migration versions (OPS_SCHEMA.schema_migrations)
    """
    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute("select 1")
                cur.fetchone()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB connection failed: {e}") from e

    arp_schema = _require_safe_ident("ARP_SCHEMA", ARP_SCHEMA)
    ops_schema = _require_safe_ident("OPS_SCHEMA", OPS_SCHEMA)

    tables = [
        "activities",
        "sources",
        "documents",
        "chunks",
        "reports",
        "activity_icons",
    ]

    exists: dict[str, bool] = {}
    counts: dict[str, int] = {}
    migrations: list[str] = []

    with _connect() as conn:
        with conn.cursor() as cur:
            # Table existence (to_regclass returns NULL when missing)
            for t in tables:
                cur.execute("SELECT to_regclass(%s);", (f"{arp_schema}.{t}",))
                exists[t] = cur.fetchone()[0] is not None  # type: ignore[index]

            # Row counts (best effort)
            for t in tables:
                if not exists.get(t):
                    continue
                try:
                    cur.execute(f'SELECT COUNT(*) FROM "{arp_schema}".{t};')
                    counts[t] = int(cur.fetchone()[0] or 0)  # type: ignore[index]
                except Exception:
                    counts[t] = -1

            # Applied migrations (best effort)
            try:
                cur.execute("SELECT to_regclass(%s);", (f"{ops_schema}.schema_migrations",))
                has_m = cur.fetchone()[0] is not None  # type: ignore[index]
                if has_m:
                    cur.execute(f'SELECT version FROM "{ops_schema}".schema_migrations ORDER BY applied_at ASC;')
                    migrations = [str(r[0]) for r in (cur.fetchall() or [])]  # type: ignore[index]
            except Exception:
                migrations = []

        conn.commit()

    return JSONResponse(
        {
            "ok": True,
            "schemas": {"arp": arp_schema, "ops": ops_schema},
            "tables_exist": exists,
            "row_counts": counts,
            "migrations_applied": migrations,
        },
        headers={"Cache-Control": "no-store"},
    )


@app.get("/health/version")
def health_version() -> JSONResponse:
    """
    Safe diagnostics for deploy/version checking (no secrets).
    """
    main_path = Path(__file__).resolve()
    try:
        mtime = datetime.fromtimestamp(main_path.stat().st_mtime, tz=timezone.utc).isoformat()
    except Exception:
        mtime = ""

    keys = [
        "RENDER",
        "RENDER_SERVICE_ID",
        "RENDER_SERVICE_NAME",
        "RENDER_GIT_COMMIT",
        "RENDER_GIT_BRANCH",
    ]
    env = {k: (os.environ.get(k) or "") for k in keys if os.environ.get(k) is not None}

    return JSONResponse(
        {
            "ok": True,
            "utc_now": datetime.now(tz=timezone.utc).isoformat(),
            "main_py_mtime_utc": mtime,
            "env": env,
        },
        headers={"Cache-Control": "no-store"},
    )


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


def _run_weather_auto_batch(*, locations: list[str], force_refresh: bool, job_id: str | None = None) -> dict[str, Any]:
    locations = [str(x).strip() for x in (locations or []) if str(x).strip()]
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

    for i, q in enumerate(locations, start=1):
        if job_id:
            try:
                with _connect() as conn:
                    with conn.cursor() as cur:
                        _job_append_log(cur, job_id=job_id, line=f"[{i}/{len(locations)}] {q}")
                    conn.commit()
            except Exception:
                pass

        try:
            res, tok, model, wtok, wmodel, dtok, dmodel = _auto_generate_one(location_query=q, force_refresh=force_refresh)
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
    enqueue: bool = Query(default=False),
) -> dict[str, Any]:
    _require_access(request=request, x_api_key=x_api_key, role="editor")

    locations = [str(x).strip() for x in (body.locations or []) if str(x).strip()]
    if not locations:
        raise HTTPException(status_code=400, detail="Provide at least one location")

    if enqueue:
        job_id = _enqueue_job(kind="weather_auto_batch", payload={"locations": locations, "force_refresh": bool(body.force_refresh)})
        return {"ok": True, "enqueued": True, "job_id": job_id, "job_url": f"/jobs/ui?job_id={job_id}"}

    return _run_weather_auto_batch(locations=locations, force_refresh=bool(body.force_refresh))


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
