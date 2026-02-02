from __future__ import annotations

import os

import psycopg
from fastapi import FastAPI, HTTPException

app = FastAPI(title="ETI360 Internal API", docs_url=None, redoc_url=None)


@app.get("/health")
def health() -> dict[str, bool]:
    return {"ok": True}


@app.get("/health/db")
def health_db() -> dict[str, bool]:
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        raise HTTPException(status_code=500, detail="DATABASE_URL is not set")

    try:
        with psycopg.connect(database_url, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("select 1")
                cur.fetchone()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB connection failed: {e}") from e

    return {"ok": True}
