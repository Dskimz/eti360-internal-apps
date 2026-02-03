from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class PerplexityResult:
    payload: dict[str, Any]
    citations: list[str]
    model: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int


def require_perplexity_key() -> str:
    key = os.environ.get("PERPLEXITY_API_KEY", "").strip()
    if not key:
        raise RuntimeError("PERPLEXITY_API_KEY is not set")
    return key


def _extract_json_object(text: str) -> dict[str, Any]:
    text = (text or "").strip()
    if not text:
        raise ValueError("Empty model response")

    # Common case: response is already a pure JSON object.
    if text.startswith("{") and text.endswith("}"):
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
        raise ValueError("Expected JSON object")

    # Otherwise, try to extract the first {...} block.
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        raise ValueError("No JSON object found in model response")
    obj = json.loads(m.group(0))
    if not isinstance(obj, dict):
        raise ValueError("Expected JSON object")
    return obj


def fetch_monthly_weather_normals(
    *,
    location_label: str,
    location_hint: str = "",
) -> PerplexityResult:
    """
    Ask Perplexity for 12-month climate normals (high/low C, precip cm).
    """
    api_key = require_perplexity_key()
    model = os.environ.get("PERPLEXITY_MODEL", "").strip() or "sonar-pro"

    accessed_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    hint = f"\n\nLocation hint: {location_hint.strip()}" if location_hint.strip() else ""

    prompt = f"""
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

    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a careful data extraction assistant. Output JSON only."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
    }

    req = Request(
        "https://api.perplexity.ai/chat/completions",
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        data=json.dumps(body).encode("utf-8"),
    )

    with urlopen(req, timeout=45) as resp:  # nosec - internal service call
        raw = resp.read().decode("utf-8", errors="replace")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("Unexpected Perplexity response")

    citations = data.get("citations") or []
    if not isinstance(citations, list):
        citations = []
    citations_out = [str(u) for u in citations if isinstance(u, str)]

    usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
    prompt_tokens = int(usage.get("prompt_tokens") or 0) if isinstance(usage.get("prompt_tokens"), (int, float)) else 0
    completion_tokens = (
        int(usage.get("completion_tokens") or 0) if isinstance(usage.get("completion_tokens"), (int, float)) else 0
    )
    total_tokens = int(usage.get("total_tokens") or 0) if isinstance(usage.get("total_tokens"), (int, float)) else 0
    model_used = str(data.get("model") or model).strip() or model

    choices = data.get("choices") or []
    if not isinstance(choices, list) or not choices:
        raise ValueError("Perplexity returned no choices")
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = (msg or {}).get("content") if isinstance(msg, dict) else None
    if not isinstance(content, str):
        raise ValueError("Perplexity response missing message.content")

    payload = _extract_json_object(content)
    return PerplexityResult(
        payload=payload,
        citations=citations_out,
        model=model_used,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=total_tokens,
    )
