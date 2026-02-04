from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class OpenAIResult:
    payload: dict[str, Any]
    model: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    text: str = ""


def require_openai_key() -> str:
    key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not key:
        raise RuntimeError("OPENAI_API_KEY is not set")
    return key


def _extract_json_object(text: str) -> dict[str, Any]:
    text = (text or "").strip()
    if not text:
        raise ValueError("Empty model response")

    if text.startswith("{") and text.endswith("}"):
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
        raise ValueError("Expected JSON object")

    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        raise ValueError("No JSON object found in model response")
    obj = json.loads(m.group(0))
    if not isinstance(obj, dict):
        raise ValueError("Expected JSON object")
    return obj


def chat_json(*, model: str, system: str, user: str, temperature: float = 0.2) -> OpenAIResult:
    """
    Minimal Chat Completions call that returns a JSON object (parsed from message.content).
    """
    api_key = require_openai_key()
    model = (model or "").strip()
    if not model:
        raise ValueError("model is required")

    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": float(temperature),
    }

    req = Request(
        "https://api.openai.com/v1/chat/completions",
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
        raise ValueError("Unexpected OpenAI response")

    usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
    prompt_tokens = int(usage.get("prompt_tokens") or 0) if isinstance(usage.get("prompt_tokens"), (int, float)) else 0
    completion_tokens = (
        int(usage.get("completion_tokens") or 0) if isinstance(usage.get("completion_tokens"), (int, float)) else 0
    )
    total_tokens = int(usage.get("total_tokens") or 0) if isinstance(usage.get("total_tokens"), (int, float)) else 0
    model_used = str(data.get("model") or model).strip() or model

    choices = data.get("choices") or []
    if not isinstance(choices, list) or not choices:
        raise ValueError("OpenAI returned no choices")
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = (msg or {}).get("content") if isinstance(msg, dict) else None
    if not isinstance(content, str):
        raise ValueError("OpenAI response missing message.content")

    payload = _extract_json_object(content)
    return OpenAIResult(
        payload=payload,
        model=model_used,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=total_tokens,
        text=content,
    )


def chat_text(*, model: str, system: str, user: str, temperature: float = 0.2) -> OpenAIResult:
    """
    Minimal Chat Completions call that returns raw message.content as text.

    Use this when you want line-based outputs (e.g. title + subtitle) rather than JSON.
    """
    api_key = require_openai_key()
    model = (model or "").strip()
    if not model:
        raise ValueError("model is required")

    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": float(temperature),
    }

    req = Request(
        "https://api.openai.com/v1/chat/completions",
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
        raise ValueError("Unexpected OpenAI response")

    usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
    prompt_tokens = int(usage.get("prompt_tokens") or 0) if isinstance(usage.get("prompt_tokens"), (int, float)) else 0
    completion_tokens = (
        int(usage.get("completion_tokens") or 0) if isinstance(usage.get("completion_tokens"), (int, float)) else 0
    )
    total_tokens = int(usage.get("total_tokens") or 0) if isinstance(usage.get("total_tokens"), (int, float)) else 0
    model_used = str(data.get("model") or model).strip() or model

    choices = data.get("choices") or []
    if not isinstance(choices, list) or not choices:
        raise ValueError("OpenAI returned no choices")
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = (msg or {}).get("content") if isinstance(msg, dict) else None
    if not isinstance(content, str):
        raise ValueError("OpenAI response missing message.content")

    return OpenAIResult(
        payload={},
        model=model_used,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        total_tokens=total_tokens,
        text=content,
    )
