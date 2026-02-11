from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .models import (
    ENVIRONMENTAL_CUES,
    EXCLUSIONS,
    ICON_CATEGORIES,
    PRIMARY_SYMBOLS_BY_CATEGORY,
    SECONDARY_CUES,
    IconFormInput,
    IconIntentSpec,
)

OPENAI_API_BASE = "https://api.openai.com/v1"


@dataclass(frozen=True)
class IconModelConfig:
    classifier_model: str
    renderer_model: str
    classifier_input_usd_per_1m: float
    classifier_output_usd_per_1m: float
    renderer_input_usd_per_1m: float
    renderer_output_usd_per_1m: float
    renderer_usd_per_image: float


@dataclass(frozen=True)
class UsageCost:
    model: str
    input_tokens: int
    output_tokens: int
    estimated_cost_usd: float
    raw_usage: dict[str, Any]


@dataclass(frozen=True)
class ClassificationResult:
    spec: IconIntentSpec
    usage: UsageCost


@dataclass(frozen=True)
class RenderResult:
    png_bytes: bytes
    usage: UsageCost


def get_icon_model_config() -> IconModelConfig:
    return IconModelConfig(
        classifier_model=os.environ.get("ICON_CLASSIFIER_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini",
        renderer_model=os.environ.get("ICON_RENDERER_MODEL", "gpt-image-1").strip() or "gpt-image-1",
        classifier_input_usd_per_1m=float(os.environ.get("ICON_CLASSIFIER_INPUT_USD_PER_1M", "0.4")),
        classifier_output_usd_per_1m=float(os.environ.get("ICON_CLASSIFIER_OUTPUT_USD_PER_1M", "1.6")),
        renderer_input_usd_per_1m=float(os.environ.get("ICON_RENDERER_INPUT_USD_PER_1M", "0")),
        renderer_output_usd_per_1m=float(os.environ.get("ICON_RENDERER_OUTPUT_USD_PER_1M", "0")),
        renderer_usd_per_image=float(os.environ.get("ICON_RENDERER_USD_PER_IMAGE", "0.04")),
    )


def _require_openai_api_key() -> str:
    key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not key:
        raise RuntimeError("OPENAI_API_KEY is not set")
    return key


def _post_openai_json(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    api_key = _require_openai_api_key()
    req = Request(
        f"{OPENAI_API_BASE}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(req, timeout=60) as resp:  # nosec - backend service call
            raw = resp.read().decode("utf-8", errors="replace")
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else ""
        raise RuntimeError(f"OpenAI request failed: HTTP {e.code} {body[:400]}") from e
    except URLError as e:
        raise RuntimeError(f"OpenAI request failed: {e}") from e

    obj = json.loads(raw)
    if not isinstance(obj, dict):
        raise RuntimeError("OpenAI response was not a JSON object")
    return obj


def _estimate_text_cost(
    input_tokens: int,
    output_tokens: int,
    *,
    input_rate_per_1m: float,
    output_rate_per_1m: float,
) -> float:
    in_cost = (max(input_tokens, 0) / 1_000_000.0) * max(input_rate_per_1m, 0.0)
    out_cost = (max(output_tokens, 0) / 1_000_000.0) * max(output_rate_per_1m, 0.0)
    return round(in_cost + out_cost, 8)


def _classifier_system_prompt() -> str:
    categories = ", ".join(sorted(ICON_CATEGORIES))
    env = ", ".join(sorted(ENVIRONMENTAL_CUES))
    secondary = ", ".join(sorted(SECONDARY_CUES))
    exclusions = ", ".join(sorted(EXCLUSIONS))
    primary_lines = [f"- {k}: {', '.join(sorted(v))}" for k, v in sorted(PRIMARY_SYMBOLS_BY_CATEGORY.items())]

    return "\n".join(
        [
            "You are ETI360 Icon Classifier (LLM #1).",
            "Task: map user activity text to strict icon intent JSON.",
            "Do not add any fields and do not include explanations.",
            "Use only approved vocabulary.",
            f"Allowed icon_category: {categories}",
            "Allowed primary_symbol by category:",
            *primary_lines,
            f"Allowed environmental_cues: {env}",
            f"Allowed secondary_cues: {secondary}",
            f"Allowed exclusions: {exclusions}",
            "Always set: canvas=64, stroke=2, color_token='--eti-icon-primary'.",
            "Default exclusions should include people and motion.",
        ]
    )


def classify_icon_intent(form_input: IconFormInput) -> ClassificationResult:
    cfg = get_icon_model_config()
    payload = {
        "model": cfg.classifier_model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": _classifier_system_prompt()},
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "activity_name": form_input.activity_name,
                        "context_note": form_input.context_note,
                    },
                    ensure_ascii=True,
                ),
            },
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "eti360_icon_intent",
                "strict": True,
                "schema": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "icon_category",
                        "primary_symbol",
                        "environmental_cues",
                        "secondary_cues",
                        "exclusions",
                        "canvas",
                        "stroke",
                        "color_token",
                    ],
                    "properties": {
                        "icon_category": {"type": "string"},
                        "primary_symbol": {"type": "string"},
                        "environmental_cues": {"type": "array", "items": {"type": "string"}},
                        "secondary_cues": {"type": "array", "items": {"type": "string"}},
                        "exclusions": {"type": "array", "items": {"type": "string"}},
                        "canvas": {"type": "integer"},
                        "stroke": {"type": "integer"},
                        "color_token": {"type": "string"},
                    },
                },
            },
        },
    }

    data = _post_openai_json("/chat/completions", payload)
    choices = data.get("choices") or []
    if not choices:
        raise RuntimeError("Classifier returned no choices")
    first = choices[0] if isinstance(choices[0], dict) else {}
    msg = first.get("message") if isinstance(first.get("message"), dict) else {}
    raw_content = str(msg.get("content") or "").strip()
    if not raw_content:
        raise RuntimeError("Classifier response content is empty")

    try:
        spec_obj = json.loads(raw_content)
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"Classifier did not return valid JSON: {e}") from e

    spec = IconIntentSpec.model_validate(spec_obj).canonical()

    usage_obj = data.get("usage") if isinstance(data.get("usage"), dict) else {}
    input_tokens = int(usage_obj.get("prompt_tokens") or 0)
    output_tokens = int(usage_obj.get("completion_tokens") or 0)
    cost = _estimate_text_cost(
        input_tokens,
        output_tokens,
        input_rate_per_1m=cfg.classifier_input_usd_per_1m,
        output_rate_per_1m=cfg.classifier_output_usd_per_1m,
    )

    usage = UsageCost(
        model=cfg.classifier_model,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        estimated_cost_usd=cost,
        raw_usage=usage_obj,
    )
    return ClassificationResult(spec=spec, usage=usage)


def render_icon_png(prompt: str) -> RenderResult:
    cfg = get_icon_model_config()
    payload = {
        "model": cfg.renderer_model,
        "prompt": prompt,
        "size": "1024x1024",
        "background": "transparent",
        "output_format": "png",
    }

    data = _post_openai_json("/images/generations", payload)
    rows = data.get("data") or []
    if not rows or not isinstance(rows[0], dict):
        raise RuntimeError("Renderer returned no image rows")

    b64 = str(rows[0].get("b64_json") or "").strip()
    if not b64:
        raise RuntimeError("Renderer response missing b64_json")
    png_bytes = base64.b64decode(b64)

    usage_obj = data.get("usage") if isinstance(data.get("usage"), dict) else {}
    input_tokens = int(usage_obj.get("input_tokens") or usage_obj.get("prompt_tokens") or 0)
    output_tokens = int(usage_obj.get("output_tokens") or usage_obj.get("completion_tokens") or 0)

    cost = cfg.renderer_usd_per_image
    if input_tokens or output_tokens:
        cost += _estimate_text_cost(
            input_tokens,
            output_tokens,
            input_rate_per_1m=cfg.renderer_input_usd_per_1m,
            output_rate_per_1m=cfg.renderer_output_usd_per_1m,
        )
    cost = round(cost, 8)

    usage = UsageCost(
        model=cfg.renderer_model,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        estimated_cost_usd=cost,
        raw_usage=usage_obj,
    )
    return RenderResult(png_bytes=png_bytes, usage=usage)
