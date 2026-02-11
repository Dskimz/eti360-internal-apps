from __future__ import annotations

import hashlib
import json
from typing import Any

from .models import IconIntentSpec

FIXED_GENERATION_PARAMS = {
    "size": "1024x1024",
    "background": "transparent",
    "output_format": "png",
}
ETI_ICON_PRIMARY_HEX = "#002b4f"


def sha256_json(obj: dict[str, Any]) -> str:
    payload = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _join_or_none(values: list[str]) -> str:
    return ", ".join(values) if values else "none"


def build_icon_prompt(spec: IconIntentSpec) -> str:
    canonical = spec.canonical()
    return "\n".join(
        [
            "Minimalist monochrome line icon.",
            f"Icon category: {canonical.icon_category}.",
            f"Primary symbol: {canonical.primary_symbol}.",
            f"Environmental cues: {_join_or_none(canonical.environmental_cues)}.",
            f"Secondary cues: {_join_or_none(canonical.secondary_cues)}.",
            f"Exclusions: {_join_or_none(canonical.exclusions)}.",
            f"{canonical.canvas}x{canonical.canvas} canvas, {canonical.stroke}px stroke.",
            f"Color token: {canonical.color_token} ({ETI_ICON_PRIMARY_HEX}).",
            "Neutral institutional style.",
        ]
    )
