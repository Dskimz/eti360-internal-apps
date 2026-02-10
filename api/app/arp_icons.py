from __future__ import annotations

import json
import re
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

ICON_CANVAS = 64
ICON_STROKE_WIDTH = 2
ICON_RENDERER_VERSION = "v1"

# Inline SVGs inherit CSS variables from the page.
PRIMARY_STROKE = "var(--eti-icon-primary, #1F4E79)"
NEUTRAL_STROKE = "var(--eti-icon-neutral, #2B2B2B)"

ALLOWED_ACTIVITY_TYPES = {
    "water_flat",
    "water_moving",
    "water_coastal",
    "land_trail",
    "urban_path",
    "vehicle_bus",
    "vehicle_train",
    "building_hotel",
    "indoor_activity",
}

ALLOWED_PRIMARY_SYMBOLS = {
    "kayak_top_down",
    "canoe_side",
    "ascending_trail",
    "urban_path",
    "vehicle_bus",
    "vehicle_train",
    "building_hotel",
    "indoor_activity",
}

ALLOWED_ENVIRONMENTAL_CUES = {"still_water", "elevation", "weather", "heat", "cold", "rain"}

ALLOWED_SECONDARY_CUES = {"guided", "group", "restricted"}

ALLOWED_EXCLUDE = {"people", "faces", "motion", "waves", "summit", "action", "instruction"}


ICON_SPEC_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "activity_type": {"type": "string"},
        "primary_symbol": {"type": "string"},
        "environmental_cues": {"type": "array", "items": {"type": "string"}},
        "secondary_cues": {"type": "array", "items": {"type": "string"}},
        "exclude": {"type": "array", "items": {"type": "string"}},
        "icon_variant": {"type": "string"},
    },
    "required": ["activity_type", "primary_symbol", "environmental_cues", "secondary_cues", "exclude", "icon_variant"],
}


ICON_CLASSIFY_SYSTEM = """
You are a semantic classifier for ETI360 activity icons.

CRITICAL:
- Return ONLY a single JSON object. No markdown. No extra keys.
- You must use ONLY approved vocabulary from the lists below.
- The icon must be neutral and institutional. No recreational vibe.
- Exclusions must include: people, motion.

Output schema:
{
  "activity_type": "",
  "primary_symbol": "",
  "environmental_cues": [],
  "secondary_cues": [],
  "exclude": [],
  "icon_variant": "standard"
}

Approved vocabulary:

activity_type:
%s

primary_symbol:
%s

environmental_cues:
%s

secondary_cues:
%s

exclude:
%s
""".strip() % (
    ", ".join(sorted(ALLOWED_ACTIVITY_TYPES)),
    ", ".join(sorted(ALLOWED_PRIMARY_SYMBOLS)),
    ", ".join(sorted(ALLOWED_ENVIRONMENTAL_CUES)),
    ", ".join(sorted(ALLOWED_SECONDARY_CUES)),
    ", ".join(sorted(ALLOWED_EXCLUDE)),
)


def icon_input_hash(*, activity_name: str, overview: str) -> str:
    h = sha256()
    h.update((activity_name or "").strip().encode("utf-8"))
    h.update(b"\n")
    h.update((overview or "").strip().encode("utf-8"))
    return h.hexdigest()[:24]


def _as_list_str(v: Any) -> list[str]:
    if not isinstance(v, list):
        return []
    out: list[str] = []
    for x in v:
        if isinstance(x, str) and x.strip():
            out.append(x.strip())
    return out


def validate_icon_spec(spec: Any) -> tuple[dict[str, Any], str]:
    if not isinstance(spec, dict):
        return {}, "Icon spec must be an object"

    activity_type = str(spec.get("activity_type") or "").strip()
    primary_symbol = str(spec.get("primary_symbol") or "").strip()
    environmental_cues = _as_list_str(spec.get("environmental_cues"))
    secondary_cues = _as_list_str(spec.get("secondary_cues"))
    exclude = _as_list_str(spec.get("exclude"))
    icon_variant = str(spec.get("icon_variant") or "standard").strip() or "standard"

    if activity_type not in ALLOWED_ACTIVITY_TYPES:
        return {}, "Invalid activity_type"
    if primary_symbol not in ALLOWED_PRIMARY_SYMBOLS:
        return {}, "Invalid primary_symbol"
    if icon_variant != "standard":
        return {}, "Invalid icon_variant"

    environmental_cues = [c for c in environmental_cues if c in ALLOWED_ENVIRONMENTAL_CUES]
    secondary_cues = [c for c in secondary_cues if c in ALLOWED_SECONDARY_CUES]
    exclude = [c for c in exclude if c in ALLOWED_EXCLUDE]

    # Composition limits (code-enforced)
    environmental_cues = environmental_cues[:2]
    secondary_cues = secondary_cues[:1]

    # Governance exclusions (always)
    if "people" not in exclude:
        exclude.append("people")
    if "motion" not in exclude:
        exclude.append("motion")

    cleaned = {
        "activity_type": activity_type,
        "primary_symbol": primary_symbol,
        "environmental_cues": environmental_cues,
        "secondary_cues": secondary_cues,
        "exclude": exclude,
        "icon_variant": "standard",
    }
    return cleaned, ""


def fallback_icon_spec(*, activity_name: str, overview: str) -> dict[str, Any]:
    text = f"{activity_name}\n{overview}".lower()
    if "kayak" in text:
        return {
            "activity_type": "water_flat",
            "primary_symbol": "kayak_top_down",
            "environmental_cues": ["still_water"],
            "secondary_cues": [],
            "exclude": ["people", "motion", "waves"],
            "icon_variant": "standard",
        }
    if "hike" in text or "trek" in text:
        return {
            "activity_type": "land_trail",
            "primary_symbol": "ascending_trail",
            "environmental_cues": ["elevation"],
            "secondary_cues": ["guided"] if "guide" in text else [],
            "exclude": ["people", "motion", "summit"],
            "icon_variant": "standard",
        }
    return {
        "activity_type": "indoor_activity",
        "primary_symbol": "indoor_activity",
        "environmental_cues": [],
        "secondary_cues": [],
        "exclude": ["people", "motion"],
        "icon_variant": "standard",
    }


def extract_activity_overview(report_md: str) -> str:
    """
    Extract the markdown section under "## Activity overview" if present.
    """
    text = str(report_md or "")
    m = re.search(r"(?mi)^##\\s+Activity overview\\s*$", text)
    if not m:
        return ""
    rest = text[m.end() :]
    # Until next H2
    nxt = re.search(r"(?mi)^##\\s+", rest)
    chunk = rest[: nxt.start()] if nxt else rest
    chunk = chunk.strip()
    # Drop leading blank lines
    chunk = re.sub(r"\\A\\s+", "", chunk)
    # Keep to a reasonable size for classification
    return chunk[:1200].strip()


def render_icon_svg(spec: dict[str, Any], *, stroke_mode: str = "primary") -> str:
    stroke = PRIMARY_STROKE if stroke_mode == "primary" else NEUTRAL_STROKE
    elems: list[str] = []

    activity_type = str(spec.get("activity_type") or "")
    primary_symbol = str(spec.get("primary_symbol") or "")
    env = list(spec.get("environmental_cues") or [])
    sec = list(spec.get("secondary_cues") or [])

    # 1) Baseline
    if activity_type.startswith("water"):
        elems.extend(_cue_still_water(y=46))
    else:
        elems.append(_path("M14 50 H50"))

    # 2) Primary symbol
    elems.extend(_primary(primary_symbol))

    # 3) Environmental cues
    for c in env:
        if c == "still_water":
            elems.extend(_cue_still_water(y=49))
        elif c == "elevation":
            elems.extend(_cue_elevation())
        elif c == "weather":
            elems.extend(_cue_weather())
        elif c == "heat":
            elems.extend(_cue_heat())
        elif c == "cold":
            elems.extend(_cue_cold())
        elif c == "rain":
            elems.extend(_cue_rain())

    # 4) Governance cues
    for c in sec:
        if c == "guided":
            elems.extend(_cue_guided())
        elif c == "group":
            elems.extend(_cue_group())
        elif c == "restricted":
            elems.extend(_cue_restricted())

    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{ICON_CANVAS}" height="{ICON_CANVAS}" '
        f'viewBox="0 0 {ICON_CANVAS} {ICON_CANVAS}" fill="none" '
        f'stroke="{stroke}" stroke-width="{ICON_STROKE_WIDTH}" stroke-linecap="round" stroke-linejoin="round">'
        + "".join(elems)
        + "</svg>"
    )
    return svg


def _path(d: str, *, extra: str = "") -> str:
    extra_s = f" {extra.strip()}" if extra.strip() else ""
    return f'<path d="{d}"{extra_s}/>'


def _primary(name: str) -> list[str]:
    if name == "kayak_top_down":
        return [
            _path("M32 10 C38 14 41 24 41 32 C41 40 38 50 32 54 C26 50 23 40 23 32 C23 24 26 14 32 10 Z"),
            _path("M32 14 L32 50"),
        ]
    if name == "ascending_trail":
        return [
            _path("M16 48 L26 38 L34 34 L42 26 L50 18"),
            _path("M24 40 H29"),
            _path("M32 33 H37"),
            _path("M40 26 H45"),
        ]
    if name == "urban_path":
        return [
            _path("M18 50 V30 H30 V18 H46"),
            _path("M18 38 H24"),
            _path("M30 24 V28"),
        ]
    if name == "vehicle_bus":
        return [
            _path("M18 22 H46 V44 H18 Z"),
            _path("M22 28 H42"),
            _path("M22 44 V48"),
            _path("M42 44 V48"),
            _path("M24 48 H22"),
            _path("M44 48 H42"),
        ]
    if name == "vehicle_train":
        return [
            _path("M20 18 H44 V44 H20 Z"),
            _path("M24 24 H40"),
            _path("M24 44 L20 48"),
            _path("M40 44 L44 48"),
            _path("M18 48 H46"),
        ]
    if name == "building_hotel":
        return [
            _path("M20 18 H44 V50 H20 Z"),
            _path("M26 24 H30"),
            _path("M34 24 H38"),
            _path("M26 32 H30"),
            _path("M34 32 H38"),
            _path("M30 50 V42 H34 V50"),
        ]
    # indoor_activity (default)
    return [
        _path("M18 18 H46 V46 H18 Z"),
        _path("M24 26 H40"),
        _path("M24 34 H40"),
    ]


def _cue_still_water(*, y: int) -> list[str]:
    return [
        _path(f"M14 {y} H50"),
        _path(f"M16 {y + 4} H48"),
    ]


def _cue_elevation() -> list[str]:
    return [
        _path("M14 46 H24 V40 H34 V34 H44"),
    ]


def _cue_weather() -> list[str]:
    # Simple cloud, no fill
    return [
        _path("M22 22 C22 19 24 17 27 17 C28 14 31 12 34 12 C38 12 41 15 41 19 C44 19 46 21 46 24 C46 27 44 29 41 29 H27 C24 29 22 27 22 24 Z"),
    ]


def _cue_heat() -> list[str]:
    return [
        _path("M48 16 V30"),
        _path("M46 30 A2 2 0 1 0 50 30 A2 2 0 1 0 46 30"),
        _path("M48 18 H50"),
        _path("M48 22 H50"),
        _path("M48 26 H50"),
    ]


def _cue_cold() -> list[str]:
    return [
        _path("M48 16 V30"),
        _path("M44 23 H52"),
        _path("M45.5 18.5 L50.5 27.5"),
        _path("M50.5 18.5 L45.5 27.5"),
    ]


def _cue_rain() -> list[str]:
    return [
        _path("M22 32 V40"),
        _path("M28 32 V40"),
        _path("M34 32 V40"),
        _path("M40 32 V40"),
    ]


def _cue_guided() -> list[str]:
    # Subtle bracket, top-left
    return [
        _path("M14 14 H22"),
        _path("M14 14 V22"),
    ]


def _cue_group() -> list[str]:
    # Enclosure outline (no people)
    return [
        _path("M14 14 H50 V50 H14 Z", extra='stroke-dasharray="4 3"'),
    ]


def _cue_restricted() -> list[str]:
    return [
        _path("M14 14 H50 V50 H14 Z", extra='stroke-dasharray="2 2"'),
        _path("M20 44 L44 20"),
    ]


@dataclass(frozen=True)
class IconRecord:
    activity_id: int
    activity_slug: str
    input_hash: str
    renderer_version: str
    spec_json: dict[str, Any]
    svg: str


def icon_record_from_row(row: tuple[Any, ...]) -> IconRecord:
    activity_id, activity_slug, input_hash, renderer_version, spec_json, svg = row
    spec = spec_json if isinstance(spec_json, dict) else {}
    return IconRecord(
        activity_id=int(activity_id),
        activity_slug=str(activity_slug),
        input_hash=str(input_hash or ""),
        renderer_version=str(renderer_version or ""),
        spec_json=spec,
        svg=str(svg or ""),
    )


def icon_spec_to_json(spec: dict[str, Any]) -> str:
    return json.dumps(spec, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

