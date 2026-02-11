from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

ICON_CATEGORIES = {
    "transport",
    "water_activity",
    "land_activity",
    "lodging",
    "education",
    "culture",
    "nature",
    "safety",
    "medical",
    "facility",
    "equipment",
}

PRIMARY_SYMBOLS_BY_CATEGORY: dict[str, set[str]] = {
    "transport": {"vehicle_bus", "vehicle_train", "vehicle_airplane", "vehicle_ferry", "vehicle_bicycle"},
    "water_activity": {"kayak", "canoe", "paddleboard", "snorkel_mask", "swim_fin"},
    "land_activity": {"hiking_boot", "climbing_holds", "ski", "tent", "compass"},
    "lodging": {"bed", "hostel_bunk", "hotel_building", "cabin", "camp_fire"},
    "education": {"book", "graduation_cap", "classroom_board", "microscope", "museum_pedestal"},
    "culture": {"theatre_mask", "music_note", "camera", "landmark_column", "palette"},
    "nature": {"mountain", "tree", "leaf", "wave", "sun"},
    "safety": {"shield", "first_aid_cross", "helmet", "lifebuoy", "warning_triangle"},
    "medical": {"stethoscope", "medical_bag", "pill", "hospital_building", "ambulance"},
    "facility": {"toilet", "faucet", "locker", "wifi", "charging_station"},
    "equipment": {"backpack", "water_bottle", "radio", "flashlight", "map"},
}

ENVIRONMENTAL_CUES = {
    "river",
    "lake",
    "sea",
    "trail",
    "mountain",
    "forest",
    "urban",
    "indoor",
    "outdoor",
    "rain",
    "snow",
    "sun",
}

SECONDARY_CUES = {
    "life_jacket",
    "paddle",
    "helmet",
    "binoculars",
    "map",
    "clock",
    "anchor",
    "bridge",
    "flag",
    "shelter",
}

EXCLUSIONS = {
    "people",
    "faces",
    "motion",
    "text",
    "logos",
    "brand_marks",
    "complex_background",
    "photorealism",
}

VISUAL_PREF_BLOCKLIST = {
    "color",
    "colour",
    "style",
    "aesthetic",
    "gradient",
    "shadow",
    "vibrant",
    "pastel",
    "minimal",
    "cute",
    "bold",
    "modern",
}


class IconFormInput(BaseModel):
    """User-facing form fields before classification."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    activity_name: str = Field(..., min_length=2, max_length=80)
    context_note: str = Field(..., min_length=10, max_length=600)

    @field_validator("context_note")
    @classmethod
    def validate_context_note(cls, value: str) -> str:
        sentences = [x for x in re.split(r"(?<=[.!?])\s+", value.strip()) if x]
        if len(sentences) > 3:
            raise ValueError("context_note must be 1 to 3 sentences")

        lowered = value.lower()
        for token in VISUAL_PREF_BLOCKLIST:
            if re.search(rf"\b{re.escape(token)}\b", lowered):
                raise ValueError("context_note cannot contain visual style directives")

        return value


class IconIntentSpec(BaseModel):
    """Strict intent schema emitted by LLM #1 (classifier only)."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    icon_category: str
    primary_symbol: str
    environmental_cues: list[str] = Field(default_factory=list, max_length=6)
    secondary_cues: list[str] = Field(default_factory=list, max_length=6)
    exclusions: list[str] = Field(default_factory=lambda: ["people", "motion"], min_length=1, max_length=8)
    canvas: Literal[64] = 64
    stroke: Literal[2] = 2
    color_token: Literal["--eti-icon-primary"] = "--eti-icon-primary"

    @field_validator("icon_category")
    @classmethod
    def validate_icon_category(cls, value: str) -> str:
        if value not in ICON_CATEGORIES:
            raise ValueError(f"icon_category must be one of: {sorted(ICON_CATEGORIES)}")
        return value

    @field_validator("environmental_cues")
    @classmethod
    def validate_environmental_cues(cls, value: list[str]) -> list[str]:
        for token in value:
            if token not in ENVIRONMENTAL_CUES:
                raise ValueError(f"environmental_cues token '{token}' is not in approved vocabulary")
        return value

    @field_validator("secondary_cues")
    @classmethod
    def validate_secondary_cues(cls, value: list[str]) -> list[str]:
        for token in value:
            if token not in SECONDARY_CUES:
                raise ValueError(f"secondary_cues token '{token}' is not in approved vocabulary")
        return value

    @field_validator("exclusions")
    @classmethod
    def validate_exclusions(cls, value: list[str]) -> list[str]:
        for token in value:
            if token not in EXCLUSIONS:
                raise ValueError(f"exclusions token '{token}' is not in approved vocabulary")
        return value

    @model_validator(mode="after")
    def cross_validate(self) -> "IconIntentSpec":
        allowed_primary = PRIMARY_SYMBOLS_BY_CATEGORY.get(self.icon_category, set())
        if self.primary_symbol not in allowed_primary:
            raise ValueError(
                f"primary_symbol '{self.primary_symbol}' is not allowed for icon_category '{self.icon_category}'"
            )

        env_dupes = len(set(self.environmental_cues)) != len(self.environmental_cues)
        sec_dupes = len(set(self.secondary_cues)) != len(self.secondary_cues)
        exc_dupes = len(set(self.exclusions)) != len(self.exclusions)
        if env_dupes or sec_dupes or exc_dupes:
            raise ValueError("environmental_cues, secondary_cues, and exclusions must not contain duplicates")

        if set(self.environmental_cues) & set(self.secondary_cues):
            raise ValueError("A cue cannot appear in both environmental_cues and secondary_cues")

        return self

    def canonical(self) -> "IconIntentSpec":
        """Canonical ordering guarantees stable hashes and repeatable prompt output."""
        return self.model_copy(
            update={
                "environmental_cues": sorted(self.environmental_cues),
                "secondary_cues": sorted(self.secondary_cues),
                "exclusions": sorted(self.exclusions),
            }
        )
