from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path


CONTINENT_ORDER: list[str] = [
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "South America",
    "Oceania",
    "Antarctica",
    "Unknown",
]


def _norm_country_key(name: str) -> str:
    s = (name or "").strip().lower()
    if not s:
        return ""
    s = s.replace("&", " and ")
    s = s.replace("’", "'")
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\s*,\s*the$", "", s)  # "Bahamas, The" -> "Bahamas"
    s = re.sub(r"^the\s+", "", s)  # "The Gambia" -> "Gambia"
    return s.strip()


_BUILTIN_COUNTRY_TO_CONTINENT: dict[str, str] = {
    # North America
    "united states": "North America",
    "united states of america": "North America",
    "u.s.": "North America",
    "usa": "North America",
    "canada": "North America",
    "mexico": "North America",
    "greenland": "North America",
    "bermuda": "North America",
    "puerto rico": "North America",
    "dominican republic": "North America",
    "haiti": "North America",
    "jamaica": "North America",
    "cuba": "North America",
    "bahamas": "North America",
    "barbados": "North America",
    "trinidad and tobago": "North America",
    "saint lucia": "North America",
    "st lucia": "North America",
    "saint vincent and the grenadines": "North America",
    "st vincent and the grenadines": "North America",
    "antigua and barbuda": "North America",
    "saint kitts and nevis": "North America",
    "st kitts and nevis": "North America",
    "grenada": "North America",
    "belize": "North America",
    "guatemala": "North America",
    "honduras": "North America",
    "el salvador": "North America",
    "nicaragua": "North America",
    "costa rica": "North America",
    "panama": "North America",

    # South America
    "argentina": "South America",
    "bolivia": "South America",
    "bolivia (plurinational state of)": "South America",
    "brazil": "South America",
    "brasil": "South America",
    "chile": "South America",
    "colombia": "South America",
    "ecuador": "South America",
    "guyana": "South America",
    "paraguay": "South America",
    "peru": "South America",
    "suriname": "South America",
    "uruguay": "South America",
    "venezuela": "South America",
    "venezuela (bolivarian republic of)": "South America",

    # Europe
    "united kingdom": "Europe",
    "uk": "Europe",
    "great britain": "Europe",
    "ireland": "Europe",
    "france": "Europe",
    "spain": "Europe",
    "portugal": "Europe",
    "italy": "Europe",
    "germany": "Europe",
    "netherlands": "Europe",
    "belgium": "Europe",
    "switzerland": "Europe",
    "austria": "Europe",
    "norway": "Europe",
    "sweden": "Europe",
    "denmark": "Europe",
    "finland": "Europe",
    "iceland": "Europe",
    "poland": "Europe",
    "czech republic": "Europe",
    "czechia": "Europe",
    "slovakia": "Europe",
    "hungary": "Europe",
    "romania": "Europe",
    "bulgaria": "Europe",
    "greece": "Europe",
    "turkey": "Europe",
    "turkiye": "Europe",
    "croatia": "Europe",
    "slovenia": "Europe",
    "serbia": "Europe",
    "bosnia and herzegovina": "Europe",
    "montenegro": "Europe",
    "north macedonia": "Europe",
    "albania": "Europe",
    "estonia": "Europe",
    "latvia": "Europe",
    "lithuania": "Europe",
    "ukraine": "Europe",
    "belarus": "Europe",
    "moldova": "Europe",
    "republic of moldova": "Europe",
    "russia": "Europe",
    "russian federation": "Europe",

    # Asia
    "japan": "Asia",
    "china": "Asia",
    "hong kong": "Asia",
    "macao": "Asia",
    "taiwan": "Asia",
    "republic of korea": "Asia",
    "south korea": "Asia",
    "democratic people's republic of korea": "Asia",
    "north korea": "Asia",
    "india": "Asia",
    "pakistan": "Asia",
    "bangladesh": "Asia",
    "sri lanka": "Asia",
    "nepal": "Asia",
    "bhutan": "Asia",
    "maldives": "Asia",
    "afghanistan": "Asia",
    "iran": "Asia",
    "iran (islamic republic of)": "Asia",
    "iraq": "Asia",
    "israel": "Asia",
    "palestine": "Asia",
    "state of palestine": "Asia",
    "jordan": "Asia",
    "lebanon": "Asia",
    "saudi arabia": "Asia",
    "united arab emirates": "Asia",
    "uae": "Asia",
    "qatar": "Asia",
    "kuwait": "Asia",
    "bahrain": "Asia",
    "oman": "Asia",
    "yemen": "Asia",
    "syria": "Asia",
    "syrian arab republic": "Asia",
    "georgia": "Asia",
    "armenia": "Asia",
    "azerbaijan": "Asia",
    "kazakhstan": "Asia",
    "uzbekistan": "Asia",
    "turkmenistan": "Asia",
    "kyrgyzstan": "Asia",
    "tajikistan": "Asia",
    "mongolia": "Asia",
    "myanmar": "Asia",
    "burma": "Asia",
    "thailand": "Asia",
    "vietnam": "Asia",
    "laos": "Asia",
    "lao people's democratic republic": "Asia",
    "cambodia": "Asia",
    "malaysia": "Asia",
    "singapore": "Asia",
    "indonesia": "Asia",
    "philippines": "Asia",

    # Africa
    "morocco": "Africa",
    "algeria": "Africa",
    "tunisia": "Africa",
    "egypt": "Africa",
    "libya": "Africa",
    "sudan": "Africa",
    "south sudan": "Africa",
    "ethiopia": "Africa",
    "kenya": "Africa",
    "tanzania": "Africa",
    "united republic of tanzania": "Africa",
    "uganda": "Africa",
    "rwanda": "Africa",
    "burundi": "Africa",
    "somalia": "Africa",
    "ghana": "Africa",
    "nigeria": "Africa",
    "senegal": "Africa",
    "côte d'ivoire": "Africa",
    "cote d'ivoire": "Africa",
    "ivory coast": "Africa",
    "cameroon": "Africa",
    "angola": "Africa",
    "democratic republic of the congo": "Africa",
    "republic of the congo": "Africa",
    "congo": "Africa",
    "gabon": "Africa",
    "zambia": "Africa",
    "zimbabwe": "Africa",
    "botswana": "Africa",
    "namibia": "Africa",
    "south africa": "Africa",
    "mauritius": "Africa",
    "seychelles": "Africa",
    "réunion": "Africa",
    "reunion": "Africa",

    # Oceania
    "australia": "Oceania",
    "new zealand": "Oceania",
    "fiji": "Oceania",
    "papua new guinea": "Oceania",
    "samoa": "Oceania",
    "tonga": "Oceania",
    "vanuatu": "Oceania",
    "solomon islands": "Oceania",
    "french polynesia": "Oceania",
    "new caledonia": "Oceania",
    "guam": "Oceania",
}


@lru_cache(maxsize=1)
def _file_overrides() -> dict[str, str]:
    """
    Optional local overrides to keep the mapping editable without code changes.
    File format:
      {
        "united states": "North America",
        "côte d'ivoire": "Africa"
      }
    Keys are normalized via _norm_country_key.
    """
    path = Path(__file__).resolve().parent / "static" / "country_continent_overrides.json"
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in raw.items():
        key = _norm_country_key(str(k))
        val = str(v or "").strip()
        if not key or not val:
            continue
        out[key] = val
    return out


def continent_for_country(country_or_territory: str) -> str:
    key = _norm_country_key(country_or_territory)
    if not key:
        return "Unknown"

    overrides = _file_overrides()
    if key in overrides:
        return overrides[key]

    if key in _BUILTIN_COUNTRY_TO_CONTINENT:
        return _BUILTIN_COUNTRY_TO_CONTINENT[key]

    # Fall back for "X (Y)" where X is in our map.
    base = re.sub(r"\s*\([^)]*\)\s*$", "", key).strip()
    if base and base in _BUILTIN_COUNTRY_TO_CONTINENT:
        return _BUILTIN_COUNTRY_TO_CONTINENT[base]

    return "Unknown"

