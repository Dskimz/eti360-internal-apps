# Geometry & Color Rules (Immutable Contract, v1)

This appendix is intended to be treated as an immutable contract between design, engineering, and governance.

## ETI360 Blue: Meaning and Allowed Uses

Principle:
- ETI360 Blue means **intentional student movement**, and nothing else.

Allowed uses (only):
- Route line (primary use)
- Origin/destination markers: **outline only** (secondary use)

Not allowed (never):
- City names
- Roads
- Land or water
- Background elements

## Color Authority

HEX:
- Authoritative for Mapbox JSON and web rendering.

CMYK:
- Authoritative for print fidelity **only when sourced from the ETI360 brand spec**.
- Engineering treats CMYK as read-only input.

Fallback (only if brand spec lacks CMYK):
- Use HEX as source of truth.
- Perform documented best-effort HEX->CMYK conversion.
- Flag output as **print approximation** (non-authoritative).

## Route Geometry Rules

Non-negotiable constraints:
- No station names
- No line names
- No arrows or turn cues
- Path is representative, not operational

Geometry engines:
- Primary: Mapbox Directions
- Air + Water: straight-line geometry

Mode policy:
- Coach/Bus: Mapbox Directions geometry
- Walking/Trekking: Mapbox Directions geometry
- Train/Metro/Subway/Tram:
  - Use transit-appropriate routing where supported
  - If transit is unavailable: use Mapbox driving corridor geometry while keeping the rail mode label

One-sentence rule (for code comments):
“Route geometry is representative and mode-aware; when true transit routing is unavailable, a non-authoritative corridor is rendered to preserve spatial understanding without implying operational routing.”

