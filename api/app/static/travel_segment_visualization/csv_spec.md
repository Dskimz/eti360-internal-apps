# CSV Spec (v1)

Source: Google Sheets export (CSV).

## Required Columns (per row = one segment)

- `trip_id` (string)
- `trip_name` (string)
- `segment_order` (integer, 1-based)
- `segment_name` (string)
- `date_local` (YYYY-MM-DD)
- `departure_time_local` (HH:MM, 24h)
- `timezone` (IANA tz, e.g. `America/New_York`)
- `mode` (enum; see Mode Values)
- `origin_name` (string)
- `origin_lat` (number, WGS84)
- `origin_lng` (number, WGS84)
- `destination_name` (string)
- `destination_lat` (number, WGS84)
- `destination_lng` (number, WGS84)

## Optional Override Columns (v1)

### Environment
- `environment_override` (enum: `Urban|Suburban|Rural|Remote`)

### Duration
- `base_duration_min_override` (integer minutes; if present, skip Google Distance Matrix for this segment)
- `expected_duration_min_override` (integer minutes)
- `expected_duration_max_override` (integer minutes)

Rules:
- If `expected_duration_min_override` and `expected_duration_max_override` are both present, use them.
- Else derive from `base_duration_min` using global multipliers (0.8 / 1.3).

### Distance (if you need manual override)
- `distance_km_override` (number)
- `distance_miles_override` (number)

### Notes (non-displayed, for audit/debug)
- `notes` (string)

## Mode Values (v1)

Allowed `mode` values:
- `coach`
- `walking`
- `train`
- `metro`
- `subway`
- `tram`
- `air`
- `ferry`
- `small_craft`

Mode-to-geometry policy:
- Use Mapbox Directions geometry for: `coach`, `walking`, and rail modes (`train|metro|subway|tram`) when supported.
- Use straight-line geometry for: `air`, `ferry`, `small_craft`.

## Validation Rules (v1)
- Lat/lng must be present for origin and destination.
- `segment_order` must be unique per `trip_id` and contiguous is recommended.
- `timezone` must parse as an IANA timezone.
- Date/time must parse; all displayed times are local.

## Minimal Example Row (header order not required)
```csv
trip_id,trip_name,segment_order,segment_name,date_local,departure_time_local,timezone,mode,origin_name,origin_lat,origin_lng,destination_name,destination_lat,destination_lng
T-001,Spain Spring Trip,1,Hotel to Museum,2026-05-15,08:30,Europe/Madrid,coach,Hotel,40.4168,-3.7038,Museum,40.4139,-3.6921
```

