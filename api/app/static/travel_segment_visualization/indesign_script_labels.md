# InDesign Script Labels (v1)

This contract defines the Script Labels the InDesign injector script will rely on.
Bind labels to frames or groups using InDesign's Script Label metadata.

## Document-Level Labels

- `eti360.trip_title` (text frame)
- `eti360.trip_id` (text frame, optional)
- `eti360.generated_at` (text frame, optional)
- `eti360.page_footer_note` (text frame; governance note)

## Repeated Segment Block

Each page contains 3 repeated segment blocks.
Label each block group:
- `eti360.segment_block.1`
- `eti360.segment_block.2`
- `eti360.segment_block.3`

Inside each segment block group, label these items:

### Text Frames
- `eti360.segment.title` (e.g., `Travel Segment: Hotel -> Museum`)
- `eti360.segment.date` (e.g., `May 15, 2026`)
- `eti360.segment.departure` (e.g., `Planned Departure: 8:30 - 9:00 AM`)
- `eti360.segment.arrival` (e.g., `Planned Arrival: 9:15 - 9:45 AM`)
- `eti360.segment.duration` (e.g., `Expected Duration: 20 - 40 minutes`)
- `eti360.segment.distance` (e.g., `Distance: 5 miles / 8 km`)
- `eti360.segment.mode` (e.g., `Mode: Private Coach`)
- `eti360.segment.disclaimer` (optional segment-level disclaimer)
- `eti360.segment.map_caption` (e.g., `Optional map view (for orientation only)`)

### Icon Frames (placed images or groups)
- `eti360.segment.icon.mode` (place icon asset)
- `eti360.segment.icon.environment` (place icon asset)

### Image Frames
- `eti360.segment.image.map` (place the static map PNG)
- `eti360.segment.image.qr` (place the QR PNG)

## Script Behavior (assumptions)
- Ordering is driven by `segment_order` from JSON (already sorted).
- A segment block can be hidden/removed if the page has < 3 remaining segments.
- The script should fail fast if any required labels are missing.

