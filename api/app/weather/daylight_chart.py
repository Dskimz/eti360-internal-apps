from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
from astral import Depression, Observer
from astral.sun import dawn, dusk, sunrise, sunset
from zoneinfo import ZoneInfo


# ---- Brand palette ----
ETI_BLUE = "#1F4E79"  # primary line (sunrise/sunset)
ETI_GOLD = "#C9A24D"  # subtle reference line (noon)
GRID_GREY = "#D0D3D6"
TEXT_GREY = "#333333"
MUTED_GREY = "#777777"

# Daylight bands (muted, role-based). We render night, nautical twilight,
# civil twilight, and daylight.
NIGHT = "#2F3E46"
DAYLIGHT = "#CFE0F2"

# Header layout (axes coordinates)
HEADER_X = -0.02
TITLE_PAD = 36
SUBTITLE_Y = 1.065
RULE_Y = 1.03


@dataclass(frozen=True)
class DaylightInputs:
    display_name: str
    lat: float
    lng: float
    timezone_id: str


def compute_daylight_summary(*, inputs: DaylightInputs, year: int) -> dict[str, object]:
    """
    Compute a compact summary of daylight duration across the year.

    This is intended for LLM prompting / UI display, not for scientific precision.
    """
    tz = ZoneInfo(inputs.timezone_id)
    observer = Observer(latitude=inputs.lat, longitude=inputs.lng)

    start = date(year, 1, 1)
    end = date(year + 1, 1, 1)
    days = (end - start).days

    durations_min: list[float] = []
    dates: list[date] = []
    polar_day = 0
    polar_night = 0

    for i in range(days):
        d = start + timedelta(days=i)
        dates.append(d)
        try:
            sr_dt = sunrise(observer, date=d, tzinfo=tz)
            ss_dt = sunset(observer, date=d, tzinfo=tz)
            sr_m = _minutes_since_midnight(sr_dt)
            ss_m = _minutes_since_midnight(ss_dt)
            dur = ss_m - sr_m
            if dur < 0:
                dur += 24 * 60.0
            durations_min.append(float(dur))
        except ValueError as e:
            msg = str(e).lower()
            if "never rises" in msg or "always below the horizon" in msg:
                polar_night += 1
                durations_min.append(0.0)
                continue
            if "never sets" in msg or "always above the horizon" in msg:
                polar_day += 1
                durations_min.append(24 * 60.0)
                continue
            durations_min.append(float("nan"))

    finite = [(i, m) for i, m in enumerate(durations_min) if m == m]  # NaN check
    if not finite:
        return {
            "display_name": inputs.display_name,
            "timezone_id": inputs.timezone_id,
            "lat": inputs.lat,
            "lng": inputs.lng,
            "year": year,
            "note": "No finite daylight durations computed",
        }

    max_i, max_m = max(finite, key=lambda t: t[1])
    min_i, min_m = min(finite, key=lambda t: t[1])

    max_h = round(max_m / 60.0, 2)
    min_h = round(min_m / 60.0, 2)
    return {
        "display_name": inputs.display_name,
        "timezone_id": inputs.timezone_id,
        "lat": inputs.lat,
        "lng": inputs.lng,
        "year": year,
        "daylight_max_hours": max_h,
        "daylight_min_hours": min_h,
        "daylight_range_hours": round(max_h - min_h, 2),
        "daylight_max_date": dates[max_i].isoformat(),
        "daylight_min_date": dates[min_i].isoformat(),
        "polar_day_count": polar_day,
        "polar_night_count": polar_night,
    }


def _month_midpoints(year: int) -> list[tuple[int, str]]:
    out: list[tuple[int, str]] = []
    year_start = date(year, 1, 1)
    for month in range(1, 13):
        start = date(year, month, 1)
        next_start = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
        days_in_month = (next_start - start).days
        mid = start + timedelta(days=days_in_month // 2)
        out.append(((mid - year_start).days, start.strftime("%b")))
    return out


def _minutes_since_midnight(dt: datetime) -> float:
    return dt.hour * 60.0 + dt.minute + (dt.second / 60.0)


def _twilight_bounds_minutes(observer: Observer, d: date, tz: ZoneInfo, dep: Depression) -> tuple[float, float]:
    """
    Return (dawn_min, dusk_min) in minutes since midnight local time for a twilight depression.
    """
    try:
        dawn_dt = dawn(observer, date=d, tzinfo=tz, depression=dep)
        dusk_dt = dusk(observer, date=d, tzinfo=tz, depression=dep)
        return _minutes_since_midnight(dawn_dt), _minutes_since_midnight(dusk_dt)
    except ValueError as e:
        msg = str(e).lower()
        if "never reaches" in msg and "below the horizon" in msg:
            return 0.0, 24.0 * 60.0
        if "unable to find a" in msg and "time" in msg:
            return float("nan"), float("nan")
        raise


def _interp_fill(a: np.ndarray) -> tuple[np.ndarray, int]:
    if a.ndim != 1:
        raise ValueError("Expected a 1D array")
    x = np.arange(a.size, dtype=float)
    mask = np.isfinite(a)
    missing = int(np.sum(~mask))
    if missing == 0:
        return a, 0
    if np.sum(mask) == 0:
        return np.zeros_like(a), missing
    filled = a.copy()
    filled[~mask] = np.interp(x[~mask], x[mask], a[mask])
    return filled, missing


def _hex_to_rgb01(h: str) -> tuple[float, float, float]:
    h = h.lstrip("#")
    return (int(h[0:2], 16) / 255.0, int(h[2:4], 16) / 255.0, int(h[4:6], 16) / 255.0)


def _rgb01_to_hex(rgb: tuple[float, float, float]) -> str:
    r, g, b = rgb
    return "#{:02X}{:02X}{:02X}".format(int(round(r * 255)), int(round(g * 255)), int(round(b * 255)))


def _blend_hex(a: str, b: str, t: float) -> str:
    ar, ag, ab = _hex_to_rgb01(a)
    br, bg, bb = _hex_to_rgb01(b)
    return _rgb01_to_hex((ar + (br - ar) * t, ag + (bg - ag) * t, ab + (bb - ab) * t))


def _gaussian_kernel1d(sigma: float, radius: int) -> np.ndarray:
    if sigma <= 0:
        k = np.zeros(1, dtype=np.float32)
        k[0] = 1.0
        return k
    x = np.arange(-radius, radius + 1, dtype=np.float32)
    k = np.exp(-(x * x) / (2.0 * sigma * sigma))
    k /= float(np.sum(k))
    return k.astype(np.float32)


def _blur1d_reflect(a: np.ndarray, kernel: np.ndarray, axis: int) -> np.ndarray:
    radius = (len(kernel) - 1) // 2
    pad_width = [(0, 0)] * a.ndim
    pad_width[axis] = (radius, radius)
    padded = np.pad(a, pad_width, mode="reflect")

    out = np.empty_like(a, dtype=np.float32)
    it = np.nditer(np.zeros(a.shape[:axis] + a.shape[axis + 1 :]), flags=["multi_index"])
    while not it.finished:
        idx = list(it.multi_index)
        idx.insert(axis, slice(None))
        sl = tuple(idx)
        vec = padded[sl].astype(np.float32)
        out[sl] = np.convolve(vec, kernel, mode="valid")
        it.iternext()
    return out


def _gaussian_blur2d(a: np.ndarray, sigma_y: float, sigma_x: float) -> np.ndarray:
    if a.ndim != 2:
        raise ValueError("Expected a 2D array")
    ky = _gaussian_kernel1d(sigma_y, radius=max(1, int(round(3 * sigma_y))))
    kx = _gaussian_kernel1d(sigma_x, radius=max(1, int(round(3 * sigma_x))))
    tmp = _blur1d_reflect(a, ky, axis=0)
    out = _blur1d_reflect(tmp, kx, axis=1)
    return out


def render_daylight_chart(
    *,
    inputs: DaylightInputs,
    year: int,
    output_path: Path,
    chart_title: str | None = None,
    chart_subtitle: str | None = None,
    source_left: str = "Computed from lat/lng + timezone (Astral; civil + nautical twilight).",
    brand_right: str = "ETI360",
    minute_step: int = 5,
    smooth: bool = True,
    smooth_sigma_minutes: float = 1.2,
    smooth_sigma_days: float = 0.0,
) -> None:
    """
    Ported from the legacy ETI360 daylight chart implementation (heatmap bands across the year).
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap
    from matplotlib.patches import Patch
    from matplotlib import rcParams

    rcParams["font.family"] = "DejaVu Sans"
    rcParams["axes.titleweight"] = "bold"
    rcParams["axes.titlesize"] = 20

    tz = ZoneInfo(inputs.timezone_id)
    observer = Observer(latitude=inputs.lat, longitude=inputs.lng)

    start = date(year, 1, 1)
    end = date(year + 1, 1, 1)
    days = (end - start).days

    minutes = np.arange(0, 24 * 60, minute_step, dtype=float)

    grid = np.zeros((len(minutes), days), dtype=np.float32)
    sunrise_m = np.full(days, np.nan, dtype=float)
    sunset_m = np.full(days, np.nan, dtype=float)
    dawn_c_m = np.full(days, np.nan, dtype=float)
    dusk_c_m = np.full(days, np.nan, dtype=float)
    dawn_n_m = np.full(days, np.nan, dtype=float)
    dusk_n_m = np.full(days, np.nan, dtype=float)
    polar = np.zeros(days, dtype=np.int8)  # -1 night, +1 day, 0 normal

    nautical_v = 0.45
    civil_v = 0.7
    daylight_v = 1.0

    nautical_c = _blend_hex(NIGHT, DAYLIGHT, nautical_v)
    civil_c = _blend_hex(NIGHT, DAYLIGHT, civil_v)
    cmap = LinearSegmentedColormap.from_list(
        "eti_daylight",
        [(0.0, NIGHT), (nautical_v, nautical_c), (civil_v, civil_c), (daylight_v, DAYLIGHT)],
    )

    for i in range(days):
        d = start + timedelta(days=i)
        try:
            sr_dt = sunrise(observer, date=d, tzinfo=tz)
            ss_dt = sunset(observer, date=d, tzinfo=tz)
        except ValueError as e:
            msg = str(e).lower()
            if "never rises" in msg or "always below the horizon" in msg:
                polar[i] = -1
                continue
            if "never sets" in msg or "always above the horizon" in msg:
                polar[i] = 1
                continue
            continue

        sunrise_m[i] = _minutes_since_midnight(sr_dt)
        sunset_m[i] = _minutes_since_midnight(ss_dt)
        dawn_c_m[i], dusk_c_m[i] = _twilight_bounds_minutes(observer, d, tz, Depression.CIVIL)
        dawn_n_m[i], dusk_n_m[i] = _twilight_bounds_minutes(observer, d, tz, Depression.NAUTICAL)

    sunrise_m, sunrise_missing = _interp_fill(sunrise_m)
    sunset_m, sunset_missing = _interp_fill(sunset_m)
    dawn_c_m, dawn_c_missing = _interp_fill(dawn_c_m)
    dusk_c_m, dusk_c_missing = _interp_fill(dusk_c_m)
    dawn_n_m, dawn_n_missing = _interp_fill(dawn_n_m)
    dusk_n_m, dusk_n_missing = _interp_fill(dusk_n_m)

    recovered = sunrise_missing + sunset_missing + dawn_c_missing + dusk_c_missing + dawn_n_missing + dusk_n_missing
    if recovered:
        print(f"Daylight chart note: interpolated {recovered} missing sunrise/sunset/twilight values.")

    for i in range(days):
        if polar[i] == -1:
            grid[:, i] = 0.0
            continue
        if polar[i] == 1:
            grid[:, i] = 1.0
            continue

        sr = float(sunrise_m[i])
        ss = float(sunset_m[i])
        dc = float(dawn_c_m[i])
        uc = float(dusk_c_m[i])
        dn = float(dawn_n_m[i])
        un = float(dusk_n_m[i])

        if not np.isfinite(dc) or not np.isfinite(uc):
            dc, uc = sr, ss
        if not np.isfinite(dn) or not np.isfinite(un):
            dn, un = dc, uc

        if ss < sr:
            ss += 24.0 * 60.0
        if uc < ss:
            uc += 24.0 * 60.0
        if un < uc:
            un += 24.0 * 60.0
        if dc > sr:
            dc -= 24.0 * 60.0
        if dn > dc:
            dn -= 24.0 * 60.0

        if dn > dc:
            dn = dc
        if un < uc:
            un = uc

        col = np.zeros(len(minutes), dtype=np.float32)

        def paint(start_m: float, end_m: float, value: float) -> None:
            if not np.isfinite(start_m) or not np.isfinite(end_m):
                return
            start_m = float(start_m)
            end_m = float(end_m)
            if end_m <= start_m:
                return
            if (end_m - start_m) >= (24.0 * 60.0):
                col[:] = value
                return
            s = start_m % (24.0 * 60.0)
            e = end_m % (24.0 * 60.0)
            if s < e:
                col[(minutes >= s) & (minutes < e)] = value
            else:
                col[(minutes >= s)] = value
                col[(minutes < e)] = value

        paint(dn, dc, nautical_v)
        paint(dc, sr, civil_v)
        paint(sr, ss, daylight_v)
        paint(ss, uc, civil_v)
        paint(uc, un, nautical_v)
        grid[:, i] = col

    fig, ax = plt.subplots(figsize=(12, 6))
    fig.subplots_adjust(bottom=0.22, top=0.87, left=0.09, right=0.91)

    grid_plot = _gaussian_blur2d(grid, sigma_y=smooth_sigma_minutes, sigma_x=smooth_sigma_days) if smooth else grid

    ax.imshow(
        grid_plot,
        origin="lower",
        aspect="auto",
        interpolation="bilinear",
        extent=[0, days, 0, 24],
        cmap=cmap,
        vmin=0.0,
        vmax=1.0,
        zorder=1,
    )
    ax.axhline(12.0, color=ETI_GOLD, linewidth=1.2, alpha=0.65, zorder=2)

    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.set_xlim(0, days)
    ax.set_ylim(0, 24)

    month_mids = _month_midpoints(year)
    ax.set_xticks([d for d, _ in month_mids])
    ax.set_xticklabels([m for _, m in month_mids])
    ax.set_yticks(np.arange(2, 24, 2))

    ax.yaxis.grid(True, color=GRID_GREY, linewidth=0.8)
    ax.xaxis.grid(False)
    ax.tick_params(axis="both", length=0)

    for tick in ax.get_xticklabels() + ax.get_yticklabels():
        tick.set_color(MUTED_GREY)

    key_handles = [
        Patch(facecolor=NIGHT, edgecolor="none", label="Night: No natural light"),
        Patch(facecolor=nautical_c, edgecolor="none", label="Nautical: Low visibility"),
        Patch(facecolor=civil_c, edgecolor="none", label="Civil: Moderate visibility"),
        Patch(facecolor=DAYLIGHT, edgecolor="none", label="Daylight: Full visibility"),
    ]
    ax.legend(
        handles=key_handles,
        loc="upper right",
        frameon=True,
        facecolor="white",
        edgecolor=GRID_GREY,
        framealpha=0.85,
        fontsize=9,
        labelcolor=MUTED_GREY,
        borderpad=0.6,
        handlelength=1.0,
        handletextpad=0.6,
    )

    ax.set_title(
        chart_title or f"{year} Sun Graph for {inputs.display_name}",
        loc="left",
        x=HEADER_X,
        pad=TITLE_PAD,
        color=TEXT_GREY,
        wrap=True,
    )
    ax.text(
        HEADER_X,
        SUBTITLE_Y,
        chart_subtitle or f"Rise/set times and twilight bands (nautical/civil) • {inputs.timezone_id}",
        transform=ax.transAxes,
        fontsize=12,
        color=MUTED_GREY,
        ha="left",
        wrap=True,
    )
    ax.plot(
        [HEADER_X, 1.00],
        [RULE_Y, RULE_Y],
        transform=ax.transAxes,
        color=ETI_GOLD,
        linewidth=4,
        clip_on=False,
    )

    fig.text(0.09, 0.14, source_left, fontsize=9, color=MUTED_GREY, ha="left")
    fig.text(0.91, 0.14, brand_right, fontsize=9, color=MUTED_GREY, ha="right")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=160, bbox_inches="tight")
