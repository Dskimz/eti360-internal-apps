from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np

# Canonical month order used across the pipeline.
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# ---- Tick helpers ----

def _nice_range(lo: float, hi: float, step: float = 5.0, pad_steps: int = 1) -> tuple[float, float, np.ndarray]:
    """Return (vmin, vmax, ticks) for a nice axis range."""
    if not np.isfinite(lo) or not np.isfinite(hi):
        lo, hi = 0.0, 1.0
    if hi < lo:
        lo, hi = hi, lo
    lo_n = np.floor(lo / step) * step
    hi_n = np.ceil(hi / step) * step
    lo_n -= pad_steps * step
    hi_n += pad_steps * step
    ticks = np.arange(lo_n, hi_n + 0.001, step, dtype=float)
    return float(lo_n), float(hi_n), ticks


# ---- Brand palette ----
ETI_BLUE = "#1F4E79"  # temperature + primary signal
ETI_GOLD = "#C9A24D"  # structural accent
GRID_GREY = "#D0D3D6"
TEXT_GREY = "#333333"
MUTED_GREY = "#777777"
FILL_BLUE = "#CFE0F2"

# Header layout (axes coordinates)
HEADER_X = -0.08
TITLE_PAD = 40
SUBTITLE_Y = 1.08
RULE_Y = 1.045


@dataclass(frozen=True)
class MonthlyWeather:
    month: str
    high_c: float
    low_c: float
    precip_cm: float


def _c_to_f(c: float) -> float:
    return (c * 9.0 / 5.0) + 32.0


def render_weather_chart(
    *,
    project_root: Path,
    monthly: Iterable[MonthlyWeather],
    title: str,
    subtitle: str,
    source_left: str,
    brand_right: str = "ETI360",
    output_path: Path,
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib import rcParams

    # Avoid hard-depending on local fonts in server environments.
    rcParams["font.family"] = "DejaVu Sans"

    months = [m.month for m in monthly]
    high_c = np.array([m.high_c for m in monthly], dtype=float)
    low_c = np.array([m.low_c for m in monthly], dtype=float)
    precip_cm = np.array([m.precip_cm for m in monthly], dtype=float)

    x = np.arange(len(months))
    x_smooth = np.linspace(x.min(), x.max(), 300)

    # Smoothing for display only.
    high_coeff = np.polyfit(x, high_c, deg=5)
    low_coeff = np.polyfit(x, low_c, deg=5)
    high_smooth = np.polyval(high_coeff, x_smooth)
    low_smooth = np.polyval(low_coeff, x_smooth)

    fig, ax = plt.subplots(figsize=(12, 6))
    fig.subplots_adjust(bottom=0.22, top=0.87, left=0.09, right=0.91)

    ax.set_axisbelow(True)
    ax.yaxis.grid(True, color=GRID_GREY, linewidth=0.8)
    ax.xaxis.grid(False)

    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.fill_between(x_smooth, low_smooth, high_smooth, color=FILL_BLUE, alpha=0.7, zorder=1)
    ax.plot(x_smooth, high_smooth, color=ETI_BLUE, linewidth=3, zorder=3)
    ax.plot(x_smooth, low_smooth, color=ETI_BLUE, linewidth=3, zorder=3)

    ax.set_ylabel("Temp", color=MUTED_GREY)
    tmin = float(np.nanmin(low_c))
    tmax = float(np.nanmax(high_c))
    ymin, ymax, temp_ticks = _nice_range(tmin, tmax, step=5.0, pad_steps=1)
    ax.set_ylim(ymin, ymax)
    ax.set_yticks(temp_ticks[:-1])
    ax.set_yticklabels([f"{t:.0f}/{_c_to_f(t):.0f}" for t in temp_ticks[:-1]])

    ax2 = ax.twinx()
    ax2.bar(x, precip_cm, width=0.6, color=MUTED_GREY, alpha=0.45, zorder=2)
    ax2.set_ylabel("Precipitation (cm)", color=MUTED_GREY)
    ax2.set_ylim(0, max(10, float(np.nanmax(precip_cm)) + 3))
    precip_ticks = ax2.get_yticks()
    if len(precip_ticks) > 1:
        ax2.set_yticks(precip_ticks[:-1])

    for spine in ax2.spines.values():
        spine.set_visible(False)

    ax.set_title(
        title,
        loc="left",
        x=HEADER_X,
        pad=TITLE_PAD,
        color=TEXT_GREY,
        wrap=True,
    )
    ax.text(
        HEADER_X,
        SUBTITLE_Y,
        subtitle,
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

    ax.set_xticks(x)
    ax.set_xticklabels(months)

    for tick in ax.get_xticklabels() + ax.get_yticklabels():
        tick.set_color(MUTED_GREY)
    for tick in ax2.get_yticklabels():
        tick.set_color(MUTED_GREY)
    ax.tick_params(axis="both", length=0)
    ax2.tick_params(axis="y", length=0)

    fig.text(0.09, 0.14, source_left, fontsize=9, color=MUTED_GREY, ha="left")
    fig.text(0.91, 0.14, brand_right, fontsize=9, color=MUTED_GREY, ha="right")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=160, bbox_inches="tight")
