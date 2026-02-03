from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np

from app.weather.weather_chart import ETI_BLUE, ETI_GOLD, GRID_GREY, MUTED_GREY, TEXT_GREY, MONTHS


@dataclass(frozen=True)
class MonthlyDaylight:
    month: str
    daylight_hours: float


def render_daylight_chart(
    *,
    monthly: Iterable[MonthlyDaylight],
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

    rcParams["font.family"] = "DejaVu Sans"

    months = [m.month for m in monthly]
    hours = np.array([m.daylight_hours for m in monthly], dtype=float)

    x = np.arange(len(months))

    fig, ax = plt.subplots(figsize=(12, 6))
    fig.subplots_adjust(bottom=0.22, top=0.87, left=0.09, right=0.91)

    ax.set_axisbelow(True)
    ax.yaxis.grid(True, color=GRID_GREY, linewidth=0.8)
    ax.xaxis.grid(False)

    for spine in ax.spines.values():
        spine.set_visible(False)

    ax.bar(x, hours, width=0.6, color=ETI_BLUE, alpha=0.7, zorder=2)
    ax.plot(x, hours, color=ETI_GOLD, linewidth=2.5, zorder=3)

    ax.set_ylabel("Daylight (hours)", color=MUTED_GREY)
    ax.set_ylim(0, max(16.0, float(np.nanmax(hours)) + 1.0))

    ax.set_xticks(x)
    ax.set_xticklabels(months)

    ax.set_title(title, loc="left", x=-0.08, pad=40, color=TEXT_GREY, wrap=True)
    ax.text(
        -0.08,
        1.08,
        subtitle,
        transform=ax.transAxes,
        fontsize=12,
        color=MUTED_GREY,
        ha="left",
        wrap=True,
    )
    ax.plot(
        [-0.08, 1.00],
        [1.045, 1.045],
        transform=ax.transAxes,
        color=ETI_GOLD,
        linewidth=4,
        clip_on=False,
    )

    for tick in ax.get_xticklabels() + ax.get_yticklabels():
        tick.set_color(MUTED_GREY)
    ax.tick_params(axis="both", length=0)

    fig.text(0.09, 0.14, source_left, fontsize=9, color=MUTED_GREY, ha="left")
    fig.text(0.91, 0.14, brand_right, fontsize=9, color=MUTED_GREY, ha="right")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=160, bbox_inches="tight")

