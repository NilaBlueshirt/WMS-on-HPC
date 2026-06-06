#!/usr/bin/env python3
"""plot_fig1_walltime.py — full-pipeline walltime, Phoenix public (N=3).

Required CSV columns: cluster,config,rep,walltime_h  (only cluster=="phx" plotted).
Optional columns:
    walltime_supervised_h  supervised-completion walltime; overlaid as an open
                           marker on a dashed line. Blank if completed unattended.
    censored  0/1; 1 = hit the walltime cap (walltime_h = cap), drawn as an
              upward triangle to denote a lower bound.

usage: plot_fig1_walltime.py <walltime.csv> [out.png]
"""

import sys
import pandas as pd
import matplotlib.pyplot as plt

CSV = sys.argv[1] if len(sys.argv) > 1 else "walltime.csv"
OUT = sys.argv[2] if len(sys.argv) > 2 else "fig_walltime.png"

CONFIGS = ["original", "jobarray", "refactor"]
CONFIG_LABELS = {
    "original": "Original",
    "jobarray": "Original + Job Array",
    "refactor": "nf-core refactor",
}
# Okabe-Ito with light/dark shades.
# Light = box face. Dark = individual rep dots.
CONFIG_LIGHT = {
    "original": "#ff9966",
    "jobarray": "#f7e780",
    "refactor": "#66d4b4",
}
CONFIG_DARK = {
    "original": "#b34700",
    "jobarray": "#b8a800",
    "refactor": "#00644a",
}


def _is_set(v):
    """Truthy and not NaN — for optional-column flags."""
    return pd.notna(v) and bool(v)


def _effective(row):
    """Supervised-completion walltime if present, else headline walltime_h."""
    ys = row.get("walltime_supervised_h") if "walltime_supervised_h" in row else None
    if pd.notna(ys):
        return ys
    return row.get("walltime_h")


def _phx_panel(ax, df, has_supervised, has_censored):
    """Phx full-pipeline walltime: box + individual rep dots + mean line."""
    phx = df[df["cluster"] == "phx"]
    box_data = [
        phx[phx["config"] == c]["walltime_h"].dropna().values for c in CONFIGS
    ]
    bp = ax.boxplot(
        box_data,
        positions=range(len(CONFIGS)),
        widths=0.45,
        patch_artist=True,
        medianprops=dict(color="black", linewidth=2),
        flierprops=dict(marker=""),
    )
    for patch, c in zip(bp["boxes"], CONFIGS):
        patch.set_facecolor(CONFIG_LIGHT[c])
        patch.set_alpha(0.85)

    # Mean line per box, computed on effective walltime (supervised if
    # present, else headline). Drawn across the box width so it reads
    # like a boxplot stat rather than a data point.
    mean_drawn = False
    for i, c in enumerate(CONFIGS):
        sub = phx[phx["config"] == c]
        eff = sub.apply(_effective, axis=1).dropna()
        if eff.empty:
            continue
        mu = eff.mean()
        ax.plot(
            [i - 0.225, i + 0.225], [mu, mu],
            color="navy", linestyle="-.", linewidth=1.5, zorder=4,
            label=(None if mean_drawn else "mean (effective walltime)"),
        )
        ax.annotate(
            f"μ={mu:.1f}", xy=(i - 0.225, mu), xytext=(-3, 0),
            textcoords="offset points",
            fontsize=7, color="navy",
            ha="right", va="center",
        )
        mean_drawn = True

    censored_done = False
    supervised_done = False
    rep_value_lists = []
    for i, c in enumerate(CONFIGS):
        sub = phx[phx["config"] == c].sort_values("rep")
        x = i + 0.15
        val_strs = []
        for _, row in sub.iterrows():
            y = row.get("walltime_h")
            rep = row.get("rep")
            if pd.isna(y):
                continue
            is_censored = has_censored and _is_set(row.get("censored"))
            marker = "^" if is_censored else "o"
            ax.scatter(
                [x], [y],
                color=CONFIG_DARK[c], edgecolor="black",
                s=45 if is_censored else 35,
                marker=marker, zorder=3,
                label=("censored at cap (≥ lower bound)"
                       if is_censored and not censored_done else None),
            )
            if pd.notna(rep):
                ax.annotate(
                    f"{int(rep)}", xy=(x, y), xytext=(5, 0),
                    textcoords="offset points",
                    fontsize=7, color=CONFIG_DARK[c],
                    ha="left", va="center",
                )
            val_strs.append(f"≥{y:.1f}" if is_censored else f"{y:.1f}")
            if is_censored:
                censored_done = True

            if has_supervised:
                ys = row.get("walltime_supervised_h")
                if pd.notna(ys):
                    ax.plot(
                        [x, x], [y, ys],
                        color=CONFIG_DARK[c], linestyle="--",
                        linewidth=0.8, zorder=2,
                    )
                    ax.scatter(
                        [x], [ys],
                        facecolors="none", edgecolors=CONFIG_DARK[c],
                        s=45, linewidth=1.2, zorder=3,
                        label=("supervised completion"
                               if not supervised_done else None),
                    )
                    if pd.notna(rep):
                        ax.annotate(
                            f"{int(rep)}", xy=(x, ys), xytext=(5, 0),
                            textcoords="offset points",
                            fontsize=7, color=CONFIG_DARK[c],
                            ha="left", va="center",
                        )
                    supervised_done = True
        rep_value_lists.append("[" + ", ".join(val_strs) + "] h" if val_strs else "")

    ax.set_xticks(range(len(CONFIGS)))
    ax.set_xticklabels(
        [f"{CONFIG_LABELS[c]}\n{rep_value_lists[i]}" for i, c in enumerate(CONFIGS)]
    )
    ax.set_ylabel("Full-pipeline walltime (h)")
    if censored_done or supervised_done or mean_drawn:
        ax.legend(loc="best", fontsize=8)
    ax.grid(True, axis="y", alpha=0.3)


def main():
    df = pd.read_csv(CSV)
    has_supervised = "walltime_supervised_h" in df.columns
    has_censored = "censored" in df.columns
    if "walltime_h" not in df.columns:
        sys.exit("error: CSV must contain walltime_h")

    fig, ax = plt.subplots(figsize=(6, 4))
    _phx_panel(ax, df, has_supervised, has_censored)

    fig.tight_layout()
    fig.savefig(OUT, dpi=300)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
