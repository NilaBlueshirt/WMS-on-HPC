#!/usr/bin/env python3
"""plot_fig2_sbatch_rate.py — cumulative `sbatch` invocations vs wall time.

Writes two single-panel PNGs (one per cluster):
  <out>_phx.png  Phoenix public, full pipeline, N=3 blocks: thin per-block
                 curves + bold per-config average.
  <out>_dev.png  Dev, LASTZ stage, one bold curve per config (N=1).

Reads Nextflow trace.txt. sbatch count = number of distinct Slurm array-base
IDs (tasks 12345_1, 12345_2, ... share base 12345 = one sbatch).

Expected layout:
    bench/phx/block{1,2,3}/{original,jobarray,refactor}/trace.txt
    bench/dev/sample1/{original,jobarray,refactor}/trace.txt

usage: plot_fig2_sbatch_rate.py [bench_root] [out.png]
       defaults: bench  fig_sbatch_rate.png
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe

# White halo behind annotation text so labels stay readable where they
# cross a step line.
LABEL_STROKE = [pe.withStroke(linewidth=2.5, foreground="white", alpha=0.9)]

ROOT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("bench")
OUT  = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("fig_sbatch_rate.png")

CUTOFF_H = 24.0  # uniform 24 h walltime cap

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
CONFIG_LABELS = {
    "original": "Original",
    "jobarray": "Original + Job Array",
    "refactor": "nf-core refactor",
}
CONFIG_ORDER = ["original", "jobarray", "refactor"]


def parse_trace(path):
    """trace.txt -> sorted Series indexed by hours-from-first-submit,
    values = cumulative sbatch count. Clipped to CUTOFF_H."""
    t = pd.read_csv(path, sep="\t")
    if "native_id" not in t.columns or "submit" not in t.columns:
        raise ValueError(f"{path}: trace.txt missing required columns native_id/submit")
    t["array_base"] = t["native_id"].astype(str).str.split("_").str[0]
    t["submit_dt"]  = pd.to_datetime(t["submit"])
    events = t.groupby("array_base")["submit_dt"].min().sort_values()
    t0 = events.iloc[0]
    rel_h = (events - t0).dt.total_seconds().values / 3600.0
    cum   = np.arange(1, len(events) + 1)
    s = pd.Series(cum, index=rel_h)
    return s[s.index <= CUTOFF_H]


def average_curve(curves, n_grid=400):
    """Average across cumulative step curves on a common x-grid. Curves are
    held at their final value past their own endpoint (cumulative plateau).
    Returns (avg_series, avg_endpoint_x = mean of per-curve endpoint x's)."""
    if not curves:
        return pd.Series(dtype=float), 0.0
    endpoints = [float(c.index.max()) for c in curves]
    xmax = min(max(endpoints), CUTOFF_H)
    if xmax <= 0:
        return pd.Series(dtype=float), 0.0
    grid = np.linspace(0, xmax, n_grid)
    Y = []
    for c in curves:
        y = np.interp(grid, c.index, c.values, left=0)
        Y.append(y)
    avg = pd.Series(np.mean(np.array(Y), axis=0), index=grid)
    avg_endpoint_x = min(float(np.mean(endpoints)), CUTOFF_H)
    return avg, avg_endpoint_x


def collect_phx(root):
    out = {cfg: [] for cfg in CONFIG_ORDER}
    phx = root / "phx"
    if not phx.exists():
        return out
    for block_dir in sorted(phx.iterdir()):
        if not block_dir.is_dir():
            continue
        block_label = block_dir.name.replace("block", "") or block_dir.name
        for cfg in CONFIG_ORDER:
            tp = block_dir / cfg / "trace.txt"
            if tp.is_file():
                out[cfg].append((block_label, tp))
    return out


def collect_dev(root):
    out = {cfg: [] for cfg in CONFIG_ORDER}
    dev = root / "dev"
    if not dev.exists():
        return out
    for sample_dir in sorted(dev.iterdir()):
        if not sample_dir.is_dir():
            continue
        sample_label = sample_dir.name.replace("sample", "") or sample_dir.name
        for cfg in CONFIG_ORDER:
            tp = sample_dir / cfg / "trace.txt"
            if tp.is_file():
                out[cfg].append((sample_label, tp))
    return out


def _panel(ax, files, label_runs=True, label_walltime=True, fit_x=False):
    """One cumulative-sbatch panel: thin per-run curves + bold per-config
    averages. `label_runs` toggles the "#<run>" labels (Phoenix); `label_walltime`
    toggles the "<h>h" walltime label; `fit_x` fits the x-axis to the data span
    (Dev) instead of the 24 h cap."""
    any_drawn = False
    max_x = 0.0
    for cfg in CONFIG_ORDER:
        items = files.get(cfg, [])
        if not items:
            continue
        any_drawn = True
        light = CONFIG_LIGHT[cfg]
        dark  = CONFIG_DARK[cfg]
        labelled = [(lbl, parse_trace(p)) for lbl, p in items]
        # Sort non-empty thin curves by endpoint x so the #N stagger
        # follows endpoint order left-to-right, which keeps the column
        # of labels visually adjacent to its own curve.
        thin = [(lbl, c) for lbl, c in labelled if not c.empty]
        thin_sorted = sorted(enumerate(thin), key=lambda t: float(t[1][1].index[-1]))
        rank_by_orig = {orig_i: rank for rank, (orig_i, _) in enumerate(thin_sorted)}
        for i, (lbl, c) in enumerate(thin):
            ax.step(c.index, c.values, where="post", color=light, lw=1.8)
            max_x = max(max_x, float(c.index[-1]))
            if label_runs:
                # Endpoints nearly coincide on the log axis, so stack the
                # labels tightly and draw a thin leader line back to each
                # endpoint to keep the association.
                rank = rank_by_orig[i]
                dy_pts = -3 - 10 * rank
                ax.annotate(
                    f"#{lbl}", xy=(c.index[-1], c.values[-1]),
                    xytext=(14, dy_pts), textcoords="offset points",
                    color=dark, fontsize=7, fontweight="bold",
                    ha="left", va="center",
                    arrowprops=dict(arrowstyle="-", color=dark, lw=0.5,
                                    alpha=0.55, shrinkA=0, shrinkB=2),
                    path_effects=LABEL_STROKE,
                )
        curves = [c for _, c in thin]
        if len(curves) > 1:
            avg, avg_x = average_curve(curves)
            if not avg.empty:
                ax.step(avg.index, avg.values, where="post",
                        color=dark, lw=2.5,
                        label=f"{CONFIG_LABELS[cfg]} (average, N={len(curves)})")
                if label_walltime:
                    idx = int(np.argmin(np.abs(avg.index - avg_x)))
                    # Walltime label sits just above the bold endpoint —
                    # close to its own curve, on the opposite side of the
                    # endpoint from the #N column below, so the two
                    # families never collide.
                    ax.annotate(f"{avg_x:.2f}h",
                                xy=(avg.index[idx], avg.values[idx]),
                                xytext=(4, 5), textcoords="offset points",
                                color=dark, fontsize=9, fontweight="bold",
                                ha="left", va="bottom",
                                path_effects=LABEL_STROKE)
        elif len(curves) == 1:
            c = curves[0]
            ax.step(c.index, c.values, where="post",
                    color=dark, lw=2.0,
                    label=f"{CONFIG_LABELS[cfg]} (N=1)")
            max_x = max(max_x, float(c.index[-1]))
            if label_walltime:
                wt = float(c.index[-1])
                ax.annotate(f"{wt:.2f}h", xy=(c.index[-1], c.values[-1]),
                            xytext=(4, 5), textcoords="offset points",
                            color=dark, fontsize=9, fontweight="bold",
                            ha="left", va="bottom",
                            path_effects=LABEL_STROKE)

    ax.set_ylabel("Cumulative `sbatch` invocations")
    ax.set_yscale("log")
    # On the fitted (Dev) panel leave a right margin so the endpoint
    # walltime labels are not clipped at the spine.
    margin = 1.15 if fit_x else 1.0
    ax.set_xlim(0, (max_x * margin if (fit_x and max_x > 0) else CUTOFF_H))
    if label_runs or label_walltime:
        # Headroom so the endpoint labels (walltime above, #N below) stay
        # inside the plot box.
        ymin, ymax = ax.get_ylim()
        ax.set_ylim(ymin, ymax * 1.6)
    if any_drawn:
        ax.legend(loc="best", fontsize=7)
    ax.grid(True, which="both", alpha=0.3)


def _single(files, out, label_runs=True, label_walltime=True, fit_x=False):
    """Render one cluster's panel to its own single-column figure."""
    fig, ax = plt.subplots(figsize=(5.2, 3.6))
    _panel(ax, files, label_runs=label_runs, label_walltime=label_walltime,
           fit_x=fit_x)
    ax.set_xlabel("Wall time from first submission (h)")
    fig.tight_layout()
    fig.savefig(out, dpi=300)
    print(f"wrote {out}")
    plt.close(fig)


def main():
    phx_files = collect_phx(ROOT)
    dev_files = collect_dev(ROOT)

    # Fig. 2(a): Phoenix -> fig_sbatch_rate_phx.png. Per-block "#1/#2/#3"
    # plus the per-config average walltime ("<h>h", two decimals).
    phx_out = OUT.with_name(OUT.stem + "_phx" + OUT.suffix)
    _single(phx_files, phx_out, label_runs=True, label_walltime=True,
            fit_x=False)

    # Fig. 2(b): Dev -> fig_sbatch_rate_dev.png. No per-run "#N" labels, but
    # each config keeps its walltime ("<h>h"); x-axis fitted to the short
    # LASTZ-stage span.
    dev_out = OUT.with_name(OUT.stem + "_dev" + OUT.suffix)
    _single(dev_files, dev_out, label_runs=False, label_walltime=True,
            fit_x=True)


if __name__ == "__main__":
    main()