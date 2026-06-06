#!/usr/bin/env python3
"""plot_fig4_sdiag_dev.py — Slurm controller-side load on the Dev cluster.

Companion to plot_fig3_sdiag_ecdf.py (Phoenix ECDF). Dev is a single-user
cluster, so cluster-global sdiag counters are directly attributable to
the workflow under test --- no co-tenancy to disentangle. The figure
therefore uses a time-series view (the right lens for attribution) with
the three configurations as the categorical axis rather than the three
measurement blocks.

Each configuration is a single co-tenancy-free reference run windowed to
the LASTZ stage (the dominant submission wave), with per-user Fairshare
(and sdiag counters) reset beforehand. No rolling
smoothing is applied: on a single-user cluster the trace is the
workflow's own attributable controller load, not a mixture with
co-tenant traffic, so the raw rates are the honest view and a repeat of
the same input would reproduce them.

Panels (same metrics as the Phoenix sdiag figures):
  (a) Main schedule cycles/min      -- Delta(`Total cycles`) per minute
  (b) Backfill last-cycle latency   -- `Last cycle` (us -> s), snapshot
  (c) Submit RPC rate (per minute)  -- Delta(`REQUEST_SUBMIT_BATCH_JOB count`)
  (d) Polling RPC rate (per minute) -- Delta(`REQUEST_JOB_INFO_SINGLE` +
                                      `REQUEST_JOB_USER_INFO`)

For each metric panel: one bold curve per configuration (the single
reference run), distinguished by the shared legend (no per-line labels).
If several runs per configuration are supplied, the panel falls back to
thin per-sample curves plus a bold per-config average, matching the
Phoenix convention.

All series are clipped to the first 24 h of run time, matching the
uniform 24 h walltime cap applied across all configurations on both
clusters.

Expected layout:
    bench/dev/sample1/original/sdiag/sdiag_<unixtime>.txt
    bench/dev/sample1/jobarray/sdiag/sdiag_<unixtime>.txt
    bench/dev/sample1/refactor/sdiag/sdiag_<unixtime>.txt
    bench/dev/sample2/...
    bench/dev/sample3/...

usage: plot_fig4_sdiag_dev.py [bench_root] [out.png]
       defaults: bench  fig_sdiag_dev.png
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Reuse the parser from the Phoenix time-series script so both figures
# read sdiag files identically (regexes + counter handling are not
# duplicated).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from plot_fig3_sdiag import parse_sdiag_file  # noqa: E402

ROOT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("bench")
OUT  = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("fig_sdiag_dev.png")

# Same orange/yellow/green palette as plot_fig2_sbatch_rate.py so config
# colour meaning is consistent across all of section 6.
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

CUTOFF_H = 24.0   # uniform 24 h walltime cap (no smoothing -- raw rates)


def load_raw(sdiag_dir):
    """sdiag_*.txt -> DataFrame with rel_h, main_cpm, bf_last_s, submit_rpm,
    polling_rpm. No rolling smoothing. Cumulative counters differentiated
    between consecutive samples; negative deltas (would indicate a reset
    mid-window, which the per-run reset rule precludes) are dropped."""
    rows = []
    for f in sorted(Path(sdiag_dir).glob("sdiag_*.txt")):
        parsed = parse_sdiag_file(f)
        if parsed and parsed["main_total_cyc"] is not None:
            rows.append(parsed)
    if len(rows) < 2:
        return pd.DataFrame(columns=["rel_h", "main_cpm", "bf_last_s",
                                     "submit_rpm", "polling_rpm"])
    df = pd.DataFrame(rows).sort_values("ts").reset_index(drop=True)

    dt_min = df["ts"].diff() / 60.0
    d_main    = df["main_total_cyc"].diff()
    d_submit  = df["submit_count"].diff()
    d_polling = df["polling_count"].diff()

    df["main_cpm"]    = (d_main    / dt_min).where(d_main    >= 0)
    df["submit_rpm"]  = (d_submit  / dt_min).where(d_submit  >= 0)
    df["polling_rpm"] = (d_polling / dt_min).where(d_polling >= 0)
    df["bf_last_s"]   = df["bf_last_cyc_us"] / 1e6

    t0 = df["ts"].iloc[0]
    df["rel_h"] = (df["ts"] - t0) / 3600.0

    out = df.iloc[1:][["rel_h", "main_cpm", "bf_last_s",
                       "submit_rpm", "polling_rpm"]].reset_index(drop=True)
    return out[out["rel_h"] <= CUTOFF_H].reset_index(drop=True)


def collect_dev(root):
    """Return {config: [(sample_label, df), ...]} across bench/dev/."""
    out = {cfg: [] for cfg in CONFIG_ORDER}
    dev = root / "dev"
    if not dev.exists():
        return out
    for sample_dir in sorted(dev.iterdir()):
        if not sample_dir.is_dir():
            continue
        sample_label = sample_dir.name.replace("sample", "") or sample_dir.name
        for cfg in CONFIG_ORDER:
            sd = sample_dir / cfg / "sdiag"
            if not sd.is_dir():
                continue
            df = load_raw(sd)
            if not df.empty:
                out[cfg].append((sample_label, df))
    return out


def average_curve(curves, col, n_grid=400):
    """Pointwise average of metric `col` across N runs on a common x-grid
    spanning [0, min(per-run endpoint, CUTOFF_H)]. Truncated to the
    shortest run so the average is computed only where every run has
    data. Returns (x_grid, y_avg, avg_endpoint_x), where avg_endpoint_x
    is the mean of per-run endpoint x's --- the honest position for the
    average-walltime label."""
    endpoints = [float(c["rel_h"].iloc[-1]) for c in curves if not c.empty]
    if not endpoints:
        return np.array([]), np.array([]), 0.0
    grid_max = min(min(endpoints), CUTOFF_H)
    if grid_max <= 0:
        return np.array([]), np.array([]), 0.0
    grid = np.linspace(0, grid_max, n_grid)
    Y = []
    for c in curves:
        y = np.interp(grid, c["rel_h"].values, c[col].values,
                      left=np.nan, right=np.nan)
        Y.append(y)
    y_avg = np.nanmean(np.array(Y), axis=0)
    avg_endpoint_x = min(float(np.mean(endpoints)), CUTOFF_H)
    return grid, y_avg, avg_endpoint_x


PANELS = [
    ("main_cpm",    "Main sched cycles/min",          "(a)"),
    ("bf_last_s",   "Backfill last-cycle latency (s)", "(b)"),
    ("submit_rpm",  "Submit RPC rate (per min)",      "(c)"),
    ("polling_rpm", "Polling RPC rate (per min)",     "(d)"),
]


def _draw_panel(ax, dev_runs, col):
    """One sdiag metric panel. With a single reference run per
    configuration (N=1), each config is one bold curve labelled with its
    walltime at the endpoint. If several runs are supplied (N>1), they are
    drawn as thin per-sample curves plus a bold per-config average, matching
    the Phoenix convention."""
    any_drawn = False
    for cfg in CONFIG_ORDER:
        runs = dev_runs.get(cfg, [])
        if not runs:
            continue
        any_drawn = True
        dark = CONFIG_DARK[cfg]
        curves = [df for _, df in runs]
        if len(curves) > 1:
            light = CONFIG_LIGHT[cfg]
            for sample_lbl, df in runs:
                ax.plot(df["rel_h"], df[col], color=light, lw=1.2, alpha=0.9)
                ax.text(df["rel_h"].iloc[-1], df[col].iloc[-1], f" {sample_lbl}",
                        color=dark, fontsize=7, fontweight="bold",
                        ha="left", va="center")
            x, y, _avg_x = average_curve(curves, col)
            if len(x):
                ax.plot(x, y, color=dark, lw=2.4,
                        label=f"{CONFIG_LABELS[cfg]} (average, N={len(curves)})")
        else:
            df = curves[0]
            ax.plot(df["rel_h"], df[col], color=dark, lw=2.4,
                    label=f"{CONFIG_LABELS[cfg]}")

    # Rates are non-negative; pin y=0 to the x-axis. Dev has no burst tail,
    # so a linear axis is the honest view (symlog would draw a misleading
    # negative band beneath the near-zero Original/Job Array polling curves).
    ax.set_ylim(bottom=0)
    ax.grid(True, alpha=0.3)
    return any_drawn


def main():
    dev = collect_dev(ROOT)
    if not any(dev.values()):
        print(f"skipped {OUT}: no Dev sdiag data found under {ROOT/'dev'}")
        return

    # Dev runs are windowed to the short LASTZ stage; fit the shared x-axis
    # to the data span so the curves fill the panels (the 24 h cap would
    # otherwise squeeze them into the far left). Matplotlib then auto-picks
    # tick spacing on the order of ~10 min for this range.
    all_x = [df["rel_h"].iloc[-1]
             for runs in dev.values() for _, df in runs if not df.empty]
    xmax = (max(all_x) * 1.08) if all_x else CUTOFF_H

    fig, axes = plt.subplots(2, 2, figsize=(10, 6.5), sharex=True)
    axes_flat = axes.flatten()
    for ax, (col, ylabel, tag) in zip(axes_flat, PANELS):
        _draw_panel(ax, dev, col)
        ax.set_title(f"{tag} {ylabel}", fontsize=10, loc="left")
        ax.set_ylabel(ylabel, fontsize=9)
    axes_flat[0].set_xlim(0, xmax)  # shared (sharex=True) across all panels
    axes[1, 0].set_xlabel("Wall time from sampler start (h)")
    axes[1, 1].set_xlabel("Wall time from sampler start (h)")

    handles, labels = axes_flat[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="lower center",
                   ncol=min(3, len(handles)), fontsize=8,
                   bbox_to_anchor=(0.5, -0.02))

    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(OUT, dpi=300, bbox_inches="tight")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()