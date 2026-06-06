#!/usr/bin/env python3
"""plot_fig3_sdiag_ecdf.py — ECDF view of Phoenix controller-side load.

Companion to plot_fig3_sdiag.py: same four cluster-global sdiag metrics on
Phoenix public, but each 2x2 panel is an empirical CDF per block (built from
raw per-sample values, no smoothing). Each block clipped to its first 24 h.

Expected layout: bench/phx/block{1,2,3}/sdiag/sdiag_<unixtime>.txt

usage: plot_fig3_sdiag_ecdf.py [bench_root] [out.png]
       defaults: bench  fig_sdiag_ecdf.png
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Reuse parsing + constants from the time-series script so both figures
# read the same files in the same way.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from plot_fig3_sdiag import CUTOFF_MIN, PHX_SHADES, parse_sdiag_file  # noqa: E402

ROOT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("bench")
OUT  = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("fig_sdiag_ecdf.png")


def load_raw(sdiag_dir):
    """Raw per-sample rates over the first CUTOFF_MIN minutes (no smoothing)."""
    rows = []
    for f in sorted(Path(sdiag_dir).glob("sdiag_*.txt")):
        parsed = parse_sdiag_file(f)
        if parsed and parsed["main_total_cyc"] is not None:
            rows.append(parsed)
    if len(rows) < 2:
        return pd.DataFrame(columns=["rel_min", "main_cpm", "bf_last_s",
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
    df["rel_min"] = (df["ts"] - t0) / 60.0

    out = df.iloc[1:][["rel_min", "main_cpm", "bf_last_s",
                       "submit_rpm", "polling_rpm"]].reset_index(drop=True)
    return out[out["rel_min"] <= CUTOFF_MIN].reset_index(drop=True)


def collect_phx_raw(root):
    out = []
    phx = root / "phx"
    if not phx.exists():
        return out
    for block_dir in sorted(phx.iterdir()):
        sd = block_dir / "sdiag"
        if not sd.is_dir():
            continue
        df = load_raw(sd)
        if not df.empty:
            out.append((block_dir.name, df))
    return out


PANELS = [
    ("main_cpm",    "Main sched cycles/min",          "(a)", False),
    ("bf_last_s",   "Backfill last-cycle latency (s)", "(b)", False),
    ("submit_rpm",  "Submit RPC rate (per min)",      "(c)", False),
    ("polling_rpm", "Polling RPC rate (per min)",     "(d)", True),
]


def ecdf(values):
    """Sorted x and step y = i/n for an empirical CDF."""
    v = np.asarray(values, dtype=float)
    v = v[~np.isnan(v)]
    v = np.sort(v)
    n = len(v)
    if n == 0:
        return np.array([]), np.array([])
    y = np.arange(1, n + 1) / n
    return v, y


def _panel_xlim(series_list, col):
    """Cap x at the combined 99th percentile across blocks so a single
    long tail in one block doesn't compress the visible curves."""
    pooled = np.concatenate([
        np.asarray(df[col].dropna(), dtype=float) for _, df in series_list
    ])
    if pooled.size == 0:
        return None
    return float(np.percentile(pooled, 99))


def main():
    series = collect_phx_raw(ROOT)
    if not series:
        print(f"skipped {OUT}: no Phoenix sdiag data found under {ROOT/'phx'}")
        return

    fig, axes = plt.subplots(2, 2, figsize=(10, 6.5))
    axes_flat = axes.flatten()

    for ax, (col, xlabel, tag, use_log) in zip(axes_flat, PANELS):
        for blk, df in series:
            x, y = ecdf(df[col])
            if len(x) == 0:
                continue
            ax.step(x, y, where="post",
                    color=PHX_SHADES.get(blk, "#0072B2"), lw=1.8,
                    label=f"Phx {blk} (n={len(x)})")

        ax.set_title(f"{tag} {xlabel}", fontsize=10, loc="left")
        ax.set_xlabel(xlabel, fontsize=9)
        ax.set_ylabel("F(x)", fontsize=9)
        ax.set_ylim(0, 1.01)

        # Reference lines: median and p95.
        ax.axhline(0.5,  color="grey", lw=0.6, ls="--", alpha=0.6)
        ax.axhline(0.95, color="grey", lw=0.6, ls=":",  alpha=0.6)

        if use_log:
            # polling_rpm has a heavy tail (occasional ~1000+ bursts);
            # symlog keeps small/zero values readable. Pin the left edge at
            # 0 (rates are non-negative) so the panel starts from zero.
            ax.set_xscale("symlog", linthresh=10)
            ax.set_xlim(left=0)
        else:
            xmax = _panel_xlim(series, col)
            if xmax is not None and xmax > 0:
                ax.set_xlim(left=0, right=xmax)

        ax.grid(True, alpha=0.3)

    handles, labels = axes_flat[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="lower center",
                   ncol=min(6, len(handles)), fontsize=8,
                   bbox_to_anchor=(0.5, -0.02))

    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(OUT, dpi=300, bbox_inches="tight")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
