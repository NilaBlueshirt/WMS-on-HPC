#!/usr/bin/env python3
"""plot_fig3_sdiag.py — Slurm controller-side load on Phoenix \texttt{public}.

Single figure, 2x2 panels, one per metric, three blocks overlaid.
All four panels are cluster-global on Phx (includes other users); the
useful comparison is block-to-block consistency.

Panels:
  (a) Main schedule cycles/min      — Δ(`Total cycles`) per minute
  (b) Backfill last-cycle latency   — `Last cycle` (μs → s), snapshot
  (c) Submit RPC rate (per minute)  — Δ(`REQUEST_SUBMIT_BATCH_JOB count`)
  (d) Polling RPC rate (per minute) — Δ(`REQUEST_JOB_INFO_SINGLE` +
                                      `REQUEST_JOB_USER_INFO`)

Cumulative counters (a/c/d) are differentiated between consecutive
samples, so no `sdiag --reset` is required. Missing RPC entries are
treated as 0 (sdiag omits unused RPC types from the table).

Per-minute rates are smoothed with a 10-min time-based rolling median
to suppress sampling-interval noise, and the x-axis is clipped to the
first 24 h of each block's sampler window.

Expected layout:
    bench/phx/block1/sdiag/sdiag_<unixtime>.txt
    bench/phx/block2/sdiag/sdiag_<unixtime>.txt
    bench/phx/block3/sdiag/sdiag_<unixtime>.txt

usage: plot_fig3_sdiag.py [bench_root] [out.png]
       defaults: bench  fig_sdiag.png
"""

import re
import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("bench")
OUT  = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("fig_sdiag.png")

PHX_SHADES = {
    "block1": "#984EA3",  # purple
    "block2": "#0072B2",  # Okabe-Ito blue
    "block3": "#33A02C",  # grass green
}

CUTOFF_MIN = 24.0 * 60.0   # x-axis: clip each block to first 24 h
ROLL_WINDOW = "10min"      # rolling-median window for spike suppression

TS_FROM_NAME = re.compile(r"sdiag_(\d+)\.txt$")

# Main schedule "Total cycles:" — cumulative counter. Differentiated to
# get cycles/min over each inter-sample interval. Anchored on the
# "Main schedule statistics" section to avoid grabbing the backfill
# "Total cycles" line.
MAIN_TOTAL_CYC_RE = re.compile(
    r"Main schedule statistics.*?Total cycles:\s+(\d+)", re.S
)
# Backfill "Last cycle:" in μs — instantaneous snapshot of the most
# recent backfill pass duration. Anchored on "Backfilling stats".
BF_LAST_CYC_RE = re.compile(
    r"Backfilling stats.*?Last cycle:\s+(\d+)", re.S
)
# Cumulative RPC counts. sdiag omits unused RPC types, so a None match
# is interpreted as 0 by the caller.
RPC_RES = {
    "submit":   re.compile(r"REQUEST_SUBMIT_BATCH_JOB\s*\(\s*\d+\)\s*count:(\d+)"),
    "job_single": re.compile(r"REQUEST_JOB_INFO_SINGLE\s*\(\s*\d+\)\s*count:(\d+)"),
    "job_user":   re.compile(r"REQUEST_JOB_USER_INFO\s*\(\s*\d+\)\s*count:(\d+)"),
}


def parse_sdiag_file(path):
    """Return dict with ts and parsed fields, or None on bad file."""
    m_ts = TS_FROM_NAME.search(str(path))
    if not m_ts:
        return None
    ts = int(m_ts.group(1))
    try:
        txt = Path(path).read_text(errors="replace")
    except OSError:
        return None

    def _i(rx):
        m = rx.search(txt)
        return int(m.group(1)) if m else None

    def _i0(rx):
        m = rx.search(txt)
        return int(m.group(1)) if m else 0

    return {
        "ts": ts,
        "main_total_cyc": _i(MAIN_TOTAL_CYC_RE),
        "bf_last_cyc_us": _i(BF_LAST_CYC_RE),
        "submit_count":   _i0(RPC_RES["submit"]),
        "polling_count":  _i0(RPC_RES["job_single"]) + _i0(RPC_RES["job_user"]),
    }


def load_series(sdiag_dir):
    """Return DataFrame with columns:
       rel_min, main_cpm, bf_last_s, submit_rpm, polling_rpm

    Cumulative fields (main_total_cyc, submit_count, polling_count) are
    differentiated between consecutive samples and normalised to per-minute.
    A reset/restart inside the window produces a negative delta — those
    rows are dropped.
    """
    rows = []
    for f in sorted(Path(sdiag_dir).glob("sdiag_*.txt")):
        parsed = parse_sdiag_file(f)
        if parsed and parsed["main_total_cyc"] is not None:
            rows.append(parsed)
    if len(rows) < 2:
        return pd.DataFrame(columns=["rel_min", "main_cpm", "bf_last_s",
                                     "submit_rpm", "polling_rpm"])
    df = pd.DataFrame(rows).sort_values("ts").reset_index(drop=True)

    dt_s = df["ts"].diff()
    dt_min = dt_s / 60.0

    d_main = df["main_total_cyc"].diff()
    d_submit = df["submit_count"].diff()
    d_polling = df["polling_count"].diff()

    df["main_cpm"]    = (d_main    / dt_min).where(d_main    >= 0)
    df["submit_rpm"]  = (d_submit  / dt_min).where(d_submit  >= 0)
    df["polling_rpm"] = (d_polling / dt_min).where(d_polling >= 0)
    df["bf_last_s"]   = df["bf_last_cyc_us"] / 1e6

    t0 = df["ts"].iloc[0]
    df["rel_min"] = (df["ts"] - t0) / 60.0

    out = df.iloc[1:][["rel_min", "main_cpm", "bf_last_s",
                       "submit_rpm", "polling_rpm"]].reset_index(drop=True)

    # Clip each block to the first CUTOFF_MIN minutes of its sampler window.
    out = out[out["rel_min"] <= CUTOFF_MIN].reset_index(drop=True)
    if out.empty:
        return out

    # Time-based rolling median on the four rate/snapshot columns.
    # Uses a TimedeltaIndex so the window is honest under irregular sampling.
    cols = ["main_cpm", "bf_last_s", "submit_rpm", "polling_rpm"]
    tidx = pd.TimedeltaIndex(out["rel_min"] * 60, unit="s")
    rolled = out[cols].copy()
    rolled.index = tidx
    rolled = rolled.rolling(ROLL_WINDOW, center=True, min_periods=1).median()
    out[cols] = rolled.values

    return out


PANELS = [
    ("main_cpm",    "Main sched cycles/min",          "(a)"),
    ("bf_last_s",   "Backfill last-cycle latency (s)", "(b)"),
    ("submit_rpm",  "Submit RPC rate (per min)",      "(c)"),
    ("polling_rpm", "Polling RPC rate (per min)",     "(d)"),
]


def collect_phx_series(root):
    out = []
    if not (root / "phx").exists():
        return out
    for block_dir in sorted((root / "phx").iterdir()):
        sd = block_dir / "sdiag"
        if not sd.is_dir():
            continue
        df = load_series(sd)
        if not df.empty:
            out.append((block_dir.name, df))
    return out


def render(series_list, label_fn, color_map, default_color, xlabel, outpath):
    """Render a 2x2 sdiag figure from a list of (key, df) series."""
    fig, axes = plt.subplots(2, 2, figsize=(10, 6.5), sharex=True)
    axes_flat = axes.flatten()

    for ax, (col, ylabel, tag) in zip(axes_flat, PANELS):
        for key, df in series_list:
            ax.plot(df["rel_min"], df[col],
                    color=color_map.get(key, default_color), lw=1.8,
                    label=label_fn(key))
        ax.set_title(f"{tag} {ylabel}", fontsize=10, loc="left")
        ax.grid(True, alpha=0.3)
        ax.set_ylabel(ylabel, fontsize=9)
        ax.set_xlim(0, CUTOFF_MIN)

    axes[1, 0].set_xlabel(xlabel)
    axes[1, 1].set_xlabel(xlabel)

    handles, labels = axes_flat[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="lower center",
                   ncol=min(6, len(handles)), fontsize=8,
                   bbox_to_anchor=(0.5, -0.02))

    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(outpath, dpi=300, bbox_inches="tight")
    print(f"wrote {outpath}")


def main():
    phx_series = collect_phx_series(ROOT)

    if phx_series:
        render(
            phx_series,
            label_fn=lambda blk: f"Phx {blk}",
            color_map=PHX_SHADES,
            default_color="#0072B2",
            xlabel="Wall time from sampler start (min)",
            outpath=OUT,
        )
    else:
        print(f"skipped {OUT}: no Phoenix sdiag data found under {ROOT/'phx'}")


if __name__ == "__main__":
    main()
