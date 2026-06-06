#!/usr/bin/env python3
"""check_dev_repeat.py — determinism check for the Dev reference run.

Compares the Dev reference run against a same-input repeat and prints the
max relative deviation on the quantities Section 6 cites (LASTZ-stage
walltime, total sbatch, total submit/polling RPCs, total main cycles).

usage: check_dev_repeat.py [REF_RUN_DIR] [REPEAT_RUN_DIR]
  each dir contains {original,jobarray,refactor}/trace.txt and .../sdiag/
  defaults: bench/dev/sample1  bench/dev_repeat/sample1
"""

import sys
from pathlib import Path

# Reuse the exact parsers the figures use, so the check reads the data the
# same way the published numbers are produced.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from plot_fig2_sbatch_rate import parse_trace, CONFIG_ORDER, CONFIG_LABELS  # noqa: E402
from plot_fig3_sdiag import parse_sdiag_file  # noqa: E402

REF = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("bench/dev/sample1")
REP = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("bench/dev_repeat/sample1")


def rel_dev(a, b):
    """Relative deviation |a-b| / max(|a|,|b|); None if undefined."""
    if a is None or b is None:
        return None
    m = max(abs(a), abs(b))
    return None if m == 0 else abs(a - b) / m


def trace_metrics(run_dir, cfg):
    """(LASTZ walltime h, total distinct sbatch) from a config's trace.txt."""
    tp = run_dir / cfg / "trace.txt"
    if not tp.is_file():
        return None, None
    s = parse_trace(tp)
    if s.empty:
        return None, None
    return float(s.index[-1]), float(s.iloc[-1])


# Cumulative sdiag counters compared as window totals (last - first).
SDIAG_COUNTERS = {
    "total submit RPCs":  "submit_count",
    "total polling RPCs": "polling_count",
    "total main cycles":  "main_total_cyc",
}


def sdiag_totals(run_dir, cfg):
    """Window total (last - first) of each cumulative sdiag counter.

    A counter that decreases between consecutive samples (reset / restart /
    corrupt sample in the window) is dropped and named in `skipped`, rather
    than emitting a bogus negative total. Returns (totals, skipped)."""
    sd = run_dir / cfg / "sdiag"
    if not sd.is_dir():
        return {}, []
    rows = [parse_sdiag_file(f) for f in sorted(sd.glob("sdiag_*.txt"))]
    rows = [r for r in rows if r and r["main_total_cyc"] is not None]
    if len(rows) < 2:
        return {}, []
    rows.sort(key=lambda r: r["ts"])

    totals, skipped = {}, []
    for metric, key in SDIAG_COUNTERS.items():
        vals = [r[key] for r in rows]
        if any(b < a for a, b in zip(vals, vals[1:])):
            skipped.append(metric)  # reset / restart / corrupt sample in window
        else:
            totals[metric] = float(vals[-1] - vals[0])
    return totals, skipped


MIN_ROBUST_N = 1000  # below this an integer counter's % deviation is mostly
                     # granularity noise; shown but flagged, never headlined.


def _fmt(v):
    return f"{v:.4g}" if isinstance(v, (int, float)) else v


def main():
    if not REF.exists() or not REP.exists():
        print(f"waiting for data: REF={REF} (exists={REF.exists()}), "
              f"REP={REP} (exists={REP.exists()})")
        return

    # Headline = walltime only; counts are diagnostic (small-N RPC counts
    # carry housekeeping jitter, so they never drive the headline number).
    walltime_devs = []
    for cfg in CONFIG_ORDER:
        print(f"\n{CONFIG_LABELS[cfg]}:")
        wt_r, ct_r = trace_metrics(REF, cfg)
        wt_p, ct_p = trace_metrics(REP, cfg)

        d = rel_dev(wt_r, wt_p)
        if d is None:
            print(f"  {'LASTZ walltime (h)':24s} ref={_fmt(wt_r)} rep={_fmt(wt_p)}"
                  "  (n/a from trace; use the run's recorded walltime)")
        else:
            print(f"  {'LASTZ walltime (h)':24s} ref={_fmt(wt_r)} rep={_fmt(wt_p)}"
                  f"  delta={d*100:.2f}%")
            walltime_devs.append(d)

        support = {"total sbatch": (ct_r, ct_p)}
        tot_r, skip_r = sdiag_totals(REF, cfg)
        tot_p, skip_p = sdiag_totals(REP, cfg)
        for k in tot_r:
            if k in tot_p:
                support[k] = (tot_r[k], tot_p[k])
        for name, (a, b) in support.items():
            d = rel_dev(a, b)
            if d is None:
                note = "(n/a)"
            elif max(abs(a), abs(b)) < MIN_ROBUST_N:
                note = f"delta={d*100:.2f}%  (small-N: granularity noise)"
            else:
                note = f"delta={d*100:.2f}%"
            print(f"  {name:24s} ref={_fmt(a)} rep={_fmt(b)}  {note}")
        for k in sorted(set(skip_r) | set(skip_p)):
            sides = "+".join(s for s, sk in (("ref", skip_r), ("rep", skip_p)) if k in sk)
            print(f"  {k:24s} SKIPPED (counter reset/restart in {sides} window)")

    print()
    if walltime_devs:
        worst = max(walltime_devs) * 100
        print(f"Walltime reproducibility (headline): within {worst:.2f}%")
        print(f'Paper sentence: "the reference run reproduced within {worst:.0f}% '
              'on a same-input repeat."')
    else:
        print("Walltime n/a from the traces (e.g. the repeat dir has no trace.txt).")
        print("Take the headline number from the runs' recorded walltimes; the counts")
        print("above are diagnostic, and small-N RPC deltas are granularity noise.")


if __name__ == "__main__":
    main()
