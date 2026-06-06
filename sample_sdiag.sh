#!/usr/bin/env bash
# sample_sdiag.sh — dump `sdiag` every 300 s for a fixed duration.
#
# Layout convention (one dir per "run" or "block"):
#   bench/phx/block1/sdiag/sdiag_<unixtime>.txt
#   bench/phx/block2/sdiag/sdiag_<unixtime>.txt
#   bench/phx/block3/sdiag/sdiag_<unixtime>.txt
#   bench/dev/original/sdiag/sdiag_<unixtime>.txt
#   bench/dev/jobarray/sdiag/sdiag_<unixtime>.txt
#   bench/dev/refactor/sdiag/sdiag_<unixtime>.txt
#
# Cadence: 300 s. No `sdiag --reset` is performed (requires SlurmUser
# privilege and adds controller RPC pressure). The plotter differentiates
# cumulative counters between consecutive samples, so absolute reset
# state does not matter — but it does mean the first sample of any run
# has no predecessor and is dropped. Start the sampler ~5 min BEFORE the
# benchmark workload begins so the first useful delta covers t=0.
#
# usage: sample_sdiag.sh <output_dir> <duration_seconds>
#   e.g. sample_sdiag.sh ./bench/phx/block1/sdiag 86400      # 24 h
#   e.g. sample_sdiag.sh ./bench/dev/refactor/sdiag 43200    # 12 h
#
# Run in background so it doesn't block the shell:
#   nohup ./sample_sdiag.sh ./bench/phx/block1/sdiag 86400 > sampler.log 2>&1 &
#   echo $! > sampler.pid    # save PID so you can kill it when the block ends

set -euo pipefail

OUTDIR=${1:?usage: $0 <output_dir> <duration_seconds>}
DURATION=${2:?usage: $0 <output_dir> <duration_seconds>}

mkdir -p "$OUTDIR"

END=$(( $(date +%s) + DURATION ))
while [ $(date +%s) -lt $END ]; do
    TS=$(date +%s)
    sdiag > "$OUTDIR/sdiag_${TS}.txt" 2>&1
    sleep 300
done
