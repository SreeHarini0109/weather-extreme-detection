#!/usr/bin/env bash
# Write docker/lan/hostfile for mpirun from the PCs' IPs (master first).
#   bash docker/lan/make_hostfile.sh --slots 4 192.168.1.10 192.168.1.11 192.168.1.12 192.168.1.13
set -euo pipefail
SLOTS=2
if [[ "${1:-}" == "--slots" ]]; then SLOTS=$2; shift 2; fi
[[ $# -ge 1 ]] || { echo "usage: $0 [--slots N] MASTER_IP [WORKER_IP ...]"; exit 1; }
out="$(dirname "$0")/hostfile"
{ echo "# generated $(date '+%F %T'): $# nodes x $SLOTS slots"; for ip in "$@"; do echo "$ip slots=$SLOTS"; done; } > "$out"
cat "$out"
