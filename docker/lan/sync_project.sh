#!/usr/bin/env bash
# Copy the project (code + Parquet data) from the master PC to every worker PC at the same path, so
# every MPI rank and Spark executor can read its partitions locally. Needs ssh access to the PCs.
#   bash docker/lan/sync_project.sh user@192.168.1.11 user@192.168.1.12 user@192.168.1.13
set -euo pipefail
cd "$(dirname "$0")/../.."
[[ $# -ge 1 ]] || { echo "usage: $0 user@WORKER_IP [...]"; exit 1; }
for target in "$@"; do
  echo "-> $target:$PWD"
  ssh "$target" "mkdir -p '$PWD'"
  rsync -az --delete --exclude results/ --exclude '__pycache__' ./ "$target:$PWD/"
done
