#!/usr/bin/env bash
# Run on the master PC after every node is started: MPI reaches every PC, Spark sees every worker,
# and an end-to-end run matches the sequential engine.
#   bash docker/lan/check_cluster.sh 192.168.1.10 [NP]
set -euo pipefail
cd "$(dirname "$0")/../.."
MASTER_IP=${1:?usage: $0 MASTER_IP [NP]}
HOSTS=$(grep -cv '^#' docker/lan/hostfile)
NP=${2:-$((HOSTS * 2))}
echo "== 1. MPI: one rank per PC should print $HOSTS different host names"
docker exec pds-node mpirun --hostfile /app/docker/lan/hostfile -np "$HOSTS" --map-by node hostname
echo "== 2. Spark: registered workers"
curl -s "http://$MASTER_IP:8080/json/" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('aliveworkers'), 'alive workers,', d.get('cores'), 'cores')"
echo "== 3. End-to-end: sequential vs MPI ($NP ranks) vs Spark on the cluster, then parity"
docker exec pds-node python3 -m engines.sequential --out /app/results/lan --quiet --tag lan
docker exec pds-node mpirun --hostfile /app/docker/lan/hostfile -np "$NP" python3 -m engines.mpi_engine \
    --mode station --out /app/results/lan --tag lan
docker exec pds-node python3 -m engines.spark_engine --master "spark://$MASTER_IP:7077" --out /app/results/lan --tag lan
docker exec pds-node python3 -m tests.parity /app/results/lan/sequential_latest /app/results/lan/mpi_latest
docker exec pds-node python3 -m tests.parity /app/results/lan/sequential_latest /app/results/lan/spark_latest
