#!/usr/bin/env bash
# Start this PC's node of the lab cluster.
#   ROLE=master bash docker/lan/start_node.sh 192.168.1.10            (on the master PC)
#   ROLE=worker bash docker/lan/start_node.sh 192.168.1.10            (on each worker PC)
# Optional env: NODE_IP (auto-detected), LAN_SUBNET (derived from NODE_IP as a /24),
#               SPARK_WORKER_CORES (4), SPARK_WORKER_MEMORY (6g)
set -euo pipefail
cd "$(dirname "$0")/../.."
MASTER_IP=${1:?usage: ROLE=master|worker $0 <MASTER_IP>}
: "${ROLE:?set ROLE=master or ROLE=worker}"
if [[ -z "${NODE_IP:-}" ]]; then
  if command -v ip >/dev/null; then NODE_IP=$(ip route get "$MASTER_IP" | awk '{for(i=1;i<=NF;i++) if($i=="src") print $(i+1)}' | head -1)
  else NODE_IP=$(ipconfig getifaddr en0 2>/dev/null || hostname -I | awk '{print $1}'); fi
fi
LAN_SUBNET=${LAN_SUBNET:-$(echo "$NODE_IP" | awk -F. '{print $1"."$2"."$3".0/24"}')}
echo "role=$ROLE node=$NODE_IP master=$MASTER_IP subnet=$LAN_SUBNET"
export ROLE MASTER_IP NODE_IP LAN_SUBNET
docker compose -f docker/lan/docker-compose.lan.yml up -d --build
docker ps --filter name=pds-node --format '{{.Names}}: {{.Status}}'
