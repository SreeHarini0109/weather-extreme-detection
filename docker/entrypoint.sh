#!/usr/bin/env bash
# ROLE=master -> sshd + Spark master (:7077, UI :8080); ROLE=worker -> sshd + Spark worker.
# Defaults suit docker-compose.yml (bridge network, host names master/worker1..3). For the lab LAN
# (docker/lan/, host networking) set SSH_PORT (e.g. 2222, so the PC's own sshd keeps port 22),
# SPARK_MASTER_HOST (the master PC's IP) and SPARK_LOCAL_IP (this PC's IP).
set -e
/usr/sbin/sshd -p "${SSH_PORT:-22}"
SPARK_CLASS=$(python3 -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'bin', 'spark-class'))")
MASTER_HOST="${SPARK_MASTER_HOST:-master}"
[[ -n "$SPARK_LOCAL_IP" ]] && export SPARK_LOCAL_IP
if [[ "$ROLE" == "master" ]]; then
  exec "$SPARK_CLASS" org.apache.spark.deploy.master.Master --host "$MASTER_HOST" --port 7077 --webui-port 8080
else
  until (echo > "/dev/tcp/$MASTER_HOST/7077") 2>/dev/null; do echo "waiting for Spark master $MASTER_HOST:7077"; sleep 2; done
  exec "$SPARK_CLASS" org.apache.spark.deploy.worker.Worker "spark://$MASTER_HOST:7077" \
       --cores "${SPARK_WORKER_CORES:-2}" --memory "${SPARK_WORKER_MEMORY:-3g}" --webui-port 8081
fi
