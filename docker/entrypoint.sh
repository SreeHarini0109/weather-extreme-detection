#!/usr/bin/env bash
# ROLE=master -> sshd + Spark master (:7077, UI :8080); ROLE=worker -> sshd + Spark worker.
set -e
/usr/sbin/sshd
SPARK_CLASS=$(python3 -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'bin', 'spark-class'))")
if [[ "$ROLE" == "master" ]]; then
  exec "$SPARK_CLASS" org.apache.spark.deploy.master.Master --host master --port 7077 --webui-port 8080
else
  until (echo > /dev/tcp/master/7077) 2>/dev/null; do sleep 1; done
  exec "$SPARK_CLASS" org.apache.spark.deploy.worker.Worker spark://master:7077 \
       --cores "${SPARK_WORKER_CORES:-2}" --memory "${SPARK_WORKER_MEMORY:-3g}" --webui-port 8081
fi
