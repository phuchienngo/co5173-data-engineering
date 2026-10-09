#!/usr/bin/env bash
# Submit a PySpark job to the standalone cluster from inside the pyspark container
# Usage: docker exec pyspark bash /opt/jobs/submit.sh /opt/jobs/<job>.py [job args]

set -euo pipefail

# pyspark skips bitnami's entrypoint, so uid 1001 has no passwd entry and Hadoop's login
# fails with "invalid null input: name"; register it via nss_wrapper like the entrypoint does
if ! id -un >/dev/null 2>&1; then
	echo "spark:x:$(id -u):$(id -g):Spark:/opt/bitnami/spark:/bin/false" > "$NSS_WRAPPER_PASSWD"
	echo "spark:x:$(id -g):" > "$NSS_WRAPPER_GROUP"
	export LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so
fi

# bitnami runs as uid 1001 without HOME; default ivy path fails
# executors on spark-worker connect back to the driver by container name
# TLC timestamps are NYC wall-clock; UTC session keeps hours unshifted
exec /opt/bitnami/spark/bin/spark-submit \
	--master "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
	--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,org.apache.kafka:kafka-clients:3.9.0 \
	--conf spark.jars.ivy=/tmp/.ivy2 \
	--conf spark.driver.host=pyspark \
	--conf spark.sql.ansi.enabled=false \
	--conf spark.sql.session.timeZone=UTC \
	"$@"
