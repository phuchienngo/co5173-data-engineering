# Shared Spark image for master, worker, and client.
# All roles use the same Python version to keep PySpark execution compatible.
# ════════════════════════════════════════════════════════════════════════
#
#      "Python in worker has different version 3.8 than that in driver 3.11"
# ════════════════════════════════════════════════════════════════════════
FROM python:3.10-slim-bookworm

ARG SPARK_VERSION=3.5.1
ARG SCALA_VERSION=2.12
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1
ARG CLICKHOUSE_JDBC_VERSION=0.6.5

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl ca-certificates procps tini \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "pyspark==${SPARK_VERSION}" \
    && pip install --no-cache-dir -r /tmp/requirements.txt

ENV SPARK_HOME=/usr/local/lib/python3.10/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# ──────────────────────────────────────────────────────────────────────
#
#
# ──────────────────────────────────────────────────────────────────────
ARG M2=https://repo1.maven.org/maven2
RUN set -eux; \
    cd "${SPARK_HOME}/jars"; \
    curl -fsSLO "${M2}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"; \
    curl -fsSLO "${M2}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_VERSION}-${SPARK_VERSION}.jar"; \
    curl -fsSLO "${M2}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fsSLO "${M2}/org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    curl -fsSL -o "clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar" \
        "${M2}/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar"

COPY docker/spark/entrypoint.sh /opt/spark-entrypoint.sh
RUN chmod +x /opt/spark-entrypoint.sh

WORKDIR /opt/jobs
ENV PYTHONPATH=/opt/jobs

RUN mkdir -p /opt/data/checkpoints /opt/data/spark-events

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/opt/spark-entrypoint.sh", "master"]
