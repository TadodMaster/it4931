# Spark 3.5 (local mode) with Iceberg + Kafka + S3A jars baked in; used for archiver, master loader and nightly batch.
FROM apache/spark:3.5.6-java17-python3
USER root
ARG MVN=https://repo1.maven.org/maven2
RUN set -eux; cd /opt/spark/jars; \
    for u in \
      org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.0/iceberg-spark-runtime-3.5_2.12-1.10.0.jar \
      org/apache/iceberg/iceberg-aws-bundle/1.10.0/iceberg-aws-bundle-1.10.0.jar \
      org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.6/spark-sql-kafka-0-10_2.12-3.5.6.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.6/spark-token-provider-kafka-0-10_2.12-3.5.6.jar \
      org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar; do \
      curl -fsSLO "$MVN/$u"; \
    done
RUN pip3 install --no-cache-dir redis==5.0.8 pyyaml==6.0.3 pytest==9.1.1
ENV PYTHONPATH=/app/spark:/app/python/common:/app/python/rules-python PYTHONUNBUFFERED=1 \
    LK_CONFIG=/app/shared/config/demo.yaml DATA_ROOT=/data AWS_REGION=us-east-1
WORKDIR /app/spark
COPY shared /app/shared
COPY python/common /app/python/common
COPY python/rules-python /app/python/rules-python
COPY python/tests /app/python/tests
COPY python/pytest.ini /app/python/pytest.ini
COPY spark /app/spark
