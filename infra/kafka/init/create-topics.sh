#!/bin/sh
# Idempotent topic creation. transactions has infinite retention: it is the Kappa source of truth
# (production would use tiered storage to S3 instead of a local log).
set -eu
B="${KAFKA_BOOTSTRAP:-kafka:9092}"
K=/opt/kafka/bin/kafka-topics.sh

$K --bootstrap-server "$B" --create --if-not-exists --topic transactions --partitions 6 --replication-factor 1 \
   --config retention.ms=-1 --config retention.bytes=-1 --config segment.bytes=268435456
$K --bootstrap-server "$B" --create --if-not-exists --topic merchants --partitions 1 --replication-factor 1 \
   --config cleanup.policy=compact --config min.cleanable.dirty.ratio=0.1
$K --bootstrap-server "$B" --create --if-not-exists --topic late-events --partitions 1 --replication-factor 1 \
   --config retention.ms=-1
$K --bootstrap-server "$B" --create --if-not-exists --topic rules-control --partitions 1 --replication-factor 1
echo "[kafka-init] topics:"
$K --bootstrap-server "$B" --describe | grep -E '^Topic:' || true
