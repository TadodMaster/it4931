#!/bin/sh
# Idempotent job submission: does nothing if the job is already running.
#   JOB=kappa  -> kappa-$VERSION (RULE, START_FROM, PARALLELISM)      JOB=lambda -> lambda-speed (RULE)
set -eu
JOB="${JOB:-kappa}"; RULE="${RULE:-v1}"; JM="${JM:-flink-jobmanager:8081}"
if [ "$JOB" = "lambda" ]; then
  NAME="lambda-speed"; JAR=/opt/jobs/lambda-flink-speed.jar
  set -- --rule "$RULE" --parallelism "${PARALLELISM:-1}" --start-from "${START_FROM:-committed}"
else
  VERSION="${VERSION:-$RULE}"; NAME="kappa-${VERSION}"; JAR=/opt/jobs/kappa-flink-job.jar
  set -- --rule "$RULE" --version "$VERSION" --parallelism "${PARALLELISM:-1}" --start-from "${START_FROM:-committed}" \
         --checkpoint-interval-ms "${CHECKPOINT_MS:-30000}"
fi
if flink list -r -m "$JM" 2>/dev/null | grep -q " ${NAME} "; then
  echo "[submit] ${NAME} already running"; exit 0
fi
echo "[submit] starting ${NAME} (rule ${RULE})"
exec flink run -d -m "$JM" "$JAR" "$@"
