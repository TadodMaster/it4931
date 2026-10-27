#!/bin/bash
# Whole benchmark on a running stack (make reset first). Usage: scripts/bench_all.sh [RUN_ID]   (about 1 hour on the fast profile)
# Scenarios run ONE AT A TIME; a failed scenario is logged and the sequence continues.
RUN_ID="${1:-final}"
PROFILES="--profile core --profile kappa --profile lambda --profile lambda-tools --profile lambda-load"
B="docker compose $PROFILES exec -T bench python -m lk_bench"
step() { echo; echo "=== [$(date +%H:%M:%S)] $*"; "$@" || echo "!!! step failed: $*"; }

step $B meta --run-id "$RUN_ID"
step $B run atomic --run-id "$RUN_ID"
step $B run determinism --run-id "$RUN_ID"
step $B run b1 --run-id "$RUN_ID" --repeat 3
step $B run b8 --run-id "$RUN_ID" --repeat 3
step $B run b2 --run-id "$RUN_ID"
step $B run b9 --run-id "$RUN_ID"
step $B run b3 --run-id "$RUN_ID"
step $B run b4 --run-id "$RUN_ID"
step $B run b5 --run-id "$RUN_ID"
# quiet system: corrective batch, master statistics, exact comparison of both architectures with the oracle
step $B run batch --run-id "$RUN_ID"
step $B run master-stats --run-id "$RUN_ID"
step $B verify --arch kappa --rule v1 --run-id "$RUN_ID"
step $B verify --arch lambda --run-id "$RUN_ID"
step $B run b7 --run-id "$RUN_ID"
step $B run b6 --run-id "$RUN_ID"
step $B run b10 --run-id "$RUN_ID"
echo; echo "=== [$(date +%H:%M:%S)] bench-all finished"
