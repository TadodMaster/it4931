CREATE DATABASE IF NOT EXISTS lk;

-- Bookkeeping shared by both architectures; serving tables are created per phase.
CREATE TABLE IF NOT EXISTS lk.lambda_batch_runs
(
    run_id        String,
    t_k           DateTime64(3, 'UTC'),
    rule_version  LowCardinality(String),
    snapshot_id   String,
    view_version  String,
    started_at    DateTime64(3, 'UTC'),
    duration_ms   UInt64,
    input_rows    UInt64,
    status        LowCardinality(String)
) ENGINE = MergeTree ORDER BY (started_at);
