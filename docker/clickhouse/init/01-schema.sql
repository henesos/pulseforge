-- PulseForge analytical schema.
--
-- Design note (this is the heart of the measurement layer):
-- workers never send one row per HTTP request. Each worker aggregates locally into an
-- HdrHistogram and ships a snapshot once per interval. Two shapes of that snapshot are stored:
--
--   * metric_snapshots  - counters plus the serialized histogram, one row per worker/step/window.
--   * latency_buckets   - the histogram exploded into (bucket, count) pairs.
--
-- The bucket table is what makes correct global percentiles possible. Percentiles are computed by
-- summing counts across every worker and walking the cumulative distribution, NOT by averaging the
-- per-worker percentiles: the mean of five p99 values is not the p99 of the combined population.

CREATE DATABASE IF NOT EXISTS pulseforge;

CREATE TABLE IF NOT EXISTS pulseforge.metric_snapshots
(
    run_id          UUID,
    worker_id       LowCardinality(String),
    step_name       LowCardinality(String),
    window_start    DateTime64(3, 'UTC'),
    window_end      DateTime64(3, 'UTC'),

    request_count   UInt64,
    error_count     UInt64,
    -- Samples the worker measured but could not enqueue for shipping. Never hidden: a run that
    -- dropped samples is reported with the count, because silently losing data is worse than
    -- admitting an incomplete sample.
    dropped_samples UInt64,
    -- Requests the generator was scheduled to send but could not, because the in-flight ceiling
    -- was reached. A non-zero value means the offered rate was not actually achieved.
    skipped_requests UInt64 DEFAULT 0,

    min_micros      UInt64,
    max_micros      UInt64,
    sum_micros      UInt64,

    -- Base64 HdrHistogram, kept for exact re-aggregation and debugging.
    histogram       String CODEC(ZSTD(3)),

    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (run_id, step_name, worker_id, window_start)
TTL toDateTime(window_start) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS pulseforge.latency_buckets
(
    run_id        UUID,
    step_name     LowCardinality(String),
    window_start  DateTime64(3, 'UTC'),
    -- Upper bound of the HdrHistogram bucket, in microseconds.
    bucket_micros UInt64,
    count         UInt64
)
ENGINE = SummingMergeTree(count)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (run_id, step_name, window_start, bucket_micros)
TTL toDateTime(window_start) + INTERVAL 30 DAY;

-- Convenience view: per-run totals without touching the bucket table.
CREATE VIEW IF NOT EXISTS pulseforge.run_totals AS
SELECT
    run_id,
    step_name,
    min(window_start)                                  AS started_at,
    max(window_end)                                    AS finished_at,
    sum(request_count)                                 AS requests,
    sum(error_count)                                   AS errors,
    sum(dropped_samples)                               AS dropped,
    sum(skipped_requests)                              AS skipped,
    if(sum(request_count) = 0, 0,
       sum(error_count) * 100.0 / sum(request_count))  AS error_rate_percent,
    if(sum(request_count) = 0, 0,
       sum(sum_micros) / sum(request_count) / 1000.0)  AS mean_latency_ms,
    max(max_micros) / 1000.0                           AS max_latency_ms,
    uniqExact(worker_id)                               AS workers
FROM pulseforge.metric_snapshots
GROUP BY run_id, step_name;
