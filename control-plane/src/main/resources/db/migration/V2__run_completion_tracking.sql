-- Completion tracking that survives redelivery, and the two instants the run's close depends on.
--
-- `finished_workers` was a counter incremented per WorkerFinished message. NATS delivers at least
-- once, so a redelivered completion could push the counter to the expected total while a shard was
-- still missing — reporting COMPLETED for exactly the loss the watchdog exists to catch. The
-- identities are now recorded, and the counter is derived from them.

ALTER TABLE test_runs ADD COLUMN finished_worker_ids TEXT;

COMMENT ON COLUMN test_runs.finished_worker_ids IS
    'Comma-separated ids of workers that reported completion. The set, not the count, is what '
    'decides whether every shard was accounted for.';

-- Set when the last expected shard reports. The run is not closed at that instant: its final
-- snapshots are still travelling to ClickHouse, so the close waits out the settle delay.
ALTER TABLE test_runs ADD COLUMN all_shards_reported_at TIMESTAMPTZ;

-- Recorded when the watchdog first sees a shard missing, while the run is still going. The run
-- finishes the duration that was asked for and carries this into its terminal status.
ALTER TABLE test_runs ADD COLUMN degraded_reason TEXT;
