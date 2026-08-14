package io.pulseforge.controlplane.results;

import io.pulseforge.common.domain.RunStatus;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Everything measured for a run, aggregated across the whole fleet.
 *
 * @param droppedSamples  measurements taken but never shipped. Non-zero means the percentiles below
 *                        are computed from an incomplete population — reported rather than hidden.
 * @param skippedRequests requests the generators could not issue. Non-zero means the offered rate
 *                        was not achieved and throughput understates what was asked for.
 * @param unstoredSamples measurements that were shipped, reached the ingestor, and were never
 *                        stored. Kept apart from {@code droppedSamples} because the two call for
 *                        different fixes: one says the worker cannot ship as fast as it measures,
 *                        the other says the ingestor or ClickHouse cannot keep up with the fleet.
 * @param workers         how many distinct workers contributed measurements
 */
public record RunResults(
        UUID runId,
        RunStatus status,
        String statusReason,
        Instant startedAt,
        Instant finishedAt,
        long totalRequests,
        long totalErrors,
        double errorRatePercent,
        double throughputPerSecond,
        double p50Ms,
        double p95Ms,
        double p99Ms,
        double maxMs,
        long droppedSamples,
        long skippedRequests,
        long unstoredSamples,
        int workers,
        List<StepResult> steps) {

    /**
     * True when the measurement is sound — nothing was dropped, skipped or lost on the way to
     * storage — regardless of whether the assertions passed. False means the percentiles above
     * describe an incomplete population, which is a reason to re-run rather than to trust the
     * verdict.
     */
    public boolean isComplete() {
        return totalRequests > 0
                && droppedSamples == 0
                && skippedRequests == 0
                && unstoredSamples == 0;
    }
}
