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
        int workers,
        List<StepResult> steps) {

    /** True when the measurement itself is suspect, regardless of whether assertions passed. */
    public boolean isComplete() {
        return droppedSamples == 0 && skippedRequests == 0;
    }
}
