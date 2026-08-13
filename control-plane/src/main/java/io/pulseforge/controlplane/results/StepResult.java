package io.pulseforge.controlplane.results;

/**
 * Measured outcome for one scenario step.
 *
 * <p>All latencies are milliseconds. Percentiles come from merging every worker's histogram, not
 * from averaging their individual percentiles.
 */
public record StepResult(
        String stepName,
        long requests,
        long errors,
        double errorRatePercent,
        double throughputPerSecond,
        double meanMs,
        double p50Ms,
        double p95Ms,
        double p99Ms,
        double maxMs) {}
