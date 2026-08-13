package io.pulseforge.worker.metrics;

import io.pulseforge.common.metrics.HistogramCodec;
import org.HdrHistogram.Histogram;

/**
 * Per-step latency accumulator for the current snapshot interval.
 *
 * <p>Backed by an HdrHistogram rather than a list of samples: memory is bounded and constant
 * regardless of request count, recording is O(1), and the distribution stays intact so a real
 * global p99 can be merged later.
 *
 * <p>Not thread safe by design. Exactly one aggregator thread writes here; that is what makes
 * recording cheap enough to sit in the hot path.
 */
public class StepAggregator {

    private final String stepName;
    private Histogram histogram = HistogramCodec.newHistogram();
    private long requestCount;
    private long errorCount;
    private long sumMicros;

    public StepAggregator(String stepName) {
        this.stepName = stepName;
    }

    public void record(long latencyMicros, boolean failed) {
        long clamped = Math.min(Math.max(latencyMicros, 1), HistogramCodec.MAX_TRACKABLE_MICROS);
        histogram.recordValue(clamped);
        requestCount++;
        sumMicros += clamped;
        if (failed) {
            errorCount++;
        }
    }

    public String stepName() {
        return stepName;
    }

    public long requestCount() {
        return requestCount;
    }

    public long errorCount() {
        return errorCount;
    }

    public long sumMicros() {
        return sumMicros;
    }

    public long minMicros() {
        return requestCount == 0 ? 0 : histogram.getMinValue();
    }

    public long maxMicros() {
        return requestCount == 0 ? 0 : histogram.getMaxValue();
    }

    public String encodeHistogram() {
        return HistogramCodec.encode(histogram);
    }

    /**
     * Starts a fresh interval, so each snapshot describes its own window rather than a running
     * total. Called by the publisher immediately after it has read the values out.
     */
    public void reset() {
        histogram = HistogramCodec.newHistogram();
        requestCount = 0;
        errorCount = 0;
        sumMicros = 0;
    }
}
