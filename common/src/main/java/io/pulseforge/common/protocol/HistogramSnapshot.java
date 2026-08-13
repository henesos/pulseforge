package io.pulseforge.common.protocol;

import java.time.Instant;
import java.util.UUID;

/**
 * One interval of measurements from one worker, for one scenario step.
 *
 * <p>This is the message that makes the whole design work. A worker emits one of these per second
 * per step instead of one message per request, so bus traffic is {@code workers × steps} per
 * second and is independent of the request rate.
 *
 * @param histogramBase64 base64 of HdrHistogram's compressed encoding. Carrying the distribution
 *                        rather than pre-computed percentiles is what lets the ingestor merge
 *                        shards into a correct global percentile later.
 * @param droppedSamples  measurements that were taken but could not be enqueued for aggregation.
 *                        Reported, never hidden.
 * @param skippedRequests requests the generator could not issue because the in-flight ceiling was
 *                        reached — the offered rate was not achieved.
 */
public record HistogramSnapshot(
        UUID runId,
        String workerId,
        String stepName,
        Instant windowStart,
        Instant windowEnd,
        long requestCount,
        long errorCount,
        long droppedSamples,
        long skippedRequests,
        long minMicros,
        long maxMicros,
        long sumMicros,
        String histogramBase64) {

    public boolean isEmpty() {
        return requestCount == 0 && droppedSamples == 0 && skippedRequests == 0;
    }
}
