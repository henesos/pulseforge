package io.pulseforge.worker.metrics;

/**
 * A single completed request measurement, on its way from the HTTP completion callback to the
 * aggregator thread.
 *
 * @param stepIndex     which scenario step produced it
 * @param latencyMicros time from the request's <em>intended</em> send time to response completion.
 *                      Measuring from the intended rather than the actual send time is what keeps
 *                      coordinated omission out of the histogram: when the generator falls behind,
 *                      that lateness is real latency the user would have experienced.
 * @param failed        true for a transport error or a 4xx/5xx response
 */
public record Sample(int stepIndex, long latencyMicros, boolean failed) {}
