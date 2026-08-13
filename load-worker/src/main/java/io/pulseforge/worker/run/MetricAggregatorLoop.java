package io.pulseforge.worker.run;

import io.nats.client.Connection;
import io.pulseforge.common.protocol.HistogramSnapshot;
import io.pulseforge.common.protocol.NatsSubjects;
import io.pulseforge.common.serde.JsonCodec;
import io.pulseforge.worker.metrics.MetricPipeline;
import io.pulseforge.worker.metrics.Sample;
import io.pulseforge.worker.metrics.StepAggregator;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The single writer that turns raw samples into per-interval histogram snapshots.
 *
 * <p>Runs on its own thread and owns every {@link StepAggregator}, which is what lets the
 * aggregators skip synchronisation entirely. Once per snapshot interval it publishes what it has
 * accumulated and resets, so each message describes exactly one window.
 *
 * <p>Publishing here rather than from the request threads is deliberate: the bus is touched a fixed
 * number of times per second, not once per request.
 */
public class MetricAggregatorLoop implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MetricAggregatorLoop.class);

    /** Samples pulled from the queue per drain, to bound the aggregator's own latency. */
    private static final int DRAIN_BATCH = 4096;

    private final UUID runId;
    private final String workerId;
    private final Duration snapshotInterval;
    private final MetricPipeline pipeline;
    private final StepAggregator[] aggregators;
    private final Connection nats;
    private final RunExecution execution;

    private final AtomicBoolean stopped = new AtomicBoolean();
    private long lastDroppedTotal;
    private long lastSkippedTotal;

    public MetricAggregatorLoop(
            UUID runId,
            String workerId,
            Duration snapshotInterval,
            MetricPipeline pipeline,
            StepSelector stepSelector,
            Connection nats,
            RunExecution execution) {
        this.runId = runId;
        this.workerId = workerId;
        this.snapshotInterval = snapshotInterval;
        this.pipeline = pipeline;
        this.nats = nats;
        this.execution = execution;
        this.aggregators =
                stepSelector.steps().stream()
                        .map(step -> new StepAggregator(step.name()))
                        .toArray(StepAggregator[]::new);
    }

    @Override
    public void run() {
        Instant windowStart = Instant.now();
        long nextFlushNanos = System.nanoTime() + snapshotInterval.toNanos();
        List<Sample> batch = new ArrayList<>(DRAIN_BATCH);

        while (!stopped.get()) {
            try {
                Sample first = pipeline.poll(50, TimeUnit.MILLISECONDS);
                if (first != null) {
                    batch.add(first);
                    pipeline.drainTo(batch, DRAIN_BATCH - 1);
                    for (Sample sample : batch) {
                        aggregators[sample.stepIndex()].record(sample.latencyMicros(), sample.failed());
                    }
                    batch.clear();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            if (System.nanoTime() >= nextFlushNanos) {
                Instant windowEnd = Instant.now();
                publish(windowStart, windowEnd);
                windowStart = windowEnd;
                nextFlushNanos = System.nanoTime() + snapshotInterval.toNanos();
            }
        }

        // Final flush: whatever landed after the last interval still belongs to the run.
        drainRemaining();
        publish(windowStart, Instant.now());
        log.debug("Aggregator loop for run {} stopped", runId);
    }

    private void drainRemaining() {
        List<Sample> batch = new ArrayList<>(DRAIN_BATCH);
        while (pipeline.drainTo(batch, DRAIN_BATCH) > 0) {
            for (Sample sample : batch) {
                aggregators[sample.stepIndex()].record(sample.latencyMicros(), sample.failed());
            }
            batch.clear();
        }
    }

    /**
     * Emits one snapshot per step. Counters that the worker tracks as running totals (dropped,
     * skipped) are published as deltas so that summing snapshots gives the true run total.
     */
    private void publish(Instant windowStart, Instant windowEnd) {
        long droppedTotal = pipeline.droppedSamples();
        long droppedDelta = droppedTotal - lastDroppedTotal;
        lastDroppedTotal = droppedTotal;

        long skippedTotal = execution.skippedRequests();
        long skippedDelta = skippedTotal - lastSkippedTotal;
        lastSkippedTotal = skippedTotal;

        for (int i = 0; i < aggregators.length; i++) {
            StepAggregator aggregator = aggregators[i];

            // Fleet-wide counters are attributed to the first step rather than duplicated across
            // every step, so a SUM over the run does not multiply them by the step count.
            long dropped = i == 0 ? droppedDelta : 0;
            long skipped = i == 0 ? skippedDelta : 0;

            HistogramSnapshot snapshot =
                    new HistogramSnapshot(
                            runId,
                            workerId,
                            aggregator.stepName(),
                            windowStart,
                            windowEnd,
                            aggregator.requestCount(),
                            aggregator.errorCount(),
                            dropped,
                            skipped,
                            aggregator.minMicros(),
                            aggregator.maxMicros(),
                            aggregator.sumMicros(),
                            aggregator.encodeHistogram());
            aggregator.reset();

            if (snapshot.isEmpty()) {
                continue;
            }
            try {
                nats.publish(NatsSubjects.METRICS_SNAPSHOTS, JsonCodec.encode(snapshot));
            } catch (RuntimeException e) {
                // Losing a snapshot must not kill the run; the ingestor will show the gap.
                log.error("Failed to publish snapshot for run {} step {}", runId, aggregator.stepName(), e);
            }
        }
    }

    public void stop() {
        stopped.set(true);
    }
}
