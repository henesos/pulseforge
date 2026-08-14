package io.pulseforge.worker.run;

import io.pulseforge.common.protocol.HistogramSnapshot;
import io.pulseforge.worker.metrics.MetricPipeline;
import io.pulseforge.worker.metrics.Sample;
import io.pulseforge.worker.metrics.SnapshotTransport;
import io.pulseforge.worker.metrics.StepAggregator;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
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
    private final SnapshotTransport transport;
    private final RunExecution execution;

    private final AtomicBoolean stopped = new AtomicBoolean();
    private final CountDownLatch finished = new CountDownLatch(1);
    private long lastDroppedTotal;
    private long lastSkippedTotal;
    /** Counters from windows that never left the worker, owed to the next snapshot. */
    private long carriedDropped;
    private long carriedSkipped;

    public MetricAggregatorLoop(
            UUID runId,
            String workerId,
            Duration snapshotInterval,
            MetricPipeline pipeline,
            StepSelector stepSelector,
            SnapshotTransport transport,
            RunExecution execution) {
        this.runId = runId;
        this.workerId = workerId;
        this.snapshotInterval = snapshotInterval;
        this.pipeline = pipeline;
        this.transport = transport;
        this.execution = execution;
        this.aggregators =
                stepSelector.steps().stream()
                        .map(step -> new StepAggregator(step.name()))
                        .toArray(StepAggregator[]::new);
    }

    @Override
    public void run() {
        try {
            loop();
        } finally {
            // Whoever is waiting on the final flush must be released even if the loop died, or a
            // failure here would hang the worker's completion path instead of reporting.
            finished.countDown();
        }
    }

    private void loop() {
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
            } catch (RuntimeException e) {
                // A failed publish or a malformed sample must not end the loop. Losing this thread
                // would leave the generator running with nothing recording it, and the run would
                // still report as though it had been measured.
                log.error("Aggregator loop for run {} hit an error, continuing", runId, e);
                batch.clear();
            }

            if (System.nanoTime() >= nextFlushNanos) {
                Instant windowEnd = Instant.now();
                try {
                    publish(windowStart, windowEnd);
                } catch (RuntimeException e) {
                    log.error("Run {}: failed to publish a snapshot window", runId, e);
                }
                windowStart = windowEnd;
                // Advanced by a whole interval rather than measured from now, so the time spent
                // publishing does not stretch every subsequent window.
                nextFlushNanos += snapshotInterval.toNanos();
                if (System.nanoTime() >= nextFlushNanos) {
                    nextFlushNanos = System.nanoTime() + snapshotInterval.toNanos();
                }
            }
        }

        // Final flush: whatever landed after the last interval still belongs to the run.
        try {
            drainRemaining();
            publish(windowStart, Instant.now());
        } catch (RuntimeException e) {
            log.error("Run {}: final snapshot flush failed", runId, e);
        }
        // Only after the final snapshot is sent, so a streaming transport does not close its
        // stream on data it has not shipped yet.
        transport.runFinished(runId);
        log.debug("Aggregator loop for run {} stopped", runId);
    }

    /**
     * Blocks until the loop has published its final window.
     *
     * <p>The worker announces completion only after this returns: the control plane closes the run
     * on that message, and a report read before the last snapshot shipped is short by one interval.
     *
     * @return false if the flush did not complete within {@code timeout}
     */
    public boolean awaitFinalFlush(Duration timeout) throws InterruptedException {
        return finished.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
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
     *
     * <p>A delta is only true once. If the snapshot carrying it never leaves the worker, the count
     * it described is subtracted from the run's report permanently — the running total has already
     * moved past it, so no later window will mention it again. Anything a failed send was carrying
     * is therefore kept and added to the next window, and the measurements that died with the
     * window are counted as the dropped samples they now are.
     */
    private void publish(Instant windowStart, Instant windowEnd) {
        long droppedTotal = pipeline.droppedSamples();
        long droppedDelta = droppedTotal - lastDroppedTotal + carriedDropped;
        lastDroppedTotal = droppedTotal;

        long skippedTotal = execution.skippedRequests();
        long skippedDelta = skippedTotal - lastSkippedTotal + carriedSkipped;
        lastSkippedTotal = skippedTotal;

        long undeliveredDropped = 0;
        long undeliveredSkipped = 0;

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
            if (!transport.send(snapshot)) {
                // The window's measurements are gone — the aggregator was already reset — so they
                // become dropped samples, which is exactly what they are: taken and never reported.
                undeliveredDropped += snapshot.requestCount() + snapshot.droppedSamples();
                undeliveredSkipped += snapshot.skippedRequests();
            }
        }

        carriedDropped = undeliveredDropped;
        carriedSkipped = undeliveredSkipped;
    }

    public void stop() {
        stopped.set(true);
    }
}
