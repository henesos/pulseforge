package io.pulseforge.worker.run;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.pulseforge.common.domain.HttpMethod;
import io.pulseforge.common.domain.LoadProfile;
import io.pulseforge.common.domain.Scenario;
import io.pulseforge.common.domain.ScenarioStep;
import io.pulseforge.common.metrics.HistogramCodec;
import io.pulseforge.common.protocol.HistogramSnapshot;
import io.pulseforge.worker.metrics.MetricPipeline;
import io.pulseforge.worker.metrics.Sample;
import io.pulseforge.worker.metrics.SnapshotTransport;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The aggregator loop is the last place a measurement can be lost, double-counted or attributed to
 * the wrong window, and none of those show up as an error — they show up as a number on a
 * dashboard.
 *
 * <p>Two behaviours carry most of the risk. Running totals (dropped samples, skipped requests) are
 * published as per-window deltas and attributed to one step, so that the ingestor's SUM over a run
 * returns the true figure rather than the total multiplied by the number of steps. And the final
 * flush must publish whatever arrived after the last interval before the worker announces it
 * finished, because the control plane closes the run on that announcement.
 *
 * <p>Most cases drive the loop synchronously — stopped before it starts, so {@code run()} takes the
 * final-flush path and nothing depends on timing. The one case that needs real intervals asserts
 * over the sequence of windows rather than over any single one, so a flush boundary landing between
 * two samples cannot make it fail.
 */
class MetricAggregatorLoopTest {

    private static final UUID RUN_ID = UUID.randomUUID();
    private static final String WORKER_ID = "worker-1";
    private static final Duration NO_PERIODIC_FLUSH = Duration.ofHours(1);

    private final RecordingTransport transport = new RecordingTransport();
    private final RunExecution execution = mock(RunExecution.class);

    @Test
    @DisplayName("samples that arrive after the last interval still reach the report")
    void finalFlushPublishesWhatIsLeftInTheQueue() {
        MetricPipeline pipeline = new MetricPipeline(64);
        pipeline.offer(new Sample(0, 1_200, false));
        pipeline.offer(new Sample(0, 2_400, true));
        pipeline.offer(new Sample(1, 800, false));

        runToCompletion(loopOver(pipeline, NO_PERIODIC_FLUSH));

        assertThat(transport.snapshotFor("list-products").requestCount()).isEqualTo(2);
        assertThat(transport.snapshotFor("list-products").errorCount()).isEqualTo(1);
        assertThat(transport.snapshotFor("checkout").requestCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("the stream is closed only after the last snapshot has been handed over")
    void runFinishedComesAfterTheFinalSend() {
        MetricPipeline pipeline = new MetricPipeline(64);
        pipeline.offer(new Sample(0, 1_200, false));

        runToCompletion(loopOver(pipeline, NO_PERIODIC_FLUSH));

        assertThat(transport.finishedRunId).isEqualTo(RUN_ID);
        assertThat(transport.sendsBeforeFinish)
                .as("a streaming transport that closes first drops data it has not shipped")
                .isEqualTo(transport.sent.size());
    }

    @Test
    @DisplayName("running totals are published as deltas, on one step only")
    void countersAreAttributedToASingleStep() {
        // Capacity 3, and the queue is deliberately overrun: 3 of the 6 measurements are taken and
        // thrown away, which is the number a report has to admit to.
        MetricPipeline pipeline = new MetricPipeline(3);
        pipeline.offer(new Sample(1, 500, false));
        for (int i = 0; i < 5; i++) {
            pipeline.offer(new Sample(0, 1_000, false));
        }
        when(execution.skippedRequests()).thenReturn(7L);

        runToCompletion(loopOver(pipeline, NO_PERIODIC_FLUSH));

        assertThat(pipeline.droppedSamples()).isEqualTo(3);
        assertThat(transport.snapshotFor("list-products").droppedSamples()).isEqualTo(3);
        assertThat(transport.snapshotFor("list-products").skippedRequests()).isEqualTo(7);
        assertThat(transport.snapshotFor("checkout").droppedSamples())
                .as("repeating the counter per step makes a SUM over the run report 2x the truth")
                .isZero();
        assertThat(transport.snapshotFor("checkout").skippedRequests()).isZero();
    }

    @Test
    @DisplayName("a window with no traffic and nothing to admit to is not sent at all")
    void emptyWindowsAreNotPublished() {
        MetricAggregatorLoop loop = loopOver(new MetricPipeline(64), NO_PERIODIC_FLUSH);

        runToCompletion(loop);

        assertThat(transport.sent)
                .as("an idle worker would otherwise publish one message per step per second")
                .isEmpty();
        assertThat(transport.finishedRunId).isEqualTo(RUN_ID);
    }

    @Test
    @DisplayName("the worker is released from the final flush even when the loop dies in it")
    void waiterIsReleasedWhenTheLoopFails() throws InterruptedException {
        transport.failOnFinish = true;
        MetricAggregatorLoop loop = loopOver(new MetricPipeline(64), NO_PERIODIC_FLUSH);
        loop.stop();

        assertThatThrownBy(loop::run).isInstanceOf(IllegalStateException.class);

        assertThat(loop.awaitFinalFlush(Duration.ZERO))
                .as("a failure here would otherwise hang the worker's completion path instead of reporting")
                .isTrue();
    }

    @Test
    @DisplayName("each snapshot describes its own window, not a running total")
    void windowsAreIndependentAndContiguous() throws InterruptedException {
        MetricPipeline pipeline = new MetricPipeline(64);
        MetricAggregatorLoop loop = loopOver(pipeline, Duration.ofMillis(50));
        for (int i = 0; i < 3; i++) {
            pipeline.offer(new Sample(0, 1_000, false));
        }

        Thread thread = new Thread(loop, "aggregator-test");
        thread.start();
        try {
            transport.awaitSnapshots(1);
            for (int i = 0; i < 2; i++) {
                pipeline.offer(new Sample(0, 1_000, false));
            }
            transport.awaitSnapshots(2);
        } finally {
            loop.stop();
            assertThat(loop.awaitFinalFlush(Duration.ofSeconds(10))).isTrue();
            thread.join(TimeUnit.SECONDS.toMillis(10));
        }

        List<HistogramSnapshot> sent = List.copyOf(transport.sent);
        assertThat(sent.stream().mapToLong(HistogramSnapshot::requestCount).sum())
                .as("five requests were made; an aggregator that failed to reset would report eight")
                .isEqualTo(5);
        assertThat(sent).allSatisfy(snapshot -> assertThat(snapshot.requestCount()).isLessThan(5));

        for (int i = 1; i < sent.size(); i++) {
            assertThat(sent.get(i).windowStart())
                    .as("overlapping windows would count the same second of load twice")
                    .isAfterOrEqualTo(sent.get(i - 1).windowEnd());
        }
    }

    @Test
    @DisplayName("a snapshot carries the distribution the ingestor merges, not just the counts")
    void snapshotCarriesADecodableHistogram() {
        MetricPipeline pipeline = new MetricPipeline(64);
        pipeline.offer(new Sample(0, 1_000, false));
        pipeline.offer(new Sample(0, 1_900, false));

        runToCompletion(loopOver(pipeline, NO_PERIODIC_FLUSH));

        HistogramSnapshot snapshot = transport.snapshotFor("list-products");
        assertThat(snapshot.minMicros()).isEqualTo(1_000);
        assertThat(snapshot.maxMicros()).isEqualTo(1_900);
        assertThat(snapshot.sumMicros())
                .as("the sum is kept alongside the histogram, so it is exact rather than bucketed")
                .isEqualTo(2_900);
        assertThat(HistogramCodec.decode(snapshot.histogramBase64()).getTotalCount()).isEqualTo(2);
    }

    /** Stops the loop before it starts, so {@code run()} takes the final-flush path once. */
    private static void runToCompletion(MetricAggregatorLoop loop) {
        loop.stop();
        loop.run();
    }

    private MetricAggregatorLoop loopOver(MetricPipeline pipeline, Duration interval) {
        return new MetricAggregatorLoop(
                RUN_ID, WORKER_ID, interval, pipeline, new StepSelector(scenario()), transport, execution);
    }

    private static Scenario scenario() {
        return new Scenario(
                "two-steps",
                "http://target:8081",
                new LoadProfile(Duration.ofSeconds(10), Duration.ZERO, 100),
                List.of(
                        new ScenarioStep("list-products", HttpMethod.GET, "/products", 1, null, null),
                        new ScenarioStep("checkout", HttpMethod.POST, "/checkout", 1, null, null)),
                List.of());
    }

    private static final class RecordingTransport implements SnapshotTransport {

        private final List<HistogramSnapshot> sent = Collections.synchronizedList(new ArrayList<>());
        private volatile UUID finishedRunId;
        private volatile int sendsBeforeFinish = -1;
        private volatile boolean failOnFinish;

        @Override
        public void send(HistogramSnapshot snapshot) {
            sent.add(snapshot);
        }

        @Override
        public void runFinished(UUID runId) {
            sendsBeforeFinish = sent.size();
            finishedRunId = runId;
            if (failOnFinish) {
                throw new IllegalStateException("the stream could not be closed");
            }
        }

        @Override
        public String name() {
            return "recording";
        }

        HistogramSnapshot snapshotFor(String stepName) {
            return sent.stream()
                    .filter(snapshot -> snapshot.stepName().equals(stepName))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("no snapshot published for " + stepName));
        }

        /** Waits generously: a slow CI runner must not be mistaken for a loop that stopped. */
        void awaitSnapshots(int count) throws InterruptedException {
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (sent.size() < count) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError(
                            "expected " + count + " snapshots, got " + sent.size());
                }
                Thread.sleep(5);
            }
        }
    }
}
