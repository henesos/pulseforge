package io.pulseforge.worker.run;

import io.pulseforge.common.domain.ScenarioStep;
import io.pulseforge.common.protocol.StartRunCommand;
import io.pulseforge.worker.http.RequestExecutor;
import io.pulseforge.worker.metrics.MetricPipeline;
import io.pulseforge.worker.metrics.Sample;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drives one run on this worker: walks the arrival schedule, fires requests, feeds measurements
 * into the metric pipeline.
 *
 * <p>The loop is deliberately simple and single-threaded. It sleeps until each request's scheduled
 * send time, dispatches asynchronously, and moves on without waiting. Latency is measured from the
 * <em>scheduled</em> send time, so if the loop itself falls behind, that lateness shows up in the
 * histogram instead of vanishing.
 */
public class RunExecution implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(RunExecution.class);

    /** Below this, spinning beats sleeping; above it, sleeping beats burning a core. */
    private static final long SLEEP_THRESHOLD_NANOS = 1_500_000L;

    /** How long the run waits for stragglers once the schedule is exhausted. */
    private static final Duration DRAIN_TIMEOUT = Duration.ofSeconds(15);

    private final StartRunCommand command;
    private final int shardIndex;
    private final ArrivalSchedule schedule;
    private final StepSelector stepSelector;
    private final RequestExecutor requestExecutor;
    private final MetricPipeline pipeline;
    private final Semaphore inFlightLimit;
    private final int maxConcurrentRequests;

    private final AtomicBoolean stopped = new AtomicBoolean();
    private final LongAdder issuedRequests = new LongAdder();
    private final LongAdder skippedRequests = new LongAdder();

    public RunExecution(
            StartRunCommand command,
            int shardIndex,
            ArrivalSchedule schedule,
            StepSelector stepSelector,
            RequestExecutor requestExecutor,
            MetricPipeline pipeline,
            int maxConcurrentRequests) {
        this.command = command;
        this.shardIndex = shardIndex;
        this.schedule = schedule;
        this.stepSelector = stepSelector;
        this.requestExecutor = requestExecutor;
        this.pipeline = pipeline;
        this.maxConcurrentRequests = maxConcurrentRequests;
        this.inFlightLimit = new Semaphore(maxConcurrentRequests);
    }

    @Override
    public void run() {
        long startNanos = alignToStart();
        long deadlineNanos = startNanos + schedule.duration().toNanos();
        long total = schedule.totalRequests();

        log.info(
                "Run {} shard {} started: {} req/s for {}, {} requests scheduled",
                command.runId(),
                shardIndex,
                String.format("%.1f", schedule.ratePerSecond()),
                schedule.duration(),
                total);

        for (long index = 0; index < total && !stopped.get(); index++) {
            long scheduledNanos = startNanos + schedule.sendOffsetNanos(index);
            if (scheduledNanos >= deadlineNanos) {
                break;
            }
            if (!waitUntil(scheduledNanos)) {
                break;
            }
            dispatch(stepSelector.nextIndex(), scheduledNanos);
        }

        awaitInFlight();
        log.info(
                "Run {} shard {} finished: {} issued, {} skipped, {} samples dropped",
                command.runId(),
                shardIndex,
                issuedRequests.sum(),
                skippedRequests.sum(),
                pipeline.droppedSamples());
    }

    private void dispatch(int stepIndex, long scheduledNanos) {
        // Never blocks: at the ceiling the request is abandoned and counted, because waiting here
        // would silently convert the open-loop schedule back into a closed loop.
        if (!inFlightLimit.tryAcquire()) {
            skippedRequests.increment();
            return;
        }

        ScenarioStep step = stepSelector.step(stepIndex);
        issuedRequests.increment();

        requestExecutor
                .execute(step)
                .whenComplete(
                        (response, error) -> {
                            try {
                                long latencyMicros = (System.nanoTime() - scheduledNanos) / 1_000;
                                boolean failed = error != null || response.statusCode() >= 400;
                                pipeline.offer(new Sample(stepIndex, latencyMicros, failed));
                            } finally {
                                inFlightLimit.release();
                            }
                        });
    }

    /** Sleeps until the wall-clock start instant the control plane picked for the whole fleet. */
    private long alignToStart() {
        Duration untilStart = Duration.between(Instant.now(), command.startAt());
        if (untilStart.isPositive()) {
            try {
                Thread.sleep(untilStart.toMillis(), untilStart.toNanosPart() % 1_000_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else if (untilStart.abs().toMillis() > 250) {
            log.warn(
                    "Run {} shard {} started {} late; the fleet's ramp will be uneven",
                    command.runId(),
                    shardIndex,
                    untilStart.abs());
        }
        return System.nanoTime();
    }

    /** Returns false if the run was stopped or the thread interrupted while waiting. */
    private boolean waitUntil(long targetNanos) {
        while (!stopped.get()) {
            long remaining = targetNanos - System.nanoTime();
            if (remaining <= 0) {
                return true;
            }
            if (remaining > SLEEP_THRESHOLD_NANOS) {
                try {
                    Thread.sleep(remaining / 1_000_000, (int) (remaining % 1_000_000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            } else {
                Thread.onSpinWait();
            }
        }
        return false;
    }

    /**
     * Gives outstanding responses a bounded window to land so their samples are not lost.
     *
     * <p>Acquiring every permit is the same as observing zero in-flight requests. The wait is
     * bounded: a target that has stopped answering must not keep the run open forever, and the
     * requests still pending are already counted as issued.
     */
    private void awaitInFlight() {
        try {
            boolean drained =
                    inFlightLimit.tryAcquire(
                            maxConcurrentRequests, DRAIN_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            if (!drained) {
                log.warn(
                        "Run {} shard {} ended with {} requests still in flight after {}",
                        command.runId(),
                        shardIndex,
                        maxConcurrentRequests - inFlightLimit.availablePermits(),
                        DRAIN_TIMEOUT);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void stop() {
        stopped.set(true);
    }

    public long issuedRequests() {
        return issuedRequests.sum();
    }

    public long skippedRequests() {
        return skippedRequests.sum();
    }
}
