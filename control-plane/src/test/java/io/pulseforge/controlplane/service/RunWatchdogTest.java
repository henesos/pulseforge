package io.pulseforge.controlplane.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.pulseforge.common.domain.RunStatus;
import io.pulseforge.controlplane.config.RunProperties;
import io.pulseforge.controlplane.persistence.TestRunEntity;
import io.pulseforge.controlplane.persistence.TestRunRepository;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The watchdog decides whether a run that lost a shard is reported or reported as fine, which is the
 * one failure in this system that produces no error anywhere: four surviving workers finish
 * normally and hand back a full set of percentiles measured at 80 % of the requested rate.
 *
 * <p>Two properties are worth more than the rest and are pinned here explicitly. Detection must not
 * end the run — a status that turns terminal while load is still being issued makes {@code /results}
 * answer 200 with numbers that keep growing afterwards. And an unreadable registry must not degrade
 * anything: a terminal status never recovers, so reading a Redis outage as "no workers alive" would
 * permanently condemn every run that was in flight during it.
 *
 * <p>Time is injected rather than waited on, so the grace periods are exercised at their exact
 * boundaries instead of approximately.
 */
class RunWatchdogTest {

    private static final Duration DISPATCH_LEAD = Duration.ofSeconds(2);
    private static final Duration SETTLE_DELAY = Duration.ofSeconds(5);
    /** Matches RunWatchdog.CLAIM_GRACE, which is deliberately not public. */
    private static final Duration CLAIM_GRACE = Duration.ofSeconds(10);

    private static final Instant CREATED = Instant.parse("2026-01-01T00:00:00Z");
    private static final int DURATION_SECONDS = 60;

    private final MutableClock clock = new MutableClock(CREATED);
    private final List<TestRunEntity> active = new ArrayList<>();

    private int aliveShards;
    private boolean registryUnavailable;
    private RunWatchdog watchdog;

    @BeforeEach
    void setUp() {
        TestRunRepository runs = mock(TestRunRepository.class);
        when(runs.findByStatusIn(anyList())).thenReturn(active);

        WorkerRegistry registry =
                new WorkerRegistry(null) {
                    @Override
                    public int liveShardCount(UUID runId) {
                        if (registryUnavailable) {
                            throw new WorkerRegistry.RegistryUnavailableException(
                                    "could not read liveness", new IllegalStateException("no route"));
                        }
                        return aliveShards;
                    }
                };

        watchdog =
                new RunWatchdog(
                        runs,
                        registry,
                        new RunProperties(DISPATCH_LEAD, SETTLE_DELAY, Duration.ofSeconds(5)),
                        clock);
    }

    @Test
    @DisplayName("a run still within the dispatch window is left alone")
    void undispatchedRunIsGivenTheDispatchLead() {
        TestRunEntity run = pendingRun(3);
        clock.set(CREATED.plus(DISPATCH_LEAD).plus(CLAIM_GRACE));

        watchdog.checkActiveRuns();

        assertThat(run.getStatus())
                .as("at exactly the deadline the command may still be in flight")
                .isEqualTo(RunStatus.PENDING);
    }

    @Test
    @DisplayName("a run that was never dispatched is failed rather than left PENDING forever")
    void undispatchedRunIsFailed() {
        TestRunEntity run = pendingRun(3);
        clock.set(CREATED.plus(DISPATCH_LEAD).plus(CLAIM_GRACE).plusSeconds(1));

        watchdog.checkActiveRuns();

        assertThat(run.getStatus()).isEqualTo(RunStatus.FAILED);
        assertThat(run.getStatusReason()).contains("never dispatched");
        assertThat(run.getFinishedAt())
                .as("an operator needs a closed run to act on, not a stuck one")
                .isEqualTo(clock.instant());
    }

    @Test
    @DisplayName("shard liveness is not judged before workers have had time to claim")
    void missingShardsAreIgnoredDuringTheClaimGrace() {
        TestRunEntity run = runningRun(5);
        aliveShards = 0;
        clock.set(CREATED.plus(CLAIM_GRACE).minusSeconds(1));

        watchdog.checkActiveRuns();

        assertThat(run.isDegraded())
                .as("no shard has claimed yet; every run would look lost")
                .isFalse();
        assertThat(run.getStatus()).isEqualTo(RunStatus.RUNNING);
    }

    @Test
    @DisplayName("a shard lost mid-run is recorded, and the run keeps going")
    void midRunLossIsRecordedWithoutTerminating() {
        TestRunEntity run = runningRun(5);
        aliveShards = 4;
        clock.set(CREATED.plusSeconds(20));

        watchdog.checkActiveRuns();

        assertThat(run.isDegraded()).isTrue();
        assertThat(run.getDegradedReason()).contains("1 of 5 worker shards stopped reporting");
        assertThat(run.getStatus())
                .as("the survivors were asked for 60 seconds of load and are still issuing it")
                .isEqualTo(RunStatus.RUNNING);
        assertThat(run.getFinishedAt()).isNull();
    }

    @Test
    @DisplayName("a shard that already finished still counts as accounted for")
    void finishedShardsAreNotCountedAsLost() {
        TestRunEntity run = runningRun(5);
        run.recordWorkerFinished("worker-a");
        run.recordWorkerFinished("worker-b");
        aliveShards = 3;
        clock.set(CREATED.plusSeconds(20));

        watchdog.checkActiveRuns();

        assertThat(run.isDegraded())
                .as("claimed shards == still alive + already finished")
                .isFalse();
    }

    @Test
    @DisplayName("an unreadable registry never degrades a run")
    void registryOutageDefersTheCheck() {
        TestRunEntity run = runningRun(5);
        registryUnavailable = true;
        clock.set(CREATED.plusSeconds(20));

        watchdog.checkActiveRuns();

        assertThat(run.isDegraded())
                .as("Redis being unreachable says nothing about the fleet, and DEGRADED never recovers")
                .isFalse();
        assertThat(run.getStatus()).isEqualTo(RunStatus.RUNNING);
    }

    @Test
    @DisplayName("an unreadable registry does not close a finished run either")
    void registryOutageDefersTheClose() {
        TestRunEntity run = runningRun(5);
        finishShards(run, 5, scheduledEnd());
        registryUnavailable = true;
        clock.set(scheduledEnd().plus(SETTLE_DELAY).plusSeconds(30));

        watchdog.checkActiveRuns();

        assertThat(run.getStatus())
                .as("the close is retried on the next tick, once the registry answers again")
                .isEqualTo(RunStatus.RUNNING);
    }

    @Test
    @DisplayName("a run is not closed until in-flight snapshots have settled")
    void closeWaitsOutTheSettleDelay() {
        TestRunEntity run = runningRun(3);
        finishShards(run, 3, scheduledEnd());
        aliveShards = 0;
        clock.set(scheduledEnd().plus(SETTLE_DELAY).minusSeconds(1));

        watchdog.checkActiveRuns();

        assertThat(run.getStatus())
                .as("results read now would be short by the final snapshot interval")
                .isEqualTo(RunStatus.RUNNING);

        clock.set(scheduledEnd().plus(SETTLE_DELAY).plusSeconds(1));
        watchdog.checkActiveRuns();

        assertThat(run.getStatus()).isEqualTo(RunStatus.COMPLETED);
        assertThat(run.getStatusReason()).isNull();
        assertThat(run.getFinishedAt()).isEqualTo(clock.instant());
    }

    @Test
    @DisplayName("a normal finish is not mistaken for a loss just because no shard is alive")
    void aCleanRunClosesCompleted() {
        TestRunEntity run = runningRun(3);
        finishShards(run, 3, scheduledEnd());
        // Liveness is cleared just before a worker announces itself, so at close time every shard
        // of a perfectly healthy run reads as zero alive.
        aliveShards = 0;
        clock.set(scheduledEnd().plus(SETTLE_DELAY).plusSeconds(1));

        watchdog.checkActiveRuns();

        assertThat(run.getStatus()).isEqualTo(RunStatus.COMPLETED);
        assertThat(run.isDegraded()).isFalse();
    }

    @Test
    @DisplayName("a shard that never reported is caught when the run closes")
    void aShardThatNeverReportsClosesDegraded() {
        TestRunEntity run = runningRun(5);
        finishShards(run, 4, scheduledEnd());
        aliveShards = 0;
        clock.set(scheduledEnd().plus(CLAIM_GRACE).plus(SETTLE_DELAY).plusSeconds(1));

        watchdog.checkActiveRuns();

        assertThat(run.getStatus()).isEqualTo(RunStatus.DEGRADED);
        assertThat(run.getStatusReason())
                .contains("1 of 5 worker shards stopped reporting")
                .contains("less than the requested 250 req/s");
    }

    @Test
    @DisplayName("the closing verdict reports the counts seen when the loss was detected")
    void degradationReportsDetectionTimeCounts() {
        TestRunEntity run = runningRun(5);
        aliveShards = 4;
        clock.set(CREATED.plusSeconds(20));
        watchdog.checkActiveRuns();

        // The four survivors go on to finish normally, and the run closes long after.
        finishShards(run, 4, scheduledEnd());
        aliveShards = 0;
        clock.set(scheduledEnd().plus(CLAIM_GRACE).plus(SETTLE_DELAY).plusSeconds(1));
        watchdog.checkActiveRuns();

        assertThat(run.getStatus()).isEqualTo(RunStatus.DEGRADED);
        assertThat(run.getStatusReason())
                .as("an unqualified '0 finished' would read as the final state rather than the moment of loss")
                .contains("4 alive, 0 finished when detected");
    }

    @Test
    @DisplayName("the settle window is measured from the moment every shard reported")
    void settleIsMeasuredFromTheLastReport() {
        TestRunEntity run = runningRun(3);
        // Every shard reported ten seconds late; the snapshots are that much later too.
        Instant reportedAt = scheduledEnd().plusSeconds(10);
        finishShards(run, 3, reportedAt);
        aliveShards = 0;

        clock.set(reportedAt.plus(SETTLE_DELAY).minusSeconds(1));
        watchdog.checkActiveRuns();
        assertThat(run.getStatus()).isEqualTo(RunStatus.RUNNING);

        clock.set(reportedAt.plus(SETTLE_DELAY).plusSeconds(1));
        watchdog.checkActiveRuns();
        assertThat(run.getStatus()).isEqualTo(RunStatus.COMPLETED);
    }

    @Test
    @DisplayName("one run that blows up does not stop the others being checked")
    void aFailingCheckIsIsolated() {
        // No creation instant: the undispatched branch dereferences it and throws.
        active.add(new TestRunEntity(UUID.randomUUID(), UUID.randomUUID(), 250, 60, 5, 3, null));
        TestRunEntity healthy = pendingRun(3);
        clock.set(CREATED.plus(DISPATCH_LEAD).plus(CLAIM_GRACE).plusSeconds(1));

        assertThatCode(() -> watchdog.checkActiveRuns()).doesNotThrowAnyException();

        assertThat(healthy.getStatus())
                .as("a single broken row must not blind the watchdog to the whole fleet")
                .isEqualTo(RunStatus.FAILED);
    }

    private TestRunEntity pendingRun(int expectedWorkers) {
        TestRunEntity run =
                new TestRunEntity(
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        250,
                        DURATION_SECONDS,
                        5,
                        expectedWorkers,
                        CREATED);
        active.add(run);
        return run;
    }

    private TestRunEntity runningRun(int expectedWorkers) {
        TestRunEntity run = pendingRun(expectedWorkers);
        run.markRunning(CREATED);
        return run;
    }

    /**
     * Mirrors {@code RunService.recordWorkerFinished}: reaching the expected count is what starts
     * the settle window, and a run one shard short never starts it at all.
     */
    private static void finishShards(TestRunEntity run, int count, Instant at) {
        for (int i = 0; i < count; i++) {
            run.recordWorkerFinished("worker-" + i);
        }
        if (run.getFinishedWorkers() >= run.getExpectedWorkers()) {
            run.markAllShardsReported(at);
        }
    }

    private static Instant scheduledEnd() {
        return CREATED.plusSeconds(DURATION_SECONDS);
    }

    /** A clock the test moves by hand, so grace periods are checked at their exact boundaries. */
    private static final class MutableClock extends Clock {

        private Instant now;

        private MutableClock(Instant now) {
            this.now = now;
        }

        void set(Instant instant) {
            this.now = instant;
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }
}
