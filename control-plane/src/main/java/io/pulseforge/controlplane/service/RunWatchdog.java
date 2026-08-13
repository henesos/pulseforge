package io.pulseforge.controlplane.service;

import io.pulseforge.common.domain.RunStatus;
import io.pulseforge.controlplane.config.RunProperties;
import io.pulseforge.controlplane.persistence.TestRunEntity;
import io.pulseforge.controlplane.persistence.TestRunRepository;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Detects runs that lost a worker and marks them DEGRADED.
 *
 * <p>The failure this exists for is quiet by nature. Five workers each generate a fifth of the
 * load; one dies; the other four finish normally and the run reports a full set of plausible
 * percentiles measured at 80 % of the requested rate. Nothing errors, nothing retries, and the
 * number that reaches a dashboard is simply wrong.
 *
 * <p>The check is {@code claimed shards == still alive + already finished}. Anything missing from
 * both sides stopped without saying so.
 *
 * <p>Two grace periods keep this from firing spuriously: the run is ignored until its start instant
 * has passed and workers have had time to claim shards, and a run that has passed its scheduled end
 * is given time for final completion messages to arrive.
 */
@Component
public class RunWatchdog {

    private static final Logger log = LoggerFactory.getLogger(RunWatchdog.class);

    /** Time after dispatch before shard claims are expected to have landed. */
    private static final Duration CLAIM_GRACE = Duration.ofSeconds(10);

    private final TestRunRepository runs;
    private final WorkerRegistry registry;
    private final RunProperties properties;
    private final Clock clock;

    public RunWatchdog(
            TestRunRepository runs,
            WorkerRegistry registry,
            RunProperties properties,
            Clock clock) {
        this.runs = runs;
        this.registry = registry;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * Scheduled by {@link WatchdogScheduler} rather than by an annotation here.
     *
     * <p>The indirection is load-bearing: a scheduled task that invoked this method on {@code this}
     * would bypass the transactional proxy, so the DEGRADED status set below would be computed
     * correctly and then never committed — the watchdog would log the same warning forever while
     * the run stayed RUNNING.
     */
    @Transactional
    public void checkActiveRuns() {
        List<TestRunEntity> active =
                runs.findByStatusIn(List.of(RunStatus.PENDING, RunStatus.RUNNING));

        for (TestRunEntity run : active) {
            try {
                check(run);
            } catch (RuntimeException e) {
                log.error("Watchdog check failed for run {}", run.getId(), e);
            }
        }
    }

    private void check(TestRunEntity run) {
        Instant now = clock.instant();
        if (run.getStartedAt() == null || now.isBefore(run.getStartedAt().plus(CLAIM_GRACE))) {
            return;
        }

        int alive = registry.liveShardCount(run.getId());
        int finished = run.getFinishedWorkers();
        int accountedFor = alive + finished;

        if (accountedFor >= run.getExpectedWorkers()) {
            return;
        }

        // Before the scheduled end, a missing shard can only mean a worker stopped mid-run.
        // After it, wait out the settle window in case a completion message is still travelling.
        Instant scheduledEnd = run.getStartedAt().plusSeconds(run.getDurationSeconds());
        boolean pastEnd = now.isAfter(scheduledEnd);
        if (pastEnd && now.isBefore(scheduledEnd.plus(properties.settleDelay()).plus(CLAIM_GRACE))) {
            return;
        }

        int missing = run.getExpectedWorkers() - accountedFor;
        String reason =
                "%d of %d worker shards stopped reporting (%d alive, %d finished); the run generated less than the requested %d req/s"
                        .formatted(
                                missing,
                                run.getExpectedWorkers(),
                                alive,
                                finished,
                                run.getArrivalRate());

        run.terminate(RunStatus.DEGRADED, reason, now);
        log.warn("Run {} marked DEGRADED: {}", run.getId(), reason);
    }
}
