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
 * Closes every run, and marks the ones that lost a worker DEGRADED.
 *
 * <p>The failure this exists for is quiet by nature. Five workers each generate a fifth of the
 * load; one dies; the other four finish normally and the run reports a full set of plausible
 * percentiles measured at 80 % of the requested rate. Nothing errors, nothing retries, and the
 * number that reaches a dashboard is simply wrong.
 *
 * <p>The check is {@code claimed shards == still alive + already finished}. Anything missing from
 * both sides stopped without saying so.
 *
 * <p>Detecting the loss and ending the run are deliberately separate. A shard that dies at second
 * three of a sixty-second run is recorded immediately, but the run keeps going: the survivors
 * generate load for the duration that was asked for, and the status only turns terminal once that
 * duration is spent. Ending it at detection time would flip {@code /results} to a final 200 while
 * load was still being issued, so the same URL would keep answering with larger numbers afterwards.
 *
 * <p>Three grace periods keep this from firing spuriously: a run is ignored until workers have had
 * time to claim shards, a run past its scheduled end waits out the settle delay so in-flight
 * snapshots reach ClickHouse, and a run that was never dispatched is given the dispatch lead before
 * being failed.
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

        // A run with no start instant was never dispatched. It cannot recover on its own, and
        // leaving it PENDING forever gives the operator nothing to act on.
        if (run.getStartedAt() == null) {
            Instant giveUpAt = run.getCreatedAt().plus(properties.dispatchLead()).plus(CLAIM_GRACE);
            if (now.isAfter(giveUpAt)) {
                run.terminate(RunStatus.FAILED, "the run was never dispatched to any worker", now);
                log.warn("Run {} marked FAILED: never dispatched", run.getId());
            }
            return;
        }
        if (now.isBefore(run.getStartedAt().plus(CLAIM_GRACE))) {
            return;
        }

        int alive;
        try {
            alive = registry.liveShardCount(run.getId());
        } catch (WorkerRegistry.RegistryUnavailableException e) {
            // Redis being unreachable says nothing about the fleet. Reading it as zero live shards
            // would degrade every active run for the length of an outage — and permanently, since
            // a terminal status never recovers.
            log.warn("Run {}: shard liveness unreadable, deferring the check", run.getId(), e);
            return;
        }

        int accountedFor = alive + run.getFinishedWorkers();
        Instant scheduledEnd = run.getStartedAt().plusSeconds(run.getDurationSeconds());

        if (now.isBefore(scheduledEnd)) {
            // Mid-run. A shard missing from both sides stopped without saying so — record it, and
            // let the run play out. The surviving shards keep generating load for the duration that
            // was asked for, and the loss is carried into the terminal status below.
            if (accountedFor < run.getExpectedWorkers()) {
                degrade(run, alive);
            }
            return;
        }

        // Past the scheduled end. Only now is a shard's absence ambiguous: liveness is cleared just
        // before a worker announces itself, so a normal finish briefly counts on neither side.
        Instant lastReport = run.getAllShardsReportedAt();
        Instant closeAt =
                (lastReport != null ? lastReport : scheduledEnd.plus(CLAIM_GRACE))
                        .plus(properties.settleDelay());
        if (now.isBefore(closeAt)) {
            return;
        }

        if (run.getFinishedWorkers() < run.getExpectedWorkers()) {
            degrade(run, alive);
        }

        if (run.isDegraded()) {
            run.terminate(RunStatus.DEGRADED, run.getDegradedReason(), now);
            log.warn("Run {} closed DEGRADED: {}", run.getId(), run.getDegradedReason());
        } else {
            run.terminate(RunStatus.COMPLETED, null, now);
            log.info("Run {} completed", run.getId());
        }
    }

    private void degrade(TestRunEntity run, int alive) {
        int finished = run.getFinishedWorkers();
        int missing = run.getExpectedWorkers() - (alive + finished);
        // The counts are the ones observed at detection, and are labelled as such: by the time the
        // run closes the survivors have finished, and an unqualified "0 finished" would read as a
        // description of the final state rather than of the moment the loss was seen.
        String reason =
                "%d of %d worker shards stopped reporting (%d alive, %d finished when detected); the run generated less than the requested %d req/s"
                        .formatted(
                                missing,
                                run.getExpectedWorkers(),
                                alive,
                                finished,
                                run.getArrivalRate());
        boolean firstTime = !run.isDegraded();
        run.recordDegradation(reason);
        if (firstTime) {
            log.warn("Run {} will report DEGRADED: {}", run.getId(), reason);
        }
    }
}
