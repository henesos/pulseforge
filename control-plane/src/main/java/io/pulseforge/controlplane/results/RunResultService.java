package io.pulseforge.controlplane.results;

import io.pulseforge.controlplane.persistence.TestRunEntity;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Service;

/**
 * Assembles the stored measurements into the reported result for a run.
 *
 * <p>Throughput is requests divided by <em>the time the test was asked to run</em>. That choice is
 * the whole reason this class holds a clock, and it is worth stating because two other denominators
 * are easier and both flatter the result. Dividing by the span of the windows that happened to
 * carry measurements reports a rate for the period the system was busy rather than the period it
 * was asked to be busy — a run that started late or finished early comes out faster than it ran.
 * Dividing by wall-clock start-to-finish is worse still: a terminal status arrives only after the
 * settle delay and a watchdog tick, so it would charge the run for time nobody generated load in.
 *
 * <p>Requests asked for, over the duration asked for, is the number an operator is actually
 * comparing against the arrival rate they wrote in the scenario.
 */
@Service
public class RunResultService {

    private static final double P50 = 0.50;
    private static final double P95 = 0.95;
    private static final double P99 = 0.99;

    private final ClickHouseResultRepository repository;
    private final Clock clock;

    public RunResultService(ClickHouseResultRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }

    public RunResults resultsFor(TestRunEntity run) {
        List<ClickHouseResultRepository.StepTotals> totals = repository.stepTotals(run.getId());

        List<StepResult> steps = new ArrayList<>(totals.size());
        long allRequests = 0;
        long allErrors = 0;
        long allDropped = 0;
        long allSkipped = 0;
        long maxMicros = 0;

        // One denominator for the run and for every step in it, so the step rates sum back to the
        // run rate. Per-step spans do not: a step that went quiet early would report a higher rate
        // than it sustained, and the column would not add up to the total printed under it.
        double measuredSeconds = measuredSeconds(run);

        for (ClickHouseResultRepository.StepTotals total : totals) {
            long[] percentiles =
                    repository.percentiles(run.getId(), total.stepName(), P50, P95, P99);

            steps.add(
                    new StepResult(
                            total.stepName(),
                            total.requests(),
                            total.errors(),
                            percentage(total.errors(), total.requests()),
                            measuredSeconds == 0 ? 0 : total.requests() / measuredSeconds,
                            total.requests() == 0
                                    ? 0
                                    : micronsToMillis(total.sumMicros() / total.requests()),
                            micronsToMillis(percentiles[0]),
                            micronsToMillis(percentiles[1]),
                            micronsToMillis(percentiles[2]),
                            micronsToMillis(total.maxMicros())));

            allRequests += total.requests();
            allErrors += total.errors();
            allDropped += total.droppedSamples();
            allSkipped += total.skippedRequests();
            maxMicros = Math.max(maxMicros, total.maxMicros());
        }

        // Run-wide percentiles merge every step, because an assertion like `p95 < 250ms` is a
        // statement about the whole scenario, not about one endpoint within it.
        long[] overall = repository.percentilesForRun(run.getId(), P50, P95, P99);

        // Counted across the run, not as the largest per-step figure: two workers that each served
        // a different step contribute one worker apiece to their own row, and taking the maximum
        // would report one worker for a fleet of two.
        int workers = repository.contributingWorkers(run.getId());

        // Loss on the storage side of the wire. The counters the lost snapshots carried are folded
        // into the run's own totals, because they are facts about the run that would otherwise
        // vanish with the message that was reporting them.
        ClickHouseResultRepository.IngestLosses lost = repository.ingestLosses(run.getId());
        allDropped += lost.lostDropped();
        allSkipped += lost.lostSkipped();

        return new RunResults(
                run.getId(),
                run.getStatus(),
                run.getStatusReason(),
                run.getStartedAt(),
                run.getFinishedAt(),
                allRequests,
                allErrors,
                percentage(allErrors, allRequests),
                measuredSeconds == 0 ? 0 : allRequests / measuredSeconds,
                micronsToMillis(overall[0]),
                micronsToMillis(overall[1]),
                micronsToMillis(overall[2]),
                micronsToMillis(maxMicros),
                allDropped,
                allSkipped,
                lost.lostRequests(),
                workers,
                steps);
    }

    /**
     * The seconds the run was asked to generate load for, which is what requests are divided by.
     *
     * <p>A run still in flight is credited only with the time it has actually had — otherwise the
     * first poll of a 60-second run would divide a few hundred requests by 60 and report a rate the
     * run is nowhere near, on a page that also says the run is still going.
     */
    private double measuredSeconds(TestRunEntity run) {
        if (run.getStartedAt() == null) {
            return 0;
        }
        double scheduled = run.getDurationSeconds();
        if (run.getStatus().isTerminal()) {
            return scheduled;
        }
        double elapsed =
                Duration.between(run.getStartedAt(), clock.instant()).toMillis() / 1000.0d;
        return Math.min(Math.max(elapsed, 0), scheduled);
    }

    private static double percentage(long part, long total) {
        return total == 0 ? 0 : part * 100.0d / total;
    }

    /** Microseconds to milliseconds, rounded to two decimals for a readable report. */
    private static double micronsToMillis(long micros) {
        return Math.round(micros / 10.0d) / 100.0d;
    }
}
