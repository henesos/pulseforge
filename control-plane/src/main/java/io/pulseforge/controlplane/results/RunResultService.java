package io.pulseforge.controlplane.results;

import io.pulseforge.controlplane.persistence.TestRunEntity;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Service;

/** Assembles the stored measurements into the reported result for a run. */
@Service
public class RunResultService {

    private static final double P50 = 0.50;
    private static final double P95 = 0.95;
    private static final double P99 = 0.99;

    private final ClickHouseResultRepository repository;

    public RunResultService(ClickHouseResultRepository repository) {
        this.repository = repository;
    }

    public RunResults resultsFor(TestRunEntity run) {
        List<ClickHouseResultRepository.StepTotals> totals = repository.stepTotals(run.getId());

        List<StepResult> steps = new ArrayList<>(totals.size());
        long allRequests = 0;
        long allErrors = 0;
        long allDropped = 0;
        long allSkipped = 0;
        long maxMicros = 0;
        long firstMillis = Long.MAX_VALUE;
        long lastMillis = 0;

        for (ClickHouseResultRepository.StepTotals total : totals) {
            long[] percentiles =
                    repository.percentiles(run.getId(), total.stepName(), P50, P95, P99);
            double spanSeconds = spanSeconds(total.firstWindowMillis(), total.lastWindowMillis());

            steps.add(
                    new StepResult(
                            total.stepName(),
                            total.requests(),
                            total.errors(),
                            percentage(total.errors(), total.requests()),
                            spanSeconds == 0 ? 0 : total.requests() / spanSeconds,
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
            firstMillis = Math.min(firstMillis, total.firstWindowMillis());
            lastMillis = Math.max(lastMillis, total.lastWindowMillis());
        }

        // Run-wide percentiles merge every step, because an assertion like `p95 < 250ms` is a
        // statement about the whole scenario, not about one endpoint within it.
        long[] overall = repository.percentilesForRun(run.getId(), P50, P95, P99);
        double runSeconds = totals.isEmpty() ? 0 : spanSeconds(firstMillis, lastMillis);

        // Counted across the run, not as the largest per-step figure: two workers that each served
        // a different step contribute one worker apiece to their own row, and taking the maximum
        // would report one worker for a fleet of two.
        int workers = repository.contributingWorkers(run.getId());

        return new RunResults(
                run.getId(),
                run.getStatus(),
                run.getStatusReason(),
                run.getStartedAt(),
                run.getFinishedAt(),
                allRequests,
                allErrors,
                percentage(allErrors, allRequests),
                runSeconds == 0 ? 0 : allRequests / runSeconds,
                micronsToMillis(overall[0]),
                micronsToMillis(overall[1]),
                micronsToMillis(overall[2]),
                micronsToMillis(maxMicros),
                allDropped,
                allSkipped,
                workers,
                steps);
    }

    private static double spanSeconds(long firstMillis, long lastMillis) {
        if (firstMillis == Long.MAX_VALUE || lastMillis <= firstMillis) {
            return 0;
        }
        return (lastMillis - firstMillis) / 1000.0d;
    }

    private static double percentage(long part, long total) {
        return total == 0 ? 0 : part * 100.0d / total;
    }

    /** Microseconds to milliseconds, rounded to two decimals for a readable report. */
    private static double micronsToMillis(long micros) {
        return Math.round(micros / 10.0d) / 100.0d;
    }
}
