package io.pulseforge.controlplane.results;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.pulseforge.common.domain.RunStatus;
import io.pulseforge.controlplane.persistence.TestRunEntity;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A run's report is the product. Measurements can go missing in three different places — the worker
 * could not enqueue them, the generator never issued the request, or the ingestor received the
 * snapshot and never stored it — and a report that mentions only the first two is the same clean,
 * confident, quietly wrong output that every other decision in this system is arranged to prevent.
 *
 * <p>{@code isComplete()} is what the CLI turns into an exit code, so what counts as incomplete is
 * a contract rather than a detail.
 */
class RunResultServiceTest {

    private static final Instant STARTED = Instant.parse("2026-01-01T00:00:00Z");

    private final ClickHouseResultRepository repository = mock(ClickHouseResultRepository.class);
    private RunResultService service;

    @BeforeEach
    void setUp() {
        service = new RunResultService(repository, Clock.fixed(STARTED.plusSeconds(4), ZoneOffset.UTC));
        when(repository.stepTotals(any()))
                .thenReturn(
                        List.of(
                                new ClickHouseResultRepository.StepTotals(
                                        "list-products",
                                        1_000,
                                        10,
                                        0,
                                        0,
                                        2_000_000,
                                        9_000,
                                        2)));
        when(repository.percentiles(any(), anyString(), anyDouble(), anyDouble(), anyDouble()))
                .thenReturn(new long[] {1_000, 2_000, 3_000});
        when(repository.percentilesForRun(any(), anyDouble(), anyDouble(), anyDouble()))
                .thenReturn(new long[] {1_000, 2_000, 3_000});
        when(repository.contributingWorkers(any())).thenReturn(2);
        when(repository.ingestLosses(any()))
                .thenReturn(ClickHouseResultRepository.IngestLosses.NONE);
    }

    @Test
    @DisplayName("throughput is requests over the duration the test was asked to run")
    void throughputUsesTheScheduledDuration() {
        // 1 000 requests over the 10 seconds the scenario asked for. Not over the span of the
        // windows that happened to hold measurements, which is shorter than the run whenever a
        // shard starts late or goes quiet early — and reports a rate the run never sustained.
        RunResults results = service.resultsFor(run());

        assertThat(results.throughputPerSecond()).isEqualTo(100.0);
    }

    @Test
    @DisplayName("the per-step rates sum back to the rate printed under them")
    void stepThroughputSumsToTheRunThroughput() {
        when(repository.stepTotals(any()))
                .thenReturn(
                        List.of(
                                stepTotals("list-products", 600),
                                stepTotals("checkout", 400)));

        RunResults results = service.resultsFor(run());

        assertThat(results.steps())
                .extracting(StepResult::throughputPerSecond)
                .containsExactly(60.0, 40.0);
        assertThat(
                        results.steps().stream()
                                .mapToDouble(StepResult::throughputPerSecond)
                                .sum())
                .as("a table whose column does not add up to its own total is not a report")
                .isEqualTo(results.throughputPerSecond());
    }

    @Test
    @DisplayName("a run still in flight is credited only with the time it has had")
    void anInFlightRunIsNotDividedByTheWholeDuration() {
        TestRunEntity running =
                new TestRunEntity(UUID.randomUUID(), UUID.randomUUID(), 100, 10, 0, 2, STARTED);
        running.markRunning(STARTED);

        RunResults results = service.resultsFor(running);

        assertThat(results.throughputPerSecond())
                .as("4 seconds in, 1 000 requests is 250/s — dividing by 10 would report 100/s "
                        + "for a run that is beating it")
                .isEqualTo(250.0);
    }

    @Test
    @DisplayName("a run that lost nothing anywhere reports complete")
    void aCleanRunIsComplete() {
        RunResults results = service.resultsFor(run());

        assertThat(results.unstoredSamples()).isZero();
        assertThat(results.isComplete()).isTrue();
    }

    @Test
    @DisplayName("measurements the ingestor never stored are reported, not absorbed")
    void ingestLossReachesTheReport() {
        when(repository.ingestLosses(any()))
                .thenReturn(new ClickHouseResultRepository.IngestLosses(430, 0, 0));

        RunResults results = service.resultsFor(run());

        assertThat(results.unstoredSamples())
                .as("430 measurements are missing from the percentiles above")
                .isEqualTo(430);
        assertThat(results.isComplete())
                .as("a percentile over a population 430 short is not a sound measurement")
                .isFalse();
        assertThat(results.droppedSamples())
                .as("kept apart: a worker queue and an ingestor queue are fixed in different places")
                .isZero();
    }

    @Test
    @DisplayName("counters carried by lost snapshots are folded back into the run's own totals")
    void carriedCountersAreRecovered() {
        // The lost snapshots were themselves reporting worker-side drops and skipped requests. Those
        // are facts about the run; losing the message must not also lose the fact.
        when(repository.ingestLosses(any()))
                .thenReturn(new ClickHouseResultRepository.IngestLosses(430, 12, 5));

        RunResults results = service.resultsFor(run());

        assertThat(results.droppedSamples()).isEqualTo(12);
        assertThat(results.skippedRequests()).isEqualTo(5);
        assertThat(results.unstoredSamples()).isEqualTo(430);
    }

    @Test
    @DisplayName("the measured numbers themselves are untouched by the loss record")
    void lossDoesNotInflateTheMeasurements() {
        when(repository.ingestLosses(any()))
                .thenReturn(new ClickHouseResultRepository.IngestLosses(430, 0, 0));

        RunResults results = service.resultsFor(run());

        assertThat(results.totalRequests())
                .as("the lost requests have no latencies; counting them would invent throughput")
                .isEqualTo(1_000);
        assertThat(results.p99Ms()).isEqualTo(3.0);
    }

    private static ClickHouseResultRepository.StepTotals stepTotals(String step, long requests) {
        return new ClickHouseResultRepository.StepTotals(
                step, requests, 0, 0, 0, requests * 2_000, 9_000, 2);
    }

    private static TestRunEntity run() {
        TestRunEntity run =
                new TestRunEntity(UUID.randomUUID(), UUID.randomUUID(), 100, 10, 0, 2, STARTED);
        run.markRunning(STARTED);
        run.terminate(RunStatus.COMPLETED, null, STARTED.plusSeconds(10));
        return run;
    }
}
