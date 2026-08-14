package io.pulseforge.controlplane.results;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.pulseforge.common.domain.RunStatus;
import io.pulseforge.controlplane.persistence.TestRunEntity;
import java.time.Instant;
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
        service = new RunResultService(repository);
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
                                        2,
                                        STARTED.toEpochMilli(),
                                        STARTED.plusSeconds(10).toEpochMilli())));
        when(repository.percentiles(any(), anyString(), anyDouble(), anyDouble(), anyDouble()))
                .thenReturn(new long[] {1_000, 2_000, 3_000});
        when(repository.percentilesForRun(any(), anyDouble(), anyDouble(), anyDouble()))
                .thenReturn(new long[] {1_000, 2_000, 3_000});
        when(repository.contributingWorkers(any())).thenReturn(2);
        when(repository.ingestLosses(any()))
                .thenReturn(ClickHouseResultRepository.IngestLosses.NONE);
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

    private static TestRunEntity run() {
        TestRunEntity run =
                new TestRunEntity(UUID.randomUUID(), UUID.randomUUID(), 100, 10, 0, 2, STARTED);
        run.markRunning(STARTED);
        run.terminate(RunStatus.COMPLETED, null, STARTED.plusSeconds(10));
        return run;
    }
}
