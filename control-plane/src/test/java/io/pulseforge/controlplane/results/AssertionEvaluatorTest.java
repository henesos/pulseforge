package io.pulseforge.controlplane.results;

import static org.assertj.core.api.Assertions.assertThat;

import io.pulseforge.common.domain.RunStatus;
import io.pulseforge.common.scenario.AssertionSyntax;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The verdict is the only thing a pipeline reads, so the mapping from assertion to measured field
 * is worth pinning: substituting p50 for p95 would turn a breached latency budget into a green
 * build with nothing else going red.
 */
class AssertionEvaluatorTest {

    private final AssertionEvaluator evaluator = new AssertionEvaluator();

    @Test
    @DisplayName("a run that measured nothing fails rather than passing on zeroes")
    void noMeasurementsFails() {
        RunResults empty = results(0, 0, 0, 0, 0);

        AssertionEvaluator.Verdict verdict =
                evaluator.evaluate(List.of(AssertionSyntax.parse("p95 < 250ms")), empty);

        assertThat(verdict.passed())
                .as("zero percentiles mean 'no data', not 'fast'")
                .isFalse();
        assertThat(verdict.assertions()).singleElement().satisfies(o -> {
            assertThat(o.passed()).isFalse();
            assertThat(o.actual()).isNaN();
        });
    }

    @Test
    @DisplayName("each percentile assertion reads its own measured field")
    void percentilesMapToTheirOwnField() {
        // p50 is inside the budget, p95 and p99 are not. Reading the wrong field flips a verdict.
        RunResults results = results(1_000, 10, 200, 800, 900);

        assertThat(evaluator.evaluate(List.of(AssertionSyntax.parse("p95 < 250ms")), results).passed())
                .isFalse();
        assertThat(evaluator.evaluate(List.of(AssertionSyntax.parse("p50 < 250ms")), results).passed())
                .isTrue();
        assertThat(evaluator.evaluate(List.of(AssertionSyntax.parse("p99 < 250ms")), results).passed())
                .isFalse();
    }

    @Test
    @DisplayName("every assertion is reported, not just the one that failed")
    void reportsEveryOutcome() {
        RunResults results = results(1_000, 10, 200, 800, 900);

        AssertionEvaluator.Verdict verdict =
                evaluator.evaluate(
                        List.of(
                                AssertionSyntax.parse("p95 < 250ms"),
                                AssertionSyntax.parse("errorRate < 5%"),
                                AssertionSyntax.parse("throughput > 100")),
                        results);

        assertThat(verdict.passed()).isFalse();
        assertThat(verdict.assertions()).hasSize(3);
        assertThat(verdict.assertions()).extracting(AssertionEvaluator.AssertionOutcome::passed)
                .containsExactly(false, true, true);
    }

    @Test
    @DisplayName("error rate and throughput read their own fields too")
    void nonLatencyAssertions() {
        RunResults results = results(1_000, 10, 5, 5, 5);

        assertThat(
                        evaluator
                                .evaluate(List.of(AssertionSyntax.parse("errorRate < 0.5%")), results)
                                .passed())
                .as("1% measured against a 0.5% budget")
                .isFalse();
        assertThat(
                        evaluator
                                .evaluate(List.of(AssertionSyntax.parse("throughput > 500")), results)
                                .passed())
                .as("200 req/s measured against a 500 req/s floor")
                .isFalse();
    }

    private static RunResults results(
            long totalRequests, long totalErrors, double p50, double p95, double p99) {
        double errorRate = totalRequests == 0 ? 0 : totalErrors * 100.0d / totalRequests;
        return new RunResults(
                UUID.randomUUID(),
                RunStatus.COMPLETED,
                null,
                Instant.EPOCH,
                Instant.EPOCH.plusSeconds(5),
                totalRequests,
                totalErrors,
                errorRate,
                totalRequests / 5.0d,
                p50,
                p95,
                p99,
                p99,
                0,
                0,
                1,
                List.of());
    }
}
