package io.pulseforge.controlplane.results;

import io.pulseforge.common.domain.Assertion;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Turns a finished run into a PASS or FAIL verdict.
 *
 * <p>This is what makes the tool usable in CI: the API reports the verdict and the CLI reflects it
 * in an exit code, so a pipeline can gate on latency the same way it gates on unit tests.
 */
@Component
public class AssertionEvaluator {

    private static final Logger log = LoggerFactory.getLogger(AssertionEvaluator.class);

    public Verdict evaluate(List<Assertion> assertions, RunResults results) {
        // A run with no measurements reports zero for every percentile and zero for the error rate,
        // which satisfies `p95 < 250ms` and `errorRate < 1%` for the wrong reason. Passing here
        // would gate a deploy green on an experiment that never produced a single sample.
        if (results.totalRequests() == 0) {
            log.warn("Run {}: no measurements recorded; every assertion fails", results.runId());
            return new Verdict(
                    false,
                    assertions.stream()
                            .map(a -> new AssertionOutcome(a.describe(), Double.NaN, false))
                            .toList());
        }

        List<AssertionOutcome> outcomes = new ArrayList<>(assertions.size());
        boolean allPassed = true;

        for (Assertion assertion : assertions) {
            double actual;
            try {
                actual = actualValue(assertion, results);
            } catch (UnsupportedPercentileException e) {
                // One unanswerable assertion must not hide what every other one measured. It counts
                // as a failure and the rest of the report survives.
                log.warn("Run {}: {}", results.runId(), e.getMessage());
                outcomes.add(new AssertionOutcome(assertion.describe(), Double.NaN, false));
                allPassed = false;
                continue;
            }
            boolean passed = assertion.evaluate(actual);
            allPassed &= passed;
            outcomes.add(new AssertionOutcome(assertion.describe(), actual, passed));
        }

        return new Verdict(allPassed, outcomes);
    }

    private double actualValue(Assertion assertion, RunResults results) {
        return switch (assertion.type()) {
            case PERCENTILE -> percentile(assertion.parameter(), results);
            case ERROR_RATE -> results.errorRatePercent();
            case THROUGHPUT -> results.throughputPerSecond();
        };
    }

    /**
     * Only the three percentiles the results carry are answerable. An assertion on {@code p99.9}
     * would need a query the result set does not contain, and quietly substituting p99 would make
     * the report lie about what was checked.
     */
    private double percentile(double parameter, RunResults results) {
        if (parameter == 50) {
            return results.p50Ms();
        }
        if (parameter == 95) {
            return results.p95Ms();
        }
        if (parameter == 99) {
            return results.p99Ms();
        }
        throw new UnsupportedPercentileException(
                "assertions currently support p50, p95 and p99; got p" + parameter);
    }

    /** Outcome of one assertion. {@code actual} is in the unit implied by the expression. */
    public record AssertionOutcome(String expression, double actual, boolean passed) {}

    /** Overall result. {@code passed} is false if any single assertion failed. */
    public record Verdict(boolean passed, List<AssertionOutcome> assertions) {

        public String label() {
            return passed ? "PASS" : "FAIL";
        }
    }

    /** Raised for a percentile the result set cannot answer. */
    public static class UnsupportedPercentileException extends RuntimeException {
        public UnsupportedPercentileException(String message) {
            super(message);
        }
    }
}
