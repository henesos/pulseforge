package io.pulseforge.common.domain;

import java.util.Objects;

/**
 * A pass/fail condition evaluated once the run has finished.
 *
 * <p>Assertions are what makes the tool usable in CI: the run exits non-zero when one fails.
 *
 * @param type      what is being measured
 * @param parameter qualifier for the measurement, e.g. {@code 95} for {@code p95}; unused otherwise
 * @param operator  comparison to apply
 * @param threshold value to compare against, in the canonical unit of {@code type}
 *                  (milliseconds for {@link AssertionType#PERCENTILE}, percent for
 *                  {@link AssertionType#ERROR_RATE}, requests/second for
 *                  {@link AssertionType#THROUGHPUT})
 */
public record Assertion(
        AssertionType type, double parameter, ComparisonOperator operator, double threshold) {

    public Assertion {
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(operator, "operator must not be null");
        if (type == AssertionType.PERCENTILE && (parameter <= 0 || parameter >= 100)) {
            throw new IllegalArgumentException(
                    "percentile must be in (0, 100), was " + parameter);
        }
    }

    public boolean evaluate(double actual) {
        return operator.test(actual, threshold);
    }

    /** Human-readable form, mirroring how the assertion is written in the scenario YAML. */
    public String describe() {
        String subject =
                switch (type) {
                    case PERCENTILE -> "p" + trimTrailingZero(parameter);
                    case ERROR_RATE -> "errorRate";
                    case THROUGHPUT -> "throughput";
                };
        String unit =
                switch (type) {
                    case PERCENTILE -> "ms";
                    case ERROR_RATE -> "%";
                    case THROUGHPUT -> "rps";
                };
        return subject + " " + operator.symbol() + " " + trimTrailingZero(threshold) + unit;
    }

    private static String trimTrailingZero(double value) {
        return value == Math.rint(value) ? String.valueOf((long) value) : String.valueOf(value);
    }
}
