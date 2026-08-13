package io.pulseforge.common.domain;

import java.util.Arrays;

/** Comparison used by an {@link Assertion}. */
public enum ComparisonOperator {
    LESS_THAN("<"),
    LESS_THAN_OR_EQUAL("<="),
    GREATER_THAN(">"),
    GREATER_THAN_OR_EQUAL(">=");

    private final String symbol;

    ComparisonOperator(String symbol) {
        this.symbol = symbol;
    }

    public String symbol() {
        return symbol;
    }

    public boolean test(double actual, double threshold) {
        return switch (this) {
            case LESS_THAN -> actual < threshold;
            case LESS_THAN_OR_EQUAL -> actual <= threshold;
            case GREATER_THAN -> actual > threshold;
            case GREATER_THAN_OR_EQUAL -> actual >= threshold;
        };
    }

    public static ComparisonOperator fromSymbol(String symbol) {
        return Arrays.stream(values())
                .filter(operator -> operator.symbol.equals(symbol))
                .findFirst()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "unsupported comparison operator: " + symbol));
    }
}
