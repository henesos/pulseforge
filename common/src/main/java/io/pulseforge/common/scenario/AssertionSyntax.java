package io.pulseforge.common.scenario;

import io.pulseforge.common.domain.Assertion;
import io.pulseforge.common.domain.AssertionType;
import io.pulseforge.common.domain.ComparisonOperator;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the one-line assertion expressions a scenario declares, e.g. {@code p95 < 250ms},
 * {@code errorRate < 1%}, {@code throughput > 380}.
 *
 * <p>The expressions are intentionally close to how an engineer would state the requirement out
 * loud; a nested object syntax would be more machine-friendly and far less likely to be written.
 */
public final class AssertionSyntax {

    private static final Pattern PERCENTILE =
            Pattern.compile("^p(\\d+(?:\\.\\d+)?)\\s*(<=|>=|<|>)\\s*(\\d+(?:\\.\\d+)?)\\s*(ms|s)?$");

    private static final Pattern ERROR_RATE =
            Pattern.compile("^errorrate\\s*(<=|>=|<|>)\\s*(\\d+(?:\\.\\d+)?)\\s*%?$");

    private static final Pattern THROUGHPUT =
            Pattern.compile("^throughput\\s*(<=|>=|<|>)\\s*(\\d+(?:\\.\\d+)?)\\s*(rps)?$");

    private AssertionSyntax() {
        throw new AssertionError("utility class");
    }

    public static Assertion parse(String expression) {
        if (expression == null || expression.isBlank()) {
            throw new IllegalArgumentException("assertion must not be blank");
        }
        String normalised = expression.trim().toLowerCase(Locale.ROOT);

        Matcher percentile = PERCENTILE.matcher(normalised);
        if (percentile.matches()) {
            double threshold = Double.parseDouble(percentile.group(3));
            if ("s".equals(percentile.group(4))) {
                threshold *= 1_000;
            }
            return new Assertion(
                    AssertionType.PERCENTILE,
                    Double.parseDouble(percentile.group(1)),
                    ComparisonOperator.fromSymbol(percentile.group(2)),
                    threshold);
        }

        Matcher errorRate = ERROR_RATE.matcher(normalised);
        if (errorRate.matches()) {
            return new Assertion(
                    AssertionType.ERROR_RATE,
                    0,
                    ComparisonOperator.fromSymbol(errorRate.group(1)),
                    Double.parseDouble(errorRate.group(2)));
        }

        Matcher throughput = THROUGHPUT.matcher(normalised);
        if (throughput.matches()) {
            return new Assertion(
                    AssertionType.THROUGHPUT,
                    0,
                    ComparisonOperator.fromSymbol(throughput.group(1)),
                    Double.parseDouble(throughput.group(2)));
        }

        throw new IllegalArgumentException(
                "unsupported assertion '"
                        + expression
                        + "', expected forms: 'p95 < 250ms', 'errorRate < 1%', 'throughput > 380'");
    }
}
