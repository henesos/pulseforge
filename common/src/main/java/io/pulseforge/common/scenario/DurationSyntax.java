package io.pulseforge.common.scenario;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the compact duration literals used in scenario YAML ({@code 120s}, {@code 30s},
 * {@code 2m}, {@code 500ms}).
 *
 * <p>ISO-8601 ({@code PT2M}) is deliberately not the surface syntax: scenarios are written by hand
 * and {@code 30s} is what people type.
 */
public final class DurationSyntax {

    private static final Pattern LITERAL = Pattern.compile("^(\\d+(?:\\.\\d+)?)\\s*(ms|s|m|h)$");

    private DurationSyntax() {
        throw new AssertionError("utility class");
    }

    public static Duration parse(String literal) {
        if (literal == null || literal.isBlank()) {
            throw new IllegalArgumentException("duration must not be blank");
        }
        Matcher matcher = LITERAL.matcher(literal.trim().toLowerCase());
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    "unsupported duration '" + literal + "', expected forms: 500ms, 30s, 2m, 1h");
        }
        double value = Double.parseDouble(matcher.group(1));
        return switch (matcher.group(2)) {
            case "ms" -> Duration.ofNanos(Math.round(value * 1_000_000));
            case "s" -> Duration.ofNanos(Math.round(value * 1_000_000_000L));
            case "m" -> Duration.ofNanos(Math.round(value * 60_000_000_000L));
            case "h" -> Duration.ofNanos(Math.round(value * 3_600_000_000_000L));
            default -> throw new IllegalStateException("unreachable unit " + matcher.group(2));
        };
    }

    /** Renders a duration back into the compact form, for round-tripping into reports. */
    public static String format(Duration duration) {
        long millis = duration.toMillis();
        if (millis % 3_600_000 == 0 && millis >= 3_600_000) {
            return (millis / 3_600_000) + "h";
        }
        if (millis % 60_000 == 0 && millis >= 60_000) {
            return (millis / 60_000) + "m";
        }
        if (millis % 1_000 == 0) {
            return (millis / 1_000) + "s";
        }
        return millis + "ms";
    }
}
