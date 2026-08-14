package io.pulseforge.cli;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.PrintStream;

/** Renders a run's results as a terminal report. */
public class ResultsPrinter {

    private static final String STEP_ROW = "%-24s %9s %8s %7s %9s %9s %9s %9s%n";

    private final PrintStream out;

    public ResultsPrinter(PrintStream out) {
        this.out = out;
    }

    public void print(JsonNode payload) {
        JsonNode results = payload.get("results");

        out.println();
        out.printf(STEP_ROW, "step", "requests", "errors", "err%", "p50", "p95", "p99", "max");
        out.println("-".repeat(90));

        for (JsonNode step : results.withArray("steps")) {
            out.printf(
                    STEP_ROW,
                    truncate(step.get("stepName").asText(), 24),
                    step.get("requests").asLong(),
                    step.get("errors").asLong(),
                    "%.2f%%".formatted(step.get("errorRatePercent").asDouble()),
                    ms(step.get("p50Ms")),
                    ms(step.get("p95Ms")),
                    ms(step.get("p99Ms")),
                    ms(step.get("maxMs")));
        }

        out.println("-".repeat(90));
        out.printf(
                STEP_ROW,
                "run total",
                results.get("totalRequests").asLong(),
                results.get("totalErrors").asLong(),
                "%.2f%%".formatted(results.get("errorRatePercent").asDouble()),
                ms(results.get("p50Ms")),
                ms(results.get("p95Ms")),
                ms(results.get("p99Ms")),
                ms(results.get("maxMs")));

        out.println();
        out.printf(
                "throughput %.1f req/s   workers %d   dropped samples %d   skipped requests %d%n",
                results.get("throughputPerSecond").asDouble(),
                results.get("workers").asInt(),
                results.get("droppedSamples").asLong(),
                results.get("skippedRequests").asLong());
        long unstoredSamples = results.path("unstoredSamples").asLong();
        if (unstoredSamples > 0) {
            out.printf("unstored samples %d%n", unstoredSamples);
        }

        printMeasurementWarnings(results);
        printAssertions(payload);
    }

    /**
     * Anything that makes the numbers above less than trustworthy is stated plainly, rather than
     * left for the reader to notice in a counter.
     */
    private void printMeasurementWarnings(JsonNode results) {
        String status = results.get("status").asText();
        if (!"COMPLETED".equals(status)) {
            out.println();
            out.printf("!! run status %s%n", status);
            JsonNode reason = results.get("statusReason");
            if (reason != null && !reason.isNull()) {
                out.printf("   %s%n", reason.asText());
            }
        }
        if (results.get("droppedSamples").asLong() > 0) {
            out.println("!! samples were dropped; percentiles are computed from an incomplete population");
        }
        long unstored = results.path("unstoredSamples").asLong();
        if (unstored > 0) {
            // Named apart from a dropped sample on purpose: the two are fixed in different places,
            // and telling an operator to raise a worker's queue when the ingestor is the bottleneck
            // sends them to rebuild the wrong thing.
            out.printf(
                    "!! %d measurements reached the ingestor and were never stored; "
                            + "the ingestor or ClickHouse could not keep up%n",
                    unstored);
        }
        if (results.get("skippedRequests").asLong() > 0) {
            out.println("!! requests were skipped; the offered rate was not achieved");
        }
    }

    private void printAssertions(JsonNode payload) {
        JsonNode verdict = payload.get("verdict");
        if (verdict == null || verdict.isNull()) {
            out.println();
            out.println("no assertions declared");
            return;
        }

        out.println();
        out.printf("%-28s %12s   %s%n", "ASSERTIONS", "actual", "result");
        for (JsonNode assertion : verdict.withArray("assertions")) {
            out.printf(
                    "  %-26s %12.2f   %s%n",
                    assertion.get("expression").asText(),
                    assertion.get("actual").asDouble(),
                    assertion.get("passed").asBoolean() ? "PASS" : "FAIL");
        }
        out.println();
        out.printf("  %s%n", verdict.get("passed").asBoolean() ? "PASS" : "FAIL");
    }

    private static String ms(JsonNode node) {
        return "%.2fms".formatted(node.asDouble());
    }

    private static String truncate(String value, int max) {
        return value.length() <= max ? value : value.substring(0, max - 1) + "…";
    }
}
