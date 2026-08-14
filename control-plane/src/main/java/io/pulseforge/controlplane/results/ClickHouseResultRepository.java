package io.pulseforge.controlplane.results;

import io.pulseforge.controlplane.config.ClickHouseProperties;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Repository;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * Reads run results out of ClickHouse.
 *
 * <p>The percentile query is the important one. Percentiles are derived by summing bucket counts
 * across every worker and every interval, then walking the cumulative distribution to find the
 * first bucket at or beyond the target rank:
 *
 * <pre>
 *   SELECT bucket_micros, sum(count) ... GROUP BY bucket_micros ORDER BY bucket_micros
 *   -- then: running total >= ceil(total * q) -> that bucket is the q-th percentile
 * </pre>
 *
 * <p>This is the whole point of storing distributions rather than pre-computed percentiles. Taking
 * five workers' p99 values and averaging them produces a number that is not the p99 of anything —
 * it is systematically wrong the moment load is unevenly distributed, and there is no way to bound
 * the error after the fact.
 *
 * <p>Queries go over the HTTP interface with {@code TabSeparated} output. The control plane only
 * reads aggregates, so pulling in the JDBC driver and a connection pool would buy nothing.
 */
@Repository
public class ClickHouseResultRepository {

    /** Bounds a results read; a hung ClickHouse must not pin a request thread indefinitely. */
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);

    private static final Duration READ_TIMEOUT = Duration.ofSeconds(30);

    private final RestClient restClient;
    private final ClickHouseProperties properties;

    public ClickHouseResultRepository(
            RestClient.Builder builder, ClickHouseProperties properties) {
        this.properties = properties;

        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout((int) CONNECT_TIMEOUT.toMillis());
        requestFactory.setReadTimeout((int) READ_TIMEOUT.toMillis());

        this.restClient =
                builder.baseUrl(properties.httpUrl())
                        .requestFactory(requestFactory)
                        .defaultHeader("X-ClickHouse-User", properties.username())
                        .defaultHeader("X-ClickHouse-Key", properties.password())
                        .defaultHeader("X-ClickHouse-Database", properties.database())
                        .build();
    }

    /**
     * Qualifies a table with the configured database.
     *
     * <p>The {@code X-ClickHouse-Database} header alone is not enough: every query below names its
     * table explicitly, so hardcoding the database would silently ignore the configured one.
     */
    private String table(String name) {
        return properties.database() + "." + name;
    }

    /**
     * Per-step counters.
     *
     * <p>Deliberately no window bounds. They were read here to divide requests by the span the
     * measurements happened to cover, which reports the rate over the period the system was busy
     * rather than the period it was asked to be busy. Throughput is now divided by the run's own
     * duration, and reading a span nothing uses would invite it back.
     */
    public List<StepTotals> stepTotals(UUID runId) {
        String sql =
                """
                SELECT step_name,
                       sum(request_count),
                       sum(error_count),
                       sum(dropped_samples),
                       sum(skipped_requests),
                       sum(sum_micros),
                       max(max_micros),
                       uniqExact(worker_id)
                FROM %s
                WHERE run_id = '%s'
                GROUP BY step_name
                ORDER BY step_name
                FORMAT TabSeparated
                """
                        .formatted(table("metric_snapshots"), runId);

        List<StepTotals> totals = new ArrayList<>();
        for (String[] row : query(sql, 8)) {
            totals.add(
                    new StepTotals(
                            row[0],
                            Long.parseLong(row[1]),
                            Long.parseLong(row[2]),
                            Long.parseLong(row[3]),
                            Long.parseLong(row[4]),
                            Long.parseLong(row[5]),
                            Long.parseLong(row[6]),
                            Integer.parseInt(row[7])));
        }
        return totals;
    }

    /**
     * How many distinct workers contributed measurements to the run.
     *
     * <p>A run-wide {@code uniqExact} rather than a maximum over the per-step counts: workers do not
     * all serve every step, so the largest per-step figure understates the fleet.
     */
    public int contributingWorkers(UUID runId) {
        String sql =
                """
                SELECT uniqExact(worker_id)
                FROM %s
                WHERE run_id = '%s'
                FORMAT TabSeparated
                """
                        .formatted(table("metric_snapshots"), runId);

        List<String[]> rows = query(sql, 1);
        return rows.isEmpty() ? 0 : Integer.parseInt(rows.get(0)[0]);
    }

    /**
     * What the ingestor received for this run and never stored.
     *
     * <p>Read as its own query rather than joined into the totals: the loss table is keyed by run
     * and reason, and a run with no loss has no rows at all, which a join would turn into no
     * results.
     */
    public IngestLosses ingestLosses(UUID runId) {
        String sql =
                """
                SELECT sum(lost_requests), sum(lost_dropped), sum(lost_skipped)
                FROM %s
                WHERE run_id = '%s'
                FORMAT TabSeparated
                """
                        .formatted(table("ingest_losses"), runId);

        List<String[]> rows = query(sql, 3);
        if (rows.isEmpty()) {
            return IngestLosses.NONE;
        }
        String[] row = rows.get(0);
        return new IngestLosses(
                Long.parseLong(row[0]), Long.parseLong(row[1]), Long.parseLong(row[2]));
    }

    /**
     * Global percentiles for one step, merged across every worker.
     *
     * @param quantiles fractions in (0, 1), e.g. 0.5, 0.95, 0.99
     * @return latency in microseconds, one entry per requested quantile, in the same order
     */
    public long[] percentiles(UUID runId, String stepName, double... quantiles) {
        return percentilesFrom(
                bucketQuery(runId, "AND step_name = '" + escape(stepName) + "'"), quantiles);
    }

    /**
     * Global percentiles across every step of the run.
     *
     * <p>Merging all steps is the right basis for a scenario-level assertion such as
     * {@code p95 < 250ms}: the statement is about what a user of the whole flow experiences, not
     * about one endpoint in isolation.
     */
    public long[] percentilesForRun(UUID runId, double... quantiles) {
        return percentilesFrom(bucketQuery(runId, ""), quantiles);
    }

    private String bucketQuery(UUID runId, String extraPredicate) {
        return """
                SELECT bucket_micros, sum(count) AS c
                FROM %s
                WHERE run_id = '%s' %s
                GROUP BY bucket_micros
                ORDER BY bucket_micros
                FORMAT TabSeparated
                """
                .formatted(table("latency_buckets"), runId, extraPredicate);
    }

    /**
     * Walks the merged cumulative distribution to find the bucket holding each requested quantile.
     * One pass over the buckets per quantile; the bucket count is bounded by the histogram's
     * precision, not by the request count.
     */
    private long[] percentilesFrom(String sql, double[] quantiles) {
        List<String[]> rows = query(sql, 2);
        long[] buckets = new long[rows.size()];
        long[] counts = new long[rows.size()];
        long total = 0;
        for (int i = 0; i < rows.size(); i++) {
            buckets[i] = Long.parseLong(rows.get(i)[0]);
            counts[i] = Long.parseLong(rows.get(i)[1]);
            total += counts[i];
        }

        long[] results = new long[quantiles.length];
        if (total == 0) {
            return results;
        }

        for (int q = 0; q < quantiles.length; q++) {
            long rank = (long) Math.ceil(quantiles[q] * total);
            long running = 0;
            for (int i = 0; i < buckets.length; i++) {
                running += counts[i];
                if (running >= rank) {
                    results[q] = buckets[i];
                    break;
                }
            }
        }
        return results;
    }

    private List<String[]> query(String sql, int expectedColumns) {
        String body;
        try {
            body =
                    restClient
                            .post()
                            .uri(URI.create(properties.httpUrl() + "/"))
                            .body(sql)
                            .retrieve()
                            .body(String.class);
        } catch (RestClientException e) {
            // A transport or server error here means the results are unknown, not empty. Letting
            // the raw exception escape would surface as a 500 with no indication of which
            // dependency failed.
            throw new ResultsUnavailableException("ClickHouse query failed", e);
        }

        if (body == null || body.isBlank()) {
            return List.of();
        }
        List<String[]> rows = new ArrayList<>();
        for (String line : body.split("\n")) {
            if (line.isBlank()) {
                continue;
            }
            // TabSeparated escapes tabs and newlines inside values, so splitting on the raw
            // characters keeps columns aligned — but the values still carry their escapes.
            String[] row = line.split("\t", -1);
            if (row.length != expectedColumns) {
                throw new ResultsUnavailableException(
                        "expected %d columns from ClickHouse, got %d"
                                .formatted(expectedColumns, row.length),
                        null);
            }
            for (int i = 0; i < row.length; i++) {
                row[i] = unescape(row[i]);
            }
            rows.add(row);
        }
        return rows;
    }

    /** Reverses ClickHouse's {@code TabSeparated} value escaping. */
    private static String unescape(String value) {
        if (value.indexOf('\\') < 0) {
            return value;
        }
        StringBuilder out = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c != '\\' || i + 1 == value.length()) {
                out.append(c);
                continue;
            }
            char next = value.charAt(++i);
            out.append(
                    switch (next) {
                        case 't' -> '\t';
                        case 'n' -> '\n';
                        case 'r' -> '\r';
                        case 'b' -> '\b';
                        case 'f' -> '\f';
                        case '0' -> '\0';
                        default -> next;
                    });
        }
        return out.toString();
    }

    /** Raised when the measurements cannot be read, as distinct from a run having none. */
    public static class ResultsUnavailableException extends RuntimeException {
        public ResultsUnavailableException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Step names come from the scenario the operator submitted, so they are interpolated rather
     * than bound. Quotes and backslashes are escaped to keep a crafted name from changing the
     * query's shape.
     */
    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("'", "\\'");
    }

    /**
     * Measurements that reached the ingestor and were never stored, plus the counters the lost
     * snapshots were carrying.
     *
     * @param lostRequests measurements missing from the percentiles above
     * @param lostDropped  worker-side drops the lost snapshots were reporting, which would
     *                     otherwise make the run understate its own incompleteness
     * @param lostSkipped  likewise for requests the generator never issued
     */
    public record IngestLosses(long lostRequests, long lostDropped, long lostSkipped) {
        public static final IngestLosses NONE = new IngestLosses(0, 0, 0);
    }

    /** Raw counters for one step, before percentiles are attached. */
    public record StepTotals(
            String stepName,
            long requests,
            long errors,
            long droppedSamples,
            long skippedRequests,
            long sumMicros,
            long maxMicros,
            int workers) {}
}
