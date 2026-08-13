package io.pulseforge.controlplane.results;

import io.pulseforge.controlplane.config.ClickHouseProperties;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Repository;
import org.springframework.web.client.RestClient;

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

    private final RestClient restClient;
    private final ClickHouseProperties properties;

    public ClickHouseResultRepository(
            RestClient.Builder builder, ClickHouseProperties properties) {
        this.properties = properties;
        this.restClient =
                builder.baseUrl(properties.httpUrl())
                        .defaultHeader("X-ClickHouse-User", properties.username())
                        .defaultHeader("X-ClickHouse-Key", properties.password())
                        .defaultHeader("X-ClickHouse-Database", properties.database())
                        .build();
    }

    /** Per-step counters and the run's wall-clock span. */
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
                       uniqExact(worker_id),
                       toUnixTimestamp64Milli(min(window_start)),
                       toUnixTimestamp64Milli(max(window_end))
                FROM pulseforge.metric_snapshots
                WHERE run_id = '%s'
                GROUP BY step_name
                ORDER BY step_name
                FORMAT TabSeparated
                """
                        .formatted(runId);

        List<StepTotals> totals = new ArrayList<>();
        for (String[] row : query(sql)) {
            totals.add(
                    new StepTotals(
                            row[0],
                            Long.parseLong(row[1]),
                            Long.parseLong(row[2]),
                            Long.parseLong(row[3]),
                            Long.parseLong(row[4]),
                            Long.parseLong(row[5]),
                            Long.parseLong(row[6]),
                            Integer.parseInt(row[7]),
                            Long.parseLong(row[8]),
                            Long.parseLong(row[9])));
        }
        return totals;
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
                FROM pulseforge.latency_buckets
                WHERE run_id = '%s' %s
                GROUP BY bucket_micros
                ORDER BY bucket_micros
                FORMAT TabSeparated
                """
                .formatted(runId, extraPredicate);
    }

    /**
     * Walks the merged cumulative distribution to find the bucket holding each requested quantile.
     * One pass over the buckets per quantile; the bucket count is bounded by the histogram's
     * precision, not by the request count.
     */
    private long[] percentilesFrom(String sql, double[] quantiles) {
        List<String[]> rows = query(sql);
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

    private List<String[]> query(String sql) {
        String body =
                restClient
                        .post()
                        .uri(URI.create(properties.httpUrl() + "/"))
                        .body(sql)
                        .retrieve()
                        .body(String.class);

        if (body == null || body.isBlank()) {
            return List.of();
        }
        List<String[]> rows = new ArrayList<>();
        for (String line : body.split("\n")) {
            if (!line.isBlank()) {
                rows.add(line.split("\t"));
            }
        }
        return rows;
    }

    /**
     * Step names come from the scenario the operator submitted, so they are interpolated rather
     * than bound. Quotes and backslashes are escaped to keep a crafted name from changing the
     * query's shape.
     */
    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("'", "\\'");
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
            int workers,
            long firstWindowMillis,
            long lastWindowMillis) {}
}
