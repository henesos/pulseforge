package io.pulseforge.ingestor;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import io.pulseforge.common.metrics.HistogramCodec;
import io.pulseforge.common.protocol.HistogramSnapshot;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The claim this project rests on, verified against a real ClickHouse: percentiles merged from
 * per-worker histograms equal the percentile of the combined population, and averaging per-worker
 * percentiles does not.
 *
 * <p>Uses a container rather than a mock because the merge is expressed as SQL over a
 * SummingMergeTree. A mocked datasource would verify that the code sends a string, which is not the
 * part that can be wrong.
 */
@Testcontainers
class ClickHousePercentileIT {

    @Container
    static final ClickHouseContainer CLICKHOUSE =
            new ClickHouseContainer("clickhouse/clickhouse-server:24.8-alpine");

    private static HikariDataSource dataSource;
    private static SnapshotWriter writer;

    @BeforeAll
    static void setUp() throws Exception {
        dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(CLICKHOUSE.getJdbcUrl());
        dataSource.setUsername(CLICKHOUSE.getUsername());
        dataSource.setPassword(CLICKHOUSE.getPassword());
        dataSource.setMaximumPoolSize(2);

        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE IF NOT EXISTS pulseforge");
            statement.execute(
                    """
                    CREATE TABLE pulseforge.metric_snapshots (
                        run_id UUID, worker_id String, step_name String,
                        window_start DateTime64(3, 'UTC'), window_end DateTime64(3, 'UTC'),
                        request_count UInt64, error_count UInt64, dropped_samples UInt64,
                        skipped_requests UInt64, min_micros UInt64, max_micros UInt64,
                        sum_micros UInt64, histogram String
                    ) ENGINE = MergeTree ORDER BY (run_id, step_name, worker_id, window_start)
                    """);
            statement.execute(
                    """
                    CREATE TABLE pulseforge.latency_buckets (
                        run_id UUID, step_name String, window_start DateTime64(3, 'UTC'),
                        bucket_micros UInt64, count UInt64
                    ) ENGINE = SummingMergeTree(count)
                      ORDER BY (run_id, step_name, window_start, bucket_micros)
                    """);
        }

        writer = new SnapshotWriter(dataSource);
    }

    @AfterAll
    static void tearDown() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @Test
    @DisplayName("percentiles merged across workers match the combined population, not their average")
    void mergesPercentilesAcrossWorkers() throws Exception {
        UUID runId = UUID.randomUUID();

        // Deliberately lopsided, which is the case where averaging goes wrong: four fast workers
        // carrying most of the traffic, one slow worker carrying a little.
        List<HistogramSnapshot> batch = new ArrayList<>();
        Histogram combined = HistogramCodec.newHistogram();

        for (int worker = 0; worker < 4; worker++) {
            Histogram fast = HistogramCodec.newHistogram();
            for (int i = 0; i < 2_000; i++) {
                fast.recordValue(2_000);
            }
            combined.add(fast);
            batch.add(snapshot(runId, "worker-" + worker, fast));
        }

        Histogram slow = HistogramCodec.newHistogram();
        for (int i = 0; i < 500; i++) {
            slow.recordValue(400_000);
        }
        combined.add(slow);
        batch.add(snapshot(runId, "worker-slow", slow));

        writer.write(batch);

        long mergedP99 = queryPercentile(runId, 0.99);
        long mergedP50 = queryPercentile(runId, 0.50);

        assertThat(queryTotalCount(runId))
                .as("every sample must survive the round trip through the bucket table")
                .isEqualTo(8_500);

        assertThat(mergedP50)
                .as("the median sits in the fast population")
                .isCloseTo(2_000, org.assertj.core.data.Offset.offset(50L));

        // 8500 samples, 500 of them slow: the slow ones are the top 5.9%, so p99 is squarely
        // inside them. A fast worker's own p99 is 2ms and would drag an average far below this.
        assertThat(mergedP99)
                .as("the merged p99 must land in the slow tail")
                .isCloseTo(400_000, org.assertj.core.data.Offset.offset(1_000L));

        assertThat(mergedP99)
                .as("ClickHouse's merge must agree with HdrHistogram's own answer")
                .isCloseTo(
                        combined.getValueAtPercentile(99),
                        org.assertj.core.data.Offset.offset(1_000L));

        double averagedP99 = (2_000L * 4 + 400_000L) / 5.0;
        assertThat(averagedP99)
                .as("averaging the five workers' p99 values reports ~82ms for a 400ms tail")
                .isLessThan(mergedP99 / 4.0);
    }

    @Test
    @DisplayName("counters survive the write, including the ones that admit data loss")
    void persistsDroppedAndSkippedCounters() throws Exception {
        UUID runId = UUID.randomUUID();
        Histogram histogram = HistogramCodec.newHistogram();
        histogram.recordValue(1_000);

        writer.write(
                List.of(
                        new HistogramSnapshot(
                                runId,
                                "worker-1",
                                "GET /api/fast",
                                Instant.now(),
                                Instant.now().plusSeconds(1),
                                1,
                                0,
                                42,
                                7,
                                1_000,
                                1_000,
                                1_000,
                                HistogramCodec.encode(histogram))));

        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                "SELECT sum(dropped_samples), sum(skipped_requests) "
                                        + "FROM pulseforge.metric_snapshots WHERE run_id = '"
                                        + runId
                                        + "'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong(1)).isEqualTo(42);
            assertThat(rs.getLong(2)).isEqualTo(7);
        }
    }

    private static HistogramSnapshot snapshot(UUID runId, String workerId, Histogram histogram) {
        return new HistogramSnapshot(
                runId,
                workerId,
                "GET /api/fast",
                Instant.now(),
                Instant.now().plusSeconds(1),
                histogram.getTotalCount(),
                0,
                0,
                0,
                histogram.getMinValue(),
                histogram.getMaxValue(),
                0,
                HistogramCodec.encode(histogram));
    }

    /** The same cumulative-distribution walk the control plane performs, expressed in SQL. */
    private static long queryPercentile(UUID runId, double quantile) throws Exception {
        String sql =
                """
                SELECT quantileExactWeighted(%f)(bucket_micros, count)
                FROM pulseforge.latency_buckets WHERE run_id = '%s'
                """
                        .formatted(quantile, runId);
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            assertThat(rs.next()).isTrue();
            return rs.getLong(1);
        }
    }

    private static long queryTotalCount(UUID runId) throws Exception {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                "SELECT sum(count) FROM pulseforge.latency_buckets WHERE run_id = '"
                                        + runId
                                        + "'")) {
            assertThat(rs.next()).isTrue();
            return rs.getLong(1);
        }
    }
}
