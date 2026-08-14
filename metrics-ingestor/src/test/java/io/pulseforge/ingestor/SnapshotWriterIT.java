package io.pulseforge.ingestor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zaxxer.hikari.HikariDataSource;
import io.pulseforge.common.metrics.HistogramCodec;
import io.pulseforge.common.protocol.HistogramSnapshot;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Each snapshot is stored in two tables and ClickHouse offers no transaction spanning them, so a
 * failure between the two inserts leaves the run's measurements inconsistent. This pins which way
 * that inconsistency is allowed to point.
 *
 * <p>It matters because only one direction is detectable. Counters without their distribution give
 * a percentile computed over part of the population while every count looks complete — a p99 that
 * is wrong and reads as ordinary. Distribution without counters under-reports throughput, and shows
 * up directly as {@code sum(count) > sum(request_count)}.
 *
 * <p>Run against a real server: what is being checked is what actually reached the tables, which a
 * mocked {@code DataSource} cannot answer.
 */
@Testcontainers
class SnapshotWriterIT {

    @Container
    static final ClickHouseContainer CLICKHOUSE =
            new ClickHouseContainer("clickhouse/clickhouse-server:24.8-alpine")
                    .withUsername("pulseforge")
                    .withPassword("pulseforge")
                    .withDatabaseName("pulseforge")
                    .withEnv("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1");

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
    @DisplayName("a batch that lands completely leaves the two tables in exact agreement")
    void aCompleteBatchIsConsistent() throws Exception {
        UUID runId = UUID.randomUUID();

        writer.write(
                List.of(
                        snapshot(runId, "worker-1", histogramOf(50, 1_000)),
                        snapshot(runId, "worker-2", histogramOf(30, 2_000))));

        assertThat(bucketTotal(runId))
                .as("the parity the README claims, checked rather than asserted")
                .isEqualTo(counterTotal(runId))
                .isEqualTo(80);
    }

    @Test
    @DisplayName("when the counter write fails the distribution is still the complete one")
    void aFailedCounterWriteNeverLeavesAPartialDistribution() throws Exception {
        UUID runId = UUID.randomUUID();
        SnapshotWriter failing = new SnapshotWriter(failingOn("metric_snapshots"));

        assertThatThrownBy(
                        () ->
                                failing.write(
                                        List.of(snapshot(runId, "worker-1", histogramOf(50, 1_000)))))
                .isInstanceOf(SnapshotWriter.PartialWriteException.class)
                .hasMessageContaining("but not their counters");

        assertThat(bucketTotal(runId))
                .as("every measurement of the window is in the bucket table")
                .isEqualTo(50);
        assertThat(counterTotal(runId))
                .as("the run under-reports requests, which is the visible half of the failure")
                .isZero();
        assertThat(bucketTotal(runId))
                .as("buckets may exceed counters; the reverse would be a silently wrong percentile")
                .isGreaterThan(counterTotal(runId));
    }

    @Test
    @DisplayName("one unreadable histogram costs its own latency, not the whole batch")
    void aCorruptHistogramDoesNotSinkTheBatch() throws Exception {
        UUID runId = UUID.randomUUID();
        HistogramSnapshot corrupt =
                new HistogramSnapshot(
                        runId,
                        "worker-corrupt",
                        "GET /api/fast",
                        Instant.now(),
                        Instant.now().plusSeconds(1),
                        7,
                        0,
                        0,
                        0,
                        1_000,
                        1_000,
                        7_000,
                        "bm90IGEgaGlzdG9ncmFt");

        writer.write(
                List.of(
                        snapshot(runId, "worker-1", histogramOf(50, 1_000)),
                        corrupt,
                        snapshot(runId, "worker-2", histogramOf(30, 2_000))));

        assertThat(counterTotal(runId))
                .as("the requests really happened, so throughput and error rate stay right")
                .isEqualTo(87);
        assertThat(bucketTotal(runId))
                .as("the two readable histograms are stored in full")
                .isEqualTo(80);
        assertThat(counterTotal(runId) - bucketTotal(runId))
                .as("the gap names the loss exactly: the corrupt snapshot's own requests")
                .isEqualTo(7);
    }

    /**
     * A {@link DataSource} whose connections refuse to prepare one of the two inserts, so the
     * failure lands precisely between them rather than being simulated around the writer.
     */
    private static DataSource failingOn(String table) {
        return (DataSource)
                Proxy.newProxyInstance(
                        SnapshotWriterIT.class.getClassLoader(),
                        new Class<?>[] {DataSource.class},
                        (proxy, method, args) -> {
                            if (!method.getName().equals("getConnection")) {
                                return invoke(dataSource, method, args);
                            }
                            return Proxy.newProxyInstance(
                                    SnapshotWriterIT.class.getClassLoader(),
                                    new Class<?>[] {Connection.class},
                                    refusing(dataSource.getConnection(), table));
                        });
    }

    /** Delegates every call to a real connection except the insert into {@code table}. */
    private static InvocationHandler refusing(Connection real, String table) {
        return (proxy, method, args) -> {
            if (method.getName().equals("prepareStatement")
                    && args != null
                    && args[0] instanceof String sql
                    && sql.contains(table)) {
                throw new SQLException("simulated failure inserting into " + table);
            }
            return invoke(real, method, args);
        };
    }

    private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args)
            throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static Histogram histogramOf(int samples, long latencyMicros) {
        Histogram histogram = HistogramCodec.newHistogram();
        for (int i = 0; i < samples; i++) {
            histogram.recordValue(latencyMicros);
        }
        return histogram;
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
                histogram.getTotalCount() * histogram.getMaxValue(),
                HistogramCodec.encode(histogram));
    }

    private static long bucketTotal(UUID runId) throws Exception {
        return scalar(
                "SELECT sum(count) FROM pulseforge.latency_buckets WHERE run_id = '" + runId + "'");
    }

    private static long counterTotal(UUID runId) throws Exception {
        return scalar(
                "SELECT sum(request_count) FROM pulseforge.metric_snapshots WHERE run_id = '"
                        + runId
                        + "'");
    }

    private static long scalar(String sql) throws Exception {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            assertThat(rs.next()).isTrue();
            return rs.getLong(1);
        }
    }
}
