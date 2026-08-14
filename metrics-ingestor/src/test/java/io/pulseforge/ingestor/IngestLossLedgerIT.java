package io.pulseforge.ingestor;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import io.pulseforge.common.protocol.HistogramSnapshot;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The ledger is what turns ingestor-side loss from an invisible fact into a number on the run
 * report, so the parts that must hold are the ones a mock cannot answer: that the table it depends
 * on actually exists after startup, that ClickHouse sums the rows the way the report assumes, and
 * that a loss recorded while ClickHouse was refusing writes is not quietly lost a second time.
 */
@Testcontainers
class IngestLossLedgerIT {

    @Container
    static final ClickHouseContainer CLICKHOUSE =
            new ClickHouseContainer("clickhouse/clickhouse-server:24.8-alpine")
                    .withUsername("pulseforge")
                    .withPassword("pulseforge")
                    .withDatabaseName("pulseforge")
                    .withEnv("CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT", "1");

    private static HikariDataSource dataSource;

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
        }
    }

    @AfterAll
    static void tearDown() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    @Test
    @DisplayName("the ledger creates the table it needs, on a database that never had one")
    void createsItsOwnTable() throws Exception {
        new IngestLossLedger(dataSource).createTable();

        assertThat(scalar("SELECT count() FROM system.tables WHERE database = 'pulseforge' "
                        + "AND name = 'ingest_losses'"))
                .as("the compose init script only runs on a virgin volume; this must not depend on it")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("creating the table twice is not an error")
    void createIsIdempotent() {
        IngestLossLedger ledger = new IngestLossLedger(dataSource);

        ledger.createTable();
        ledger.createTable();
    }

    @Test
    @DisplayName("losses accumulate per run and step, and add up to what was thrown away")
    void lossesSumPerRunAndStep() throws Exception {
        IngestLossLedger ledger = new IngestLossLedger(dataSource);
        ledger.createTable();
        UUID runId = UUID.randomUUID();

        ledger.record(snapshot(runId, "list-products", 400), IngestLossLedger.Reason.QUEUE_FULL);
        ledger.record(snapshot(runId, "list-products", 350), IngestLossLedger.Reason.QUEUE_FULL);
        ledger.record(snapshot(runId, "checkout", 120), IngestLossLedger.Reason.QUEUE_FULL);
        ledger.writePending();

        assertThat(lost(runId, "lost_requests"))
                .as("870 measurements reached this process and none of them were stored")
                .isEqualTo(870);
        assertThat(lost(runId, "lost_dropped"))
                .as("three snapshots carrying 3 worker-side drops each")
                .isEqualTo(9);
        assertThat(lost(runId, "lost_skipped")).isEqualTo(21);
        assertThat(ledger.pendingRows()).isZero();
    }

    @Test
    @DisplayName("a run that lost nothing has no rows, and reads as zero rather than as missing")
    void aCleanRunReadsAsZero() throws Exception {
        new IngestLossLedger(dataSource).createTable();

        assertThat(lost(UUID.randomUUID(), "lost_requests")).isZero();
    }

    @Test
    @DisplayName("a loss recorded while ClickHouse refuses writes is kept, not lost twice")
    void unwritableLossesAreRetained() {
        new IngestLossLedger(dataSource).createTable();
        IngestLossLedger ledger = new IngestLossLedger(refusingInserts());
        UUID runId = UUID.randomUUID();

        ledger.record(snapshot(runId, "list-products", 400), IngestLossLedger.Reason.WRITE_FAILED);
        ledger.writePending();

        assertThat(ledger.pendingRows())
                .as("dropping the record of a drop is the one failure that makes this class useless")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("a retained loss is written once the store accepts again")
    void retainedLossesAreWrittenOnRetry() throws Exception {
        new IngestLossLedger(dataSource).createTable();
        UUID runId = UUID.randomUUID();

        Failable store = new Failable();
        IngestLossLedger ledger = new IngestLossLedger(store.dataSource());
        store.failing = true;
        ledger.record(snapshot(runId, "list-products", 400), IngestLossLedger.Reason.WRITE_FAILED);
        ledger.writePending();

        store.failing = false;
        ledger.writePending();

        assertThat(ledger.pendingRows()).isZero();
        assertThat(lost(runId, "lost_requests")).isEqualTo(400);
    }

    /** A DataSource whose insert can be turned off and on again. */
    private static final class Failable {
        volatile boolean failing;

        DataSource dataSource() {
            return (DataSource)
                    Proxy.newProxyInstance(
                            IngestLossLedgerIT.class.getClassLoader(),
                            new Class<?>[] {DataSource.class},
                            (proxy, method, args) -> {
                                if (!method.getName().equals("getConnection")) {
                                    return invoke(IngestLossLedgerIT.dataSource, method, args);
                                }
                                Connection real = IngestLossLedgerIT.dataSource.getConnection();
                                return Proxy.newProxyInstance(
                                        IngestLossLedgerIT.class.getClassLoader(),
                                        new Class<?>[] {Connection.class},
                                        (p, m, a) -> {
                                            if (failing
                                                    && m.getName().equals("prepareStatement")) {
                                                throw new SQLException("ClickHouse is refusing writes");
                                            }
                                            return invoke(real, m, a);
                                        });
                            });
        }
    }

    private static DataSource refusingInserts() {
        Failable failable = new Failable();
        failable.failing = true;
        return failable.dataSource();
    }

    private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args)
            throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private static HistogramSnapshot snapshot(UUID runId, String stepName, long requestCount) {
        return new HistogramSnapshot(
                runId,
                "worker-1",
                stepName,
                Instant.now(),
                Instant.now().plusSeconds(1),
                requestCount,
                0,
                3,
                7,
                1_000,
                2_000,
                requestCount * 1_500,
                "");
    }

    private static long lost(UUID runId, String column) throws Exception {
        return scalar(
                "SELECT sum(%s) FROM pulseforge.ingest_losses WHERE run_id = '%s'"
                        .formatted(column, runId));
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
