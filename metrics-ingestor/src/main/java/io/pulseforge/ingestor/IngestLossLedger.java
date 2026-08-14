package io.pulseforge.ingestor;

import io.pulseforge.common.protocol.HistogramSnapshot;
import jakarta.annotation.PostConstruct;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Records, per run, the measurements the ingestor received and never stored.
 *
 * <p>Without this the loss is real and invisible. A worker that drops a sample reports it in the
 * snapshot's {@code dropped_samples}, so the run says so; a snapshot the <em>ingestor</em> drops
 * takes its own admission with it, and the run reports a full set of confident percentiles computed
 * over a population it never saw. That is the exact failure the whole backpressure design exists to
 * avoid, reintroduced one hop later.
 *
 * <p>The ledger owns its table rather than sharing the init script. {@code 01-schema.sql} runs only
 * when a ClickHouse volume is first created, so a table added to it never appears in a deployment
 * that already has data — and a loss table that silently does not exist is worse than no loss table
 * at all. Exactly one writer, exactly one definition, created at startup.
 *
 * <p>Only losses that can be attributed to a run are recorded. A message too malformed to parse has
 * no run id to attribute it to and stays a log line; that limit is documented rather than papered
 * over with a guess.
 */
@Component
public class IngestLossLedger {

    private static final Logger log = LoggerFactory.getLogger(IngestLossLedger.class);

    private static final String DDL =
            """
            CREATE TABLE IF NOT EXISTS pulseforge.ingest_losses
            (
                run_id        UUID,
                step_name     LowCardinality(String),
                -- Why the measurements were lost, so an operator knows what to change: a full queue
                -- means the ingestor cannot keep up, a failed write means ClickHouse refused it.
                reason        LowCardinality(String),
                -- Measurements that reached this process and were never stored.
                lost_requests UInt64,
                -- Counters the lost snapshots were carrying. They describe the run, so losing them
                -- silently would make the report understate its own incompleteness.
                lost_dropped  UInt64,
                lost_skipped  UInt64,
                recorded_at   DateTime64(3, 'UTC') DEFAULT now64(3)
            )
            ENGINE = SummingMergeTree((lost_requests, lost_dropped, lost_skipped))
            ORDER BY (run_id, step_name, reason)
            TTL toDateTime(recorded_at) + INTERVAL 30 DAY
            """;

    private static final String INSERT =
            """
            INSERT INTO pulseforge.ingest_losses
                (run_id, step_name, reason, lost_requests, lost_dropped, lost_skipped)
            VALUES (?, ?, ?, ?, ?, ?)
            """;

    /** Why a snapshot never reached storage. Stored verbatim, so keep these stable. */
    public enum Reason {
        /** The ingestor's bounded queue was full: it cannot keep up with the fleet. */
        QUEUE_FULL("queue_full"),
        /** ClickHouse refused the batch. */
        WRITE_FAILED("write_failed");

        private final String stored;

        Reason(String stored) {
            this.stored = stored;
        }

        public String stored() {
            return stored;
        }
    }

    private final DataSource dataSource;
    private final Map<Key, long[]> pending = new ConcurrentHashMap<>();

    public IngestLossLedger(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @PostConstruct
    public void createTable() {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(DDL);
            log.info("Ingest loss ledger ready");
        } catch (SQLException e) {
            // Deliberately fatal. Starting without somewhere to record loss means every drop from
            // here on is silent, which is the condition this class exists to end.
            throw new IllegalStateException("could not create the ingest loss table", e);
        }
    }

    /** Notes one snapshot that will never be stored. */
    public void record(HistogramSnapshot snapshot, Reason reason) {
        pending.merge(
                new Key(snapshot.runId(), snapshot.stepName(), reason),
                new long[] {
                    snapshot.requestCount(), snapshot.droppedSamples(), snapshot.skippedRequests()
                },
                IngestLossLedger::add);
    }

    /** Notes a whole batch, for the case where the write itself failed. */
    public void recordAll(List<HistogramSnapshot> batch, Reason reason) {
        for (HistogramSnapshot snapshot : batch) {
            record(snapshot, reason);
        }
    }

    /**
     * Writes what has accumulated, and keeps it if the write fails.
     *
     * <p>Called from the writer thread after a batch, so a loss caused by ClickHouse being
     * unreachable is retried on the next flush rather than dropped a second time — the one failure
     * that would make this class part of the problem.
     */
    public void writePending() {
        if (pending.isEmpty()) {
            return;
        }
        List<Map.Entry<Key, long[]>> taken = new ArrayList<>(pending.size());
        for (Key key : List.copyOf(pending.keySet())) {
            long[] counts = pending.remove(key);
            if (counts != null) {
                taken.add(Map.entry(key, counts));
            }
        }
        if (taken.isEmpty()) {
            return;
        }

        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(INSERT)) {
            for (Map.Entry<Key, long[]> entry : taken) {
                statement.setObject(1, entry.getKey().runId());
                statement.setString(2, entry.getKey().stepName());
                statement.setString(3, entry.getKey().reason().stored());
                statement.setLong(4, entry.getValue()[0]);
                statement.setLong(5, entry.getValue()[1]);
                statement.setLong(6, entry.getValue()[2]);
                statement.addBatch();
            }
            statement.executeBatch();
            log.warn(
                    "Recorded {} ingest loss rows; affected runs will report unstored samples",
                    taken.size());
        } catch (SQLException e) {
            for (Map.Entry<Key, long[]> entry : taken) {
                pending.merge(entry.getKey(), entry.getValue(), IngestLossLedger::add);
            }
            log.error("Could not record {} ingest loss rows, keeping them for retry", taken.size(), e);
        }
    }

    /** Rows accumulated but not yet written. */
    public int pendingRows() {
        return pending.size();
    }

    private static long[] add(long[] existing, long[] addition) {
        return new long[] {
            existing[0] + addition[0], existing[1] + addition[1], existing[2] + addition[2]
        };
    }

    private record Key(UUID runId, String stepName, Reason reason) {}
}
