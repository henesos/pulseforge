package io.pulseforge.ingestor;

import io.pulseforge.common.metrics.HistogramCodec;
import io.pulseforge.common.protocol.HistogramSnapshot;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Writes a batch of snapshots into ClickHouse.
 *
 * <p>Each snapshot is stored twice, on purpose:
 *
 * <ul>
 *   <li>{@code metric_snapshots} keeps the counters and the serialized histogram verbatim, so a
 *       result can always be recomputed from the original measurement.
 *   <li>{@code latency_buckets} keeps the histogram exploded into {@code (bucket, count)} rows.
 *       This is what makes a correct global percentile a plain SQL aggregation: sum the counts
 *       across every worker, then walk the cumulative distribution. Averaging per-worker
 *       percentiles would be far easier and statistically meaningless.
 * </ul>
 */
@Component
public class SnapshotWriter {

    private static final Logger log = LoggerFactory.getLogger(SnapshotWriter.class);

    private static final String INSERT_SNAPSHOT =
            """
            INSERT INTO pulseforge.metric_snapshots
                (run_id, worker_id, step_name, window_start, window_end, request_count,
                 error_count, dropped_samples, skipped_requests, min_micros, max_micros,
                 sum_micros, histogram)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String INSERT_BUCKET =
            """
            INSERT INTO pulseforge.latency_buckets
                (run_id, step_name, window_start, bucket_micros, count)
            VALUES (?, ?, ?, ?, ?)
            """;

    private final DataSource dataSource;

    public SnapshotWriter(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void write(List<HistogramSnapshot> batch) throws SQLException {
        if (batch.isEmpty()) {
            return;
        }
        try (Connection connection = dataSource.getConnection()) {
            writeSnapshots(connection, batch);
            writeBuckets(connection, batch);
        }
        log.debug("Wrote {} snapshots to ClickHouse", batch.size());
    }

    private void writeSnapshots(Connection connection, List<HistogramSnapshot> batch)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_SNAPSHOT)) {
            for (HistogramSnapshot snapshot : batch) {
                statement.setObject(1, snapshot.runId());
                statement.setString(2, snapshot.workerId());
                statement.setString(3, snapshot.stepName());
                statement.setTimestamp(4, Timestamp.from(snapshot.windowStart()));
                statement.setTimestamp(5, Timestamp.from(snapshot.windowEnd()));
                statement.setLong(6, snapshot.requestCount());
                statement.setLong(7, snapshot.errorCount());
                statement.setLong(8, snapshot.droppedSamples());
                statement.setLong(9, snapshot.skippedRequests());
                statement.setLong(10, snapshot.minMicros());
                statement.setLong(11, snapshot.maxMicros());
                statement.setLong(12, snapshot.sumMicros());
                statement.setString(13, snapshot.histogramBase64());
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    private void writeBuckets(Connection connection, List<HistogramSnapshot> batch)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT_BUCKET)) {
            int rows = 0;
            for (HistogramSnapshot snapshot : batch) {
                if (snapshot.requestCount() == 0) {
                    continue;
                }
                Histogram histogram = HistogramCodec.decode(snapshot.histogramBase64());
                UUID runId = snapshot.runId();
                Timestamp windowStart = Timestamp.from(snapshot.windowStart());

                // recordedValues() visits only non-empty buckets, so an idle step costs no rows.
                for (HistogramIterationValue value : histogram.recordedValues()) {
                    statement.setObject(1, runId);
                    statement.setString(2, snapshot.stepName());
                    statement.setTimestamp(3, windowStart);
                    statement.setLong(4, value.getValueIteratedTo());
                    statement.setLong(5, value.getCountAtValueIteratedTo());
                    statement.addBatch();
                    rows++;
                }
            }
            if (rows > 0) {
                statement.executeBatch();
            }
        }
    }
}
