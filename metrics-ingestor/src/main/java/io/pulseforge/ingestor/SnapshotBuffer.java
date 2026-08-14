package io.pulseforge.ingestor;

import io.pulseforge.common.protocol.HistogramSnapshot;
import io.pulseforge.ingestor.config.IngestorProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Where every snapshot lands, whichever transport carried it, and the only thing that writes to
 * ClickHouse.
 *
 * <p>Keeping the buffer transport-agnostic is what makes NATS and gRPC genuinely interchangeable:
 * batching, overflow behaviour and failure handling are defined once, so switching transport
 * cannot quietly change how data is persisted.
 *
 * <p>Batching is a requirement rather than an optimisation. ClickHouse merges parts in the
 * background, and a stream of single-row inserts produces so many parts that the server spends its
 * time merging instead of answering queries. Snapshots accumulate until {@code batchSize} is
 * reached or {@code flushInterval} elapses — the second condition is what keeps a low-rate run's
 * results timely.
 *
 * <p>The queue is bounded and drops on overflow, for the same reason the worker's is: applying
 * backpressure here would eventually stall the workers and distort the measurement this exists to
 * record.
 */
@Component
public class SnapshotBuffer {

    private static final Logger log = LoggerFactory.getLogger(SnapshotBuffer.class);

    private final SnapshotWriter writer;
    private final IngestLossLedger losses;
    private final IngestorProperties properties;
    private final BlockingQueue<HistogramSnapshot> queue;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final LongAdder received = new LongAdder();
    private final LongAdder written = new LongAdder();
    private final LongAdder dropped = new LongAdder();
    private final LongAdder failed = new LongAdder();
    private final LongAdder partiallyWritten = new LongAdder();

    private Thread writerThread;

    public SnapshotBuffer(
            SnapshotWriter writer, IngestLossLedger losses, IngestorProperties properties) {
        this.writer = writer;
        this.losses = losses;
        this.properties = properties;
        this.queue = new ArrayBlockingQueue<>(properties.queueCapacity());
    }

    @PostConstruct
    public void start() {
        writerThread = new Thread(this::writeLoop, "clickhouse-writer");
        writerThread.start();
        log.info(
                "Snapshot buffer ready (capacity {}, batch {}, flush every {})",
                properties.queueCapacity(),
                properties.batchSize(),
                properties.flushInterval());
    }

    /** Non-blocking by contract. Returns false when the snapshot was dropped. */
    public boolean offer(HistogramSnapshot snapshot) {
        received.increment();
        if (queue.offer(snapshot)) {
            return true;
        }
        dropped.increment();
        // Attributed to the run before the snapshot is let go: this object is the only place that
        // still knows how many measurements are about to disappear and which run they belonged to.
        losses.record(snapshot, IngestLossLedger.Reason.QUEUE_FULL);
        return false;
    }

    private void writeLoop() {
        List<HistogramSnapshot> batch = new ArrayList<>(properties.batchSize());
        long flushIntervalNanos = properties.flushInterval().toNanos();
        long nextFlush = System.nanoTime() + flushIntervalNanos;

        while (running.get() || !queue.isEmpty()) {
            try {
                HistogramSnapshot snapshot = queue.poll(100, TimeUnit.MILLISECONDS);
                if (snapshot != null) {
                    batch.add(snapshot);
                    queue.drainTo(batch, properties.batchSize() - batch.size());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            boolean sizeReached = batch.size() >= properties.batchSize();
            boolean intervalElapsed = System.nanoTime() >= nextFlush;
            if (!batch.isEmpty() && (sizeReached || intervalElapsed)) {
                flush(batch);
                batch.clear();
                nextFlush = System.nanoTime() + flushIntervalNanos;
            } else if (intervalElapsed) {
                // Recorded loss can outlive the traffic that caused it: a run whose final snapshots
                // were the ones dropped leaves nothing behind for them to ride along with.
                losses.writePending();
                nextFlush = System.nanoTime() + flushIntervalNanos;
            }
        }

        if (!batch.isEmpty()) {
            flush(batch);
        }
        losses.writePending();
        log.info(
                "Writer stopped: {} received, {} written, {} dropped, {} failed, {} partial",
                received.sum(),
                written.sum(),
                dropped.sum(),
                failed.sum(),
                partiallyWritten.sum());
    }

    private void flush(List<HistogramSnapshot> batch) {
        try {
            writer.write(batch);
            written.add(batch.size());
        } catch (SnapshotWriter.PartialWriteException e) {
            // Counted apart from a clean failure because the two are different problems for whoever
            // reads the run: this batch's latency distribution is stored and complete, and only its
            // counters are missing, so the run under-reports throughput rather than losing a window.
            partiallyWritten.add(batch.size());
            log.error(
                    "Wrote the buckets but not the counters for {} snapshots; "
                            + "the run will under-report requests for this window",
                    batch.size(),
                    e);
        } catch (Exception e) {
            // A failed batch is lost rather than retried forever: retrying indefinitely would fill
            // the queue and cause drops that look like measurement loss instead of a storage fault.
            // Lost, but not silently — the runs it belonged to say so.
            failed.add(batch.size());
            losses.recordAll(batch, IngestLossLedger.Reason.WRITE_FAILED);
            log.error("Failed to write batch of {} snapshots", batch.size(), e);
        }
        // After the batch either way, so a loss recorded while ClickHouse was refusing writes is
        // retried as soon as it accepts one again.
        losses.writePending();
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (writerThread != null) {
            try {
                writerThread.join(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public long receivedCount() {
        return received.sum();
    }

    public long writtenCount() {
        return written.sum();
    }

    public long droppedCount() {
        return dropped.sum();
    }

    public long failedCount() {
        return failed.sum();
    }

    /** Batches whose buckets were stored but whose counters were not. */
    public long partiallyWrittenCount() {
        return partiallyWritten.sum();
    }
}
