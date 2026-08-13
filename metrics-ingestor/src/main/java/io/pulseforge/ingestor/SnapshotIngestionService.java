package io.pulseforge.ingestor;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.pulseforge.common.protocol.HistogramSnapshot;
import io.pulseforge.common.protocol.NatsSubjects;
import io.pulseforge.common.serde.JsonCodec;
import io.pulseforge.ingestor.config.IngestorProperties;
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
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

/**
 * Consumes histogram snapshots from the bus and writes them to ClickHouse in batches.
 *
 * <p>Batching is not an optimisation here, it is a requirement: ClickHouse merges parts in the
 * background and a stream of single-row inserts produces so many parts that the server spends its
 * time merging instead of answering. Snapshots are accumulated until {@code batchSize} is reached
 * or {@code flushInterval} elapses, whichever comes first — the second condition is what keeps a
 * low-rate run's results timely.
 *
 * <p>The inbound queue is bounded and drops on overflow, for the same reason the worker's is: an
 * ingestor that applied backpressure to the bus would eventually stall the workers, distorting the
 * very measurement it exists to record.
 */
@Service
public class SnapshotIngestionService {

    private static final Logger log = LoggerFactory.getLogger(SnapshotIngestionService.class);

    private final Connection nats;
    private final SnapshotWriter writer;
    private final IngestorProperties properties;
    private final BlockingQueue<HistogramSnapshot> queue;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final LongAdder received = new LongAdder();
    private final LongAdder written = new LongAdder();
    private final LongAdder dropped = new LongAdder();
    private final LongAdder failed = new LongAdder();

    private Dispatcher dispatcher;
    private Thread writerThread;

    public SnapshotIngestionService(
            Connection nats, SnapshotWriter writer, IngestorProperties properties) {
        this.nats = nats;
        this.writer = writer;
        this.properties = properties;
        this.queue = new ArrayBlockingQueue<>(properties.queueCapacity());
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        dispatcher = nats.createDispatcher(message -> {});
        dispatcher.subscribe(NatsSubjects.METRICS_SNAPSHOTS, this::onSnapshot);

        writerThread = new Thread(this::writeLoop, "clickhouse-writer");
        writerThread.start();

        log.info(
                "Ingesting {} (batch {}, flush every {})",
                NatsSubjects.METRICS_SNAPSHOTS,
                properties.batchSize(),
                properties.flushInterval());
    }

    private void onSnapshot(io.nats.client.Message message) {
        try {
            HistogramSnapshot snapshot = JsonCodec.decode(message.getData(), HistogramSnapshot.class);
            received.increment();
            if (!queue.offer(snapshot)) {
                dropped.increment();
            }
        } catch (RuntimeException e) {
            log.error("Discarding unreadable snapshot", e);
        }
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
            }
        }

        if (!batch.isEmpty()) {
            flush(batch);
        }
        log.info(
                "Writer stopped: {} received, {} written, {} dropped, {} failed",
                received.sum(),
                written.sum(),
                dropped.sum(),
                failed.sum());
    }

    private void flush(List<HistogramSnapshot> batch) {
        try {
            writer.write(batch);
            written.add(batch.size());
        } catch (Exception e) {
            // A failed batch is lost rather than retried forever: retrying indefinitely would fill
            // the queue and cause drops that look like measurement loss instead of a storage fault.
            failed.add(batch.size());
            log.error("Failed to write batch of {} snapshots", batch.size(), e);
        }
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (dispatcher != null) {
            nats.closeDispatcher(dispatcher);
        }
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
}
