package io.pulseforge.worker.metrics;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounded hand-off between the request threads and the single aggregator thread.
 *
 * <p>This is where the backpressure decision is enforced. {@link #offer} never blocks and never
 * grows without limit: on overflow the sample is discarded and counted. Blocking here would stall
 * the load generator, which silently lowers the offered rate and invalidates the entire run —
 * a far worse outcome than an incomplete sample that announces itself.
 */
public class MetricPipeline {

    private static final Logger log = LoggerFactory.getLogger(MetricPipeline.class);

    private final BlockingQueue<Sample> queue;
    private final LongAdder droppedSamples = new LongAdder();
    private volatile boolean warnedAboutDrops;

    public MetricPipeline(int capacity) {
        this.queue = new ArrayBlockingQueue<>(capacity);
    }

    /** Non-blocking by contract. Returns false when the sample was dropped. */
    public boolean offer(Sample sample) {
        if (queue.offer(sample)) {
            return true;
        }
        droppedSamples.increment();
        if (!warnedAboutDrops) {
            warnedAboutDrops = true;
            log.warn(
                    "Metric queue saturated (capacity {}); samples are being dropped and counted. "
                            + "Percentiles for this run will be reported with a dropped-sample count.",
                    queue.remainingCapacity() + queue.size());
        }
        return false;
    }

    /** Blocks briefly for the aggregator thread; returns null when the interval had no traffic. */
    public Sample poll(long timeout, TimeUnit unit) throws InterruptedException {
        return queue.poll(timeout, unit);
    }

    public int drainTo(java.util.Collection<Sample> sink, int max) {
        return queue.drainTo(sink, max);
    }

    /** Total dropped since the worker started. Snapshots report the delta over their interval. */
    public long droppedSamples() {
        return droppedSamples.sum();
    }

    public int pending() {
        return queue.size();
    }
}
