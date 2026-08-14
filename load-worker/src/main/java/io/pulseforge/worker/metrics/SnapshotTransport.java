package io.pulseforge.worker.metrics;

import io.pulseforge.common.protocol.HistogramSnapshot;

/**
 * How a worker ships its snapshots.
 *
 * <p>The aggregator loop depends on this interface rather than on NATS or gRPC directly, so the
 * measurement pipeline is genuinely transport-agnostic: swapping transports cannot change what is
 * measured or when, only how it travels.
 *
 * <p>Implementations must not block the caller. The aggregator thread is also the thread that
 * records samples into histograms; stalling it there would build a queue of unrecorded
 * measurements, which is the failure mode the whole backpressure design exists to avoid.
 */
public interface SnapshotTransport {

    /**
     * Ships one snapshot. Implementations report failure by logging, never by throwing.
     *
     * @return false if the snapshot did not leave this worker. The caller owns the counters the
     *     snapshot was carrying, so it needs to know: a lost window whose running-total deltas were
     *     treated as delivered subtracts those counts from the run's report permanently.
     */
    boolean send(HistogramSnapshot snapshot);

    /** Called once when a run ends, so a streaming transport can close its stream cleanly. */
    default void runFinished(java.util.UUID runId) {
        // Nothing to do for connectionless transports.
    }

    /** Name used in logs and health output. */
    String name();
}
