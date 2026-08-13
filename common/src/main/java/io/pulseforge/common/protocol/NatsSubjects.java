package io.pulseforge.common.protocol;

/**
 * Every NATS subject used by the system, in one place.
 *
 * <p>Subjects are a contract between independently deployed processes; scattering the literals
 * across modules is how control plane and workers silently stop talking to each other.
 */
public final class NatsSubjects {

    /** Fan-out of run commands. Every worker receives every command and self-selects its share. */
    public static final String RUN_COMMANDS = "pulseforge.run.commands";

    /** Abort signal for an in-flight run. */
    public static final String RUN_CONTROL = "pulseforge.run.control";

    /** Histogram snapshots emitted by workers, consumed by the ingestor. */
    public static final String METRICS_SNAPSHOTS = "pulseforge.metrics.snapshots";

    /** Per-run completion notice a worker sends once its share of the run is finished. */
    public static final String RUN_WORKER_FINISHED = "pulseforge.run.worker-finished";

    private NatsSubjects() {
        throw new AssertionError("constants holder");
    }
}
