package io.pulseforge.common.domain;

/**
 * Lifecycle of a single test run.
 *
 * <p>{@link #DEGRADED} is a deliberate first-class state rather than a flag on
 * {@link #COMPLETED}: if a worker dies mid-run the produced numbers are still readable, but they
 * no longer describe the load profile that was requested. Reporting them as a clean success would
 * be silently wrong.
 */
public enum RunStatus {

    /** Accepted and persisted, not yet dispatched to workers. */
    PENDING,

    /** Dispatched; at least one worker is generating load. */
    RUNNING,

    /** Finished with the full worker fleet alive for the whole run. */
    COMPLETED,

    /** Finished, but one or more workers dropped out; results under-represent the target rate. */
    DEGRADED,

    /** Stopped on explicit operator request. */
    ABORTED,

    /** Could not be executed at all (dispatch failure, no workers, invalid scenario). */
    FAILED;

    public boolean isTerminal() {
        return this != PENDING && this != RUNNING;
    }
}
