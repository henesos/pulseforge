package io.pulseforge.cli;

/**
 * Process exit codes.
 *
 * <p>Distinct codes for distinct failures, because a pipeline reacts differently to each: a failed
 * assertion is a regression to investigate, a degraded run is infrastructure to fix and a reason to
 * re-run, and a usage error is neither.
 */
public enum ExitCode {

    /** Every assertion passed and the measurement was complete. */
    PASS(0),

    /** The run completed and at least one assertion failed. This is the regression signal. */
    ASSERTION_FAILED(1),

    /**
     * The run did not produce a trustworthy measurement — a worker was lost, or samples were
     * dropped. Deliberately not reported as a pass even when the assertions happen to hold.
     */
    DEGRADED(2),

    /** Bad arguments, unreachable control plane, invalid scenario. */
    ERROR(3);

    private final int code;

    ExitCode(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}
