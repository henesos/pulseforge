package io.pulseforge.common.protocol;

import java.util.UUID;

/**
 * Redis key layout, in one place.
 *
 * <p>Redis holds only live coordination state — which workers exist, which shard each claimed,
 * which are still breathing. None of it is durable and none of it needs to be: it describes what is
 * happening right now, and a restart legitimately means "nothing is happening right now".
 */
public final class RedisKeys {

    private static final String PREFIX = "pulseforge";

    private RedisKeys() {
        throw new AssertionError("constants holder");
    }

    /** Presence of one worker in the fleet. Value is the worker id; liveness is the key's TTL. */
    public static String workerPresence(String workerId) {
        return PREFIX + ":workers:" + workerId;
    }

    /** Glob for counting the fleet. */
    public static String workerPresencePattern() {
        return PREFIX + ":workers:*";
    }

    /**
     * Monotonic counter workers INCR to claim a shard of a run.
     *
     * <p>A single atomic INCR is the whole distribution mechanism: the Nth worker to see the
     * command gets shard N-1. No leader, no negotiation, no way for two workers to claim the same
     * shard and double the offered rate.
     */
    public static String shardCounter(UUID runId) {
        return PREFIX + ":run:" + runId + ":shard-counter";
    }

    /** Per-run liveness for one worker. Expiry while the run is active means the worker died. */
    public static String runWorkerAlive(UUID runId, String workerId) {
        return PREFIX + ":run:" + runId + ":alive:" + workerId;
    }

    /** Glob for the workers currently generating load for a run. */
    public static String runWorkerAlivePattern(UUID runId) {
        return PREFIX + ":run:" + runId + ":alive:*";
    }
}
