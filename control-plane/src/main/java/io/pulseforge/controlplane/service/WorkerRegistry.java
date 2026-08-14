package io.pulseforge.controlplane.service;

import io.pulseforge.common.protocol.RedisKeys;
import java.util.Set;
import java.util.UUID;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * The control plane's view of the fleet.
 *
 * <p>Membership is derived from live TTL keys rather than a registration table, so a worker that
 * dies without warning leaves the fleet on its own. Nothing has to notice and delete it.
 */
@Component
public class WorkerRegistry {

    private final StringRedisTemplate redis;

    public WorkerRegistry(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /**
     * How many workers are available right now.
     *
     * <p>Read once at dispatch time and then frozen into the run: if the fleet grew mid-run, the
     * arrival rate would silently change and the results would describe two different experiments
     * stitched together.
     */
    public int liveWorkerCount() {
        return count(RedisKeys.workerPresencePattern(), "the worker fleet");
    }

    /** Workers currently generating load for a run, by their liveness keys. */
    public int liveShardCount(UUID runId) {
        return count(RedisKeys.runWorkerAlivePattern(runId), "liveness for run " + runId);
    }

    private int count(String pattern, String what) {
        try {
            Set<String> keys = redis.keys(pattern);
            return keys == null ? 0 : keys.size();
        } catch (RuntimeException e) {
            // Deliberately not zero. "Redis is unreachable" and "no workers are running" are
            // different facts, and substituting the first for the second sends the operator to
            // inspect a healthy fleet — or, in the watchdog, degrades every active run.
            throw new RegistryUnavailableException("could not read " + what + " from Redis", e);
        }
    }

    /** Raised when the coordination store cannot answer, as distinct from answering zero. */
    public static class RegistryUnavailableException extends RuntimeException {
        public RegistryUnavailableException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
