package io.pulseforge.controlplane.service;

import io.pulseforge.common.protocol.RedisKeys;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(WorkerRegistry.class);

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
        try {
            Set<String> keys = redis.keys(RedisKeys.workerPresencePattern());
            return keys == null ? 0 : keys.size();
        } catch (RuntimeException e) {
            log.error("Could not read the worker fleet from Redis", e);
            return 0;
        }
    }

    /** Workers currently generating load for a run, by their liveness keys. */
    public int liveShardCount(UUID runId) {
        try {
            Set<String> keys = redis.keys(RedisKeys.runWorkerAlivePattern(runId));
            return keys == null ? 0 : keys.size();
        } catch (RuntimeException e) {
            log.error("Could not read liveness for run {}", runId, e);
            return 0;
        }
    }
}
