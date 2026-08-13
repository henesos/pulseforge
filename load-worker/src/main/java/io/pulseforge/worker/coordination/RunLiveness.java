package io.pulseforge.worker.coordination;

import io.pulseforge.common.protocol.RedisKeys;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Per-run liveness: proof that this worker is still generating load for a specific run.
 *
 * <p>Fleet-level presence is not enough. A worker can be perfectly healthy as a process while its
 * generator thread has died, and the control plane needs to distinguish "the run is progressing on
 * N shards" from "N workers exist". The key is written when a shard starts, refreshed while it
 * runs, and deliberately left to expire when it stops — the control plane reads its absence
 * together with the completion message to tell a finished shard from a lost one.
 */
@Component
public class RunLiveness {

    private static final Logger log = LoggerFactory.getLogger(RunLiveness.class);

    private static final int TTL_MULTIPLIER = 3;

    private final StringRedisTemplate redis;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(
                    runnable -> {
                        Thread thread = new Thread(runnable, "run-liveness");
                        thread.setDaemon(true);
                        return thread;
                    });
    private final Map<UUID, ScheduledFuture<?>> beacons = new ConcurrentHashMap<>();

    public RunLiveness(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /** Starts refreshing liveness for a run until {@link #stop} is called. */
    public void start(UUID runId, String workerId, Duration interval) {
        Duration ttl = interval.multipliedBy(TTL_MULTIPLIER);
        String key = RedisKeys.runWorkerAlive(runId, workerId);

        Runnable beat =
                () -> {
                    try {
                        redis.opsForValue().set(key, workerId, ttl);
                    } catch (RuntimeException e) {
                        log.warn("Run {}: liveness refresh failed: {}", runId, e.getMessage());
                    }
                };

        beat.run();
        ScheduledFuture<?> future =
                scheduler.scheduleAtFixedRate(
                        beat, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
        beacons.put(runId, future);
    }

    /**
     * Stops refreshing and removes the key.
     *
     * <p>Removing it on a clean finish is what makes expiry meaningful: a key that is still there
     * but stale can only mean the worker stopped refreshing without finishing.
     */
    public void stop(UUID runId, String workerId) {
        ScheduledFuture<?> future = beacons.remove(runId);
        if (future != null) {
            future.cancel(false);
        }
        try {
            redis.delete(RedisKeys.runWorkerAlive(runId, workerId));
        } catch (RuntimeException e) {
            log.debug("Run {}: liveness key will expire on its own", runId);
        }
    }
}
