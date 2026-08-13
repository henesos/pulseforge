package io.pulseforge.worker.coordination;

import io.pulseforge.common.protocol.RedisKeys;
import io.pulseforge.worker.config.WorkerProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Announces that this worker exists and is healthy.
 *
 * <p>Presence is a key with a TTL that gets refreshed, not a registration that gets deleted on
 * shutdown. A worker that is SIGKILLed, loses its network, or wedges in a GC pause never gets to
 * deregister itself — and those are exactly the failures worth detecting. Expiry is the signal.
 *
 * <p>The TTL is a multiple of the heartbeat interval so a single missed beat, which a GC pause can
 * easily cause, is not mistaken for a death.
 */
@Component
public class WorkerPresence {

    private static final Logger log = LoggerFactory.getLogger(WorkerPresence.class);

    /** Tolerate two missed heartbeats before a worker is considered gone. */
    private static final int TTL_MULTIPLIER = 3;

    private final StringRedisTemplate redis;
    private final WorkerProperties properties;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(
                    runnable -> {
                        Thread thread = new Thread(runnable, "worker-presence");
                        thread.setDaemon(true);
                        return thread;
                    });

    public WorkerPresence(StringRedisTemplate redis, WorkerProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    /**
     * Scheduled programmatically rather than with {@code @Scheduled(fixedRateString = ...)}: that
     * annotation accepts only milliseconds or ISO-8601, not the {@code 2s} form the rest of the
     * configuration uses, and would fail at startup. Here the typed Duration is the single source
     * of truth.
     */
    @PostConstruct
    public void startHeartbeat() {
        long millis = properties.heartbeatInterval().toMillis();
        scheduler.scheduleAtFixedRate(this::heartbeat, 0, millis, TimeUnit.MILLISECONDS);
        log.info(
                "Worker '{}' announcing presence every {} (TTL {})",
                properties.id(),
                properties.heartbeatInterval(),
                presenceTtl());
    }

    void heartbeat() {
        try {
            redis.opsForValue()
                    .set(
                            RedisKeys.workerPresence(properties.id()),
                            properties.id(),
                            presenceTtl());
        } catch (RuntimeException e) {
            // Losing Redis must not stop load generation; the control plane will see the gap.
            log.warn("Failed to refresh presence for worker {}: {}", properties.id(), e.getMessage());
        }
    }

    public Duration presenceTtl() {
        return properties.heartbeatInterval().multipliedBy(TTL_MULTIPLIER);
    }

    @PreDestroy
    public void deregister() {
        scheduler.shutdownNow();
        // Best-effort tidy-up for a graceful stop. Correctness never depends on this running.
        try {
            redis.delete(RedisKeys.workerPresence(properties.id()));
        } catch (RuntimeException e) {
            log.debug("Presence key for {} will expire on its own", properties.id());
        }
    }
}
