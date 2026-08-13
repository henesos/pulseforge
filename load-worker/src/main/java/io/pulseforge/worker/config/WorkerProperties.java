package io.pulseforge.worker.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Everything the load generator is allowed to tune.
 *
 * @param id                stable identity of this worker; defaults to the container hostname
 * @param heartbeatInterval how often the worker announces liveness in Redis
 * @param snapshotInterval  how often the locally aggregated histogram is shipped; the single most
 *                          important knob in the system, since per-request metric messages would
 *                          saturate the bus long before the target saturates
 * @param metricQueueCapacity bounded outbound queue; when full, samples are dropped and counted
 *                            rather than allowed to slow the generator down
 * @param maxConcurrentRequests ceiling on in-flight requests, so a stalled target cannot exhaust
 *                              the worker's memory with pending exchanges
 */
@ConfigurationProperties(prefix = "pulseforge.worker")
public record WorkerProperties(
        String id,
        Duration heartbeatInterval,
        Duration snapshotInterval,
        int metricQueueCapacity,
        int maxConcurrentRequests) {

    public WorkerProperties {
        id = (id == null || id.isBlank()) ? defaultWorkerId() : id;
        heartbeatInterval = heartbeatInterval == null ? Duration.ofSeconds(2) : heartbeatInterval;
        snapshotInterval = snapshotInterval == null ? Duration.ofSeconds(1) : snapshotInterval;
        metricQueueCapacity = metricQueueCapacity <= 0 ? 1024 : metricQueueCapacity;
        maxConcurrentRequests = maxConcurrentRequests <= 0 ? 10_000 : maxConcurrentRequests;
    }

    private static String defaultWorkerId() {
        String hostname = System.getenv("HOSTNAME");
        return (hostname == null || hostname.isBlank())
                ? "worker-" + java.util.UUID.randomUUID().toString().substring(0, 8)
                : hostname;
    }
}
