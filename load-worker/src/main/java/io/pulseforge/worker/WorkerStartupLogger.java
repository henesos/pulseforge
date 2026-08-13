package io.pulseforge.worker;

import io.pulseforge.worker.config.WorkerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Announces the worker's identity once at startup.
 *
 * <p>Workers are scaled with {@code --scale}, so the only way to tell them apart in aggregated
 * container logs is the id they claim here. Phase 2 replaces this with an actual subscription to
 * the run-command subject.
 */
@Component
public class WorkerStartupLogger {

    private static final Logger log = LoggerFactory.getLogger(WorkerStartupLogger.class);

    private final WorkerProperties properties;

    public WorkerStartupLogger(WorkerProperties properties) {
        this.properties = properties;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        log.info(
                "Load worker '{}' ready (snapshot every {}, metric queue capacity {})",
                properties.id(),
                properties.snapshotInterval(),
                properties.metricQueueCapacity());
    }
}
