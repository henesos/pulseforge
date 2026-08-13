package io.pulseforge.controlplane.service;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.pulseforge.common.protocol.NatsSubjects;
import io.pulseforge.common.protocol.WorkerFinished;
import io.pulseforge.common.serde.JsonCodec;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/** Closes runs as their workers report in. */
@Component
public class WorkerFinishedListener {

    private static final Logger log = LoggerFactory.getLogger(WorkerFinishedListener.class);

    private final Connection nats;
    private final RunService runService;

    private Dispatcher dispatcher;

    public WorkerFinishedListener(Connection nats, RunService runService) {
        this.nats = nats;
        this.runService = runService;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void subscribe() {
        dispatcher = nats.createDispatcher(message -> {});
        dispatcher.subscribe(NatsSubjects.RUN_WORKER_FINISHED, this::onWorkerFinished);
        log.info("Listening for worker completions on {}", NatsSubjects.RUN_WORKER_FINISHED);
    }

    private void onWorkerFinished(io.nats.client.Message message) {
        try {
            WorkerFinished finished = JsonCodec.decode(message.getData(), WorkerFinished.class);
            if (finished.skippedRequests() > 0 || finished.droppedSamples() > 0) {
                log.warn(
                        "Run {} worker {} reported {} skipped requests and {} dropped samples",
                        finished.runId(),
                        finished.workerId(),
                        finished.skippedRequests(),
                        finished.droppedSamples());
            }
            runService.recordWorkerFinished(finished.runId(), finished.workerId());
        } catch (RuntimeException e) {
            log.error("Failed to process worker completion", e);
        }
    }

    @PreDestroy
    public void shutdown() {
        if (dispatcher != null) {
            nats.closeDispatcher(dispatcher);
        }
    }
}
