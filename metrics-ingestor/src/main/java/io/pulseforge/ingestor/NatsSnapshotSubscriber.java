package io.pulseforge.ingestor;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.pulseforge.common.protocol.HistogramSnapshot;
import io.pulseforge.common.protocol.NatsSubjects;
import io.pulseforge.common.serde.JsonCodec;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Receives snapshots over NATS and hands them to the shared buffer.
 *
 * <p>Enabled by default. The gRPC listener can run alongside it, which is what makes migrating a
 * fleet between transports possible one worker at a time.
 */
@Component
@ConditionalOnProperty(
        prefix = "pulseforge.ingestor",
        name = "nats-enabled",
        havingValue = "true",
        matchIfMissing = true)
public class NatsSnapshotSubscriber {

    private static final Logger log = LoggerFactory.getLogger(NatsSnapshotSubscriber.class);

    private final Connection nats;
    private final SnapshotBuffer buffer;

    private Dispatcher dispatcher;

    public NatsSnapshotSubscriber(Connection nats, SnapshotBuffer buffer) {
        this.nats = nats;
        this.buffer = buffer;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void subscribe() {
        dispatcher = nats.createDispatcher(message -> {});
        dispatcher.subscribe(NatsSubjects.METRICS_SNAPSHOTS, this::onSnapshot);
        log.info("Ingesting snapshots from NATS subject {}", NatsSubjects.METRICS_SNAPSHOTS);
    }

    private void onSnapshot(io.nats.client.Message message) {
        try {
            buffer.offer(JsonCodec.decode(message.getData(), HistogramSnapshot.class));
        } catch (RuntimeException e) {
            log.error("Discarding unreadable snapshot", e);
        }
    }

    @PreDestroy
    public void shutdown() {
        if (dispatcher != null) {
            nats.closeDispatcher(dispatcher);
        }
    }
}
