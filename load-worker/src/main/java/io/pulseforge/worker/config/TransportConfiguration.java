package io.pulseforge.worker.config;

import io.nats.client.Connection;
import io.pulseforge.worker.metrics.GrpcSnapshotTransport;
import io.pulseforge.worker.metrics.NatsSnapshotTransport;
import io.pulseforge.worker.metrics.SnapshotTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Picks the snapshot transport for this worker.
 *
 * <p>Chosen at startup rather than per snapshot: the decision is a deployment property, and
 * re-evaluating it per message would make a run's data path depend on configuration timing.
 */
@Configuration
public class TransportConfiguration {

    private static final Logger log = LoggerFactory.getLogger(TransportConfiguration.class);

    @Bean
    public SnapshotTransport snapshotTransport(WorkerProperties properties, Connection nats) {
        SnapshotTransport transport =
                switch (properties.metricsTransport()) {
                    case GRPC -> new GrpcSnapshotTransport(properties.ingestorGrpcTarget());
                    case NATS -> new NatsSnapshotTransport(nats);
                };
        log.info("Shipping snapshots over {}", transport.name());
        return transport;
    }
}
