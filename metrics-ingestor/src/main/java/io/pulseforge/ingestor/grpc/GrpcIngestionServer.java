package io.pulseforge.ingestor.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.pulseforge.ingestor.SnapshotBuffer;
import io.pulseforge.ingestor.config.IngestorProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Hosts the gRPC ingestion endpoint.
 *
 * <p>Built directly on {@link ServerBuilder} rather than through a Spring Boot gRPC starter. The
 * server has exactly one service and no cross-cutting concerns to wire, so a third-party starter
 * would add a dependency and a layer of configuration magic to save about ten lines.
 */
@Component
@ConditionalOnProperty(
        prefix = "pulseforge.ingestor",
        name = "grpc-enabled",
        havingValue = "true",
        matchIfMissing = true)
public class GrpcIngestionServer {

    private static final Logger log = LoggerFactory.getLogger(GrpcIngestionServer.class);

    private final SnapshotBuffer buffer;
    private final IngestorProperties properties;

    private Server server;

    public GrpcIngestionServer(SnapshotBuffer buffer, IngestorProperties properties) {
        this.buffer = buffer;
        this.properties = properties;
    }

    @PostConstruct
    public void start() throws IOException {
        server =
                ServerBuilder.forPort(properties.grpcPort())
                        .addService(new MetricsIngestionService(buffer))
                        // Snapshots are small; the default 4MB frame limit is generous already.
                        .maxInboundMessageSize(1024 * 1024)
                        .build()
                        .start();
        log.info("gRPC ingestion listening on port {}", properties.grpcPort());
    }

    @PreDestroy
    public void stop() {
        if (server == null) {
            return;
        }
        // Graceful first: in-flight streams carry the final snapshots of a run, and dropping them
        // would turn a clean shutdown into missing measurements.
        server.shutdown();
        try {
            if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("gRPC server did not stop gracefully; forcing shutdown");
                server.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            server.shutdownNow();
        }
    }
}
