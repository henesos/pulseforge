package io.pulseforge.ingestor.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Ingestion and batching behaviour.
 *
 * <p>Both transports can be enabled at once. That is not indecision — it is what lets a fleet be
 * migrated from one to the other a worker at a time instead of in a flag day.
 *
 * @param batchSize     rows accumulated before a write is forced
 * @param flushInterval maximum time a snapshot waits before being written, so a low-rate run still
 *                      produces timely results
 * @param queueCapacity bounded inbound buffer; drops rather than applying backpressure to workers
 * @param natsEnabled   consume snapshots published to the NATS subject
 * @param grpcEnabled   accept snapshots streamed over gRPC
 * @param grpcPort      port the gRPC server binds
 */
@ConfigurationProperties(prefix = "pulseforge.ingestor")
public record IngestorProperties(
        int batchSize,
        Duration flushInterval,
        int queueCapacity,
        Boolean natsEnabled,
        Boolean grpcEnabled,
        int grpcPort) {

    public IngestorProperties {
        batchSize = batchSize <= 0 ? 500 : batchSize;
        flushInterval = flushInterval == null ? Duration.ofSeconds(2) : flushInterval;
        queueCapacity = queueCapacity <= 0 ? 10_000 : queueCapacity;
        natsEnabled = natsEnabled == null || natsEnabled;
        grpcEnabled = grpcEnabled == null || grpcEnabled;
        grpcPort = grpcPort <= 0 ? 9090 : grpcPort;
    }
}
