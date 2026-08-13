package io.pulseforge.ingestor.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Batching behaviour of the ClickHouse writer.
 *
 * <p>ClickHouse is fast at large inserts and pathologically slow at one-row inserts, so snapshots
 * are accumulated and flushed either when {@code batchSize} is reached or {@code flushInterval}
 * elapses — whichever comes first, so a low-rate run still produces timely results.
 */
@ConfigurationProperties(prefix = "pulseforge.ingestor")
public record IngestorProperties(int batchSize, Duration flushInterval, int queueCapacity) {

    public IngestorProperties {
        batchSize = batchSize <= 0 ? 500 : batchSize;
        flushInterval = flushInterval == null ? Duration.ofSeconds(2) : flushInterval;
        queueCapacity = queueCapacity <= 0 ? 10_000 : queueCapacity;
    }
}
