package io.pulseforge.ingestor;

import io.pulseforge.common.nats.NatsConnectionConfiguration;
import io.pulseforge.ingestor.config.IngestorProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(NatsConnectionConfiguration.class)
@EnableConfigurationProperties(IngestorProperties.class)
public class MetricsIngestorApplication {

    public static void main(String[] args) {
        SpringApplication.run(MetricsIngestorApplication.class, args);
    }
}
