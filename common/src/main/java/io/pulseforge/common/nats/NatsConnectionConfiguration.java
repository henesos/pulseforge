package io.pulseforge.common.nats;

import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.Nats;
import io.nats.client.Options;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Shared NATS connection, imported by each service rather than duplicated three times.
 *
 * <p>Two deliberate choices: an unlimited reconnect budget, because a process that gives up on the
 * bus after a transient outage cannot rejoin a running test; and
 * {@link Nats#connectReconnectOnConnect}, so a bus that is not up yet at boot causes retries
 * instead of a failed start.
 */
@Configuration
@EnableConfigurationProperties(NatsProperties.class)
public class NatsConnectionConfiguration {

    private static final Logger log = LoggerFactory.getLogger(NatsConnectionConfiguration.class);

    @Bean(destroyMethod = "close")
    public Connection natsConnection(NatsProperties properties)
            throws IOException, InterruptedException {
        Options options =
                new Options.Builder()
                        .server(properties.url())
                        .connectionTimeout(properties.connectionTimeout())
                        .reconnectWait(properties.reconnectWait())
                        .maxReconnects(-1)
                        .connectionListener((connection, event) -> log.info("NATS {}", event))
                        .errorListener(
                                new ErrorListener() {
                                    @Override
                                    public void errorOccurred(Connection conn, String error) {
                                        log.error("NATS error: {}", error);
                                    }
                                })
                        .build();
        Connection connection = Nats.connectReconnectOnConnect(options);
        log.info("NATS client initialised for {}", properties.url());
        return connection;
    }

    @Bean
    public NatsHealthIndicator natsHealthIndicator(Connection connection) {
        return new NatsHealthIndicator(connection);
    }
}
