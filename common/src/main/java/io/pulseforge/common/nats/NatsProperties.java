package io.pulseforge.common.nats;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Connection settings shared by every service that talks to the message bus. */
@ConfigurationProperties(prefix = "pulseforge.nats")
public record NatsProperties(String url, Duration connectionTimeout, Duration reconnectWait) {

    public NatsProperties {
        url = (url == null || url.isBlank()) ? "nats://localhost:4222" : url;
        connectionTimeout = connectionTimeout == null ? Duration.ofSeconds(5) : connectionTimeout;
        reconnectWait = reconnectWait == null ? Duration.ofSeconds(2) : reconnectWait;
    }
}
