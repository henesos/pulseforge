package io.pulseforge.common.nats;

import io.nats.client.Connection;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/** Reports the live status of the NATS connection. Registered as the {@code nats} health entry. */
public class NatsHealthIndicator implements HealthIndicator {

    private final Connection connection;

    public NatsHealthIndicator(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Health health() {
        Connection.Status status = connection.getStatus();
        Health.Builder builder = status == Connection.Status.CONNECTED ? Health.up() : Health.down();
        return builder.withDetail("status", status.name())
                .withDetail("servers", connection.getServers())
                .build();
    }
}
