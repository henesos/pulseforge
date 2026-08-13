package io.pulseforge.controlplane.health;

import io.pulseforge.controlplane.config.ClickHouseProperties;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriUtils;

/**
 * Verifies the analytical store is reachable and the expected schema exists.
 *
 * <p>A reachable ClickHouse with no tables is not a healthy state for this system: the run would
 * start, generate load, and only fail when the first batch is written.
 */
@Component("clickhouse")
public class ClickHouseHealthIndicator implements HealthIndicator {

    private static final String SCHEMA_PROBE =
            "SELECT count() FROM system.tables WHERE database = {db:String}";

    private final RestClient restClient;
    private final ClickHouseProperties properties;
    private final URI probeUri;

    public ClickHouseHealthIndicator(
            RestClient.Builder restClientBuilder, ClickHouseProperties properties) {
        this.properties = properties;
        this.restClient =
                restClientBuilder
                        .baseUrl(properties.httpUrl())
                        .defaultHeader("X-ClickHouse-User", properties.username())
                        .defaultHeader("X-ClickHouse-Key", properties.password())
                        .build();
        // Built once, as an already-encoded URI: ClickHouse's own `{db:String}` bind syntax
        // collides with Spring's URI template expansion, which would otherwise try to resolve
        // `db` as a path variable.
        this.probeUri =
                UriComponentsBuilder.fromUriString(properties.httpUrl())
                        .queryParam("query", UriUtils.encode(SCHEMA_PROBE, StandardCharsets.UTF_8))
                        .queryParam(
                                "param_db",
                                UriUtils.encode(properties.database(), StandardCharsets.UTF_8))
                        .build(true)
                        .toUri();
    }

    @Override
    public Health health() {
        try {
            String response = restClient.get().uri(probeUri).retrieve().body(String.class);
            int tableCount = Integer.parseInt(response == null ? "0" : response.trim());
            Health.Builder builder = tableCount > 0 ? Health.up() : Health.down();
            return builder.withDetail("database", properties.database())
                    .withDetail("tables", tableCount)
                    .build();
        } catch (RuntimeException e) {
            return Health.down(e).withDetail("url", properties.httpUrl()).build();
        }
    }
}
