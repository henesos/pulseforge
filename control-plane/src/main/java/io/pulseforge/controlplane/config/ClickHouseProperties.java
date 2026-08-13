package io.pulseforge.controlplane.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * ClickHouse access for the control plane.
 *
 * <p>The control plane only ever reads aggregates over HTTP; writes belong to the ingestor, which
 * uses the JDBC driver. Keeping the read path on plain HTTP avoids pulling the shaded driver into
 * a service that does not need it.
 */
@ConfigurationProperties(prefix = "pulseforge.clickhouse")
public record ClickHouseProperties(
        String httpUrl, String database, String username, String password, Duration timeout) {

    public ClickHouseProperties {
        httpUrl = (httpUrl == null || httpUrl.isBlank()) ? "http://localhost:8123" : httpUrl;
        database = (database == null || database.isBlank()) ? "pulseforge" : database;
        username = (username == null || username.isBlank()) ? "pulseforge" : username;
        password = password == null ? "" : password;
        timeout = timeout == null ? Duration.ofSeconds(5) : timeout;
    }
}
