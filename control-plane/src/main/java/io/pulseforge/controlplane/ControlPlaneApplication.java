package io.pulseforge.controlplane;

import io.pulseforge.common.nats.NatsConnectionConfiguration;
import io.pulseforge.controlplane.config.ClickHouseProperties;
import io.pulseforge.controlplane.config.RunProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(NatsConnectionConfiguration.class)
@EnableConfigurationProperties({ClickHouseProperties.class, RunProperties.class})
public class ControlPlaneApplication {

    public static void main(String[] args) {
        SpringApplication.run(ControlPlaneApplication.class, args);
    }
}
