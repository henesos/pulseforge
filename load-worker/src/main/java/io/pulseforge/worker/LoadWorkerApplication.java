package io.pulseforge.worker;

import io.pulseforge.common.nats.NatsConnectionConfiguration;
import io.pulseforge.worker.config.WorkerProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import(NatsConnectionConfiguration.class)
@EnableConfigurationProperties(WorkerProperties.class)
public class LoadWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(LoadWorkerApplication.class, args);
    }
}
