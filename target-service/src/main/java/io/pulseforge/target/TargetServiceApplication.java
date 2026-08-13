package io.pulseforge.target;

import io.pulseforge.target.config.FaultInjectionProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(FaultInjectionProperties.class)
public class TargetServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(TargetServiceApplication.class, args);
    }
}
