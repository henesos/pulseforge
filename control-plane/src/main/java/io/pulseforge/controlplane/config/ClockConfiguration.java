package io.pulseforge.controlplane.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Time is injected rather than read from {@code Instant.now()} so run scheduling and assertion
 * evaluation stay deterministic under test.
 */
@Configuration
public class ClockConfiguration {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
