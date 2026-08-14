package io.pulseforge.worker.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The worker's wall clock, injected for the same reason the control plane's is: a value read from
 * {@code Instant.now()} cannot be held still, and the behaviour worth pinning here — which window a
 * measurement belongs to, how late a shard started — is defined entirely by what the clock said.
 *
 * <p>This covers wall-clock reads only. Interval scheduling and latency measurement stay on
 * {@link System#nanoTime()} and are deliberately not routed through here: a wall clock can be
 * stepped by NTP or a daylight-saving change, and a load generator whose request spacing or
 * measured latency moved when the host adjusted its clock would report a distortion it invented
 * itself. Monotonic time is the correct instrument for a duration; this is the instrument for an
 * instant.
 */
@Configuration
public class ClockConfiguration {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
