package io.pulseforge.target.config;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Tunables for the artificial latency and failure behaviour of the target endpoints.
 *
 * <p>Every number the target reacts to lives here rather than in the controllers, so a run can be
 * re-shaped from {@code application.yml} or an environment variable without a rebuild.
 */
@Validated
@ConfigurationProperties(prefix = "pulseforge.target")
public record FaultInjectionProperties(Fast fast, Slow slow, Flaky flaky) {

    public FaultInjectionProperties {
        fast = fast == null ? new Fast(Duration.ZERO, Duration.ofMillis(2)) : fast;
        slow = slow == null ? new Slow(Duration.ofMillis(120), Duration.ofMillis(60)) : slow;
        flaky = flaky == null ? new Flaky(Duration.ofMillis(20), 10.0d) : flaky;
    }

    /** Near-instant endpoint; the baseline every other measurement is read against. */
    public record Fast(Duration baseDelay, Duration jitter) {}

    /** Deliberately slow endpoint used to make percentile tails visible. */
    public record Slow(Duration baseDelay, Duration jitter) {}

    /** Endpoint that fails a configurable share of requests with HTTP 503. */
    public record Flaky(
            Duration baseDelay,
            @DecimalMin("0.0") @DecimalMax("100.0") double errorRatePercent) {}
}
