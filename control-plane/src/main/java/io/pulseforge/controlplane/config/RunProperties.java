package io.pulseforge.controlplane.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Timing knobs for run orchestration.
 *
 * @param dispatchLead how far in the future the fleet-wide start instant is set. Long enough for
 *                     the command to reach every worker, short enough that an operator does not
 *                     notice the delay.
 * @param settleDelay  grace period after the last worker reports, so in-flight snapshots reach
 *                     ClickHouse before results are read.
 * @param watchdogInterval how often active runs are checked for workers that stopped reporting.
 */
@ConfigurationProperties(prefix = "pulseforge.run")
public record RunProperties(
        Duration dispatchLead, Duration settleDelay, Duration watchdogInterval) {

    public RunProperties {
        dispatchLead = dispatchLead == null ? Duration.ofSeconds(2) : dispatchLead;
        settleDelay = settleDelay == null ? Duration.ofSeconds(5) : settleDelay;
        watchdogInterval = watchdogInterval == null ? Duration.ofSeconds(5) : watchdogInterval;
    }
}
