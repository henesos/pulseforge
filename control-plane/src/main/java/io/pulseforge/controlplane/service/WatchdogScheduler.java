package io.pulseforge.controlplane.service;

import io.pulseforge.controlplane.config.RunProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.IntervalTask;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

/**
 * Drives {@link RunWatchdog} on a timer.
 *
 * <p>Separate from the watchdog itself for two reasons. The interval is a typed {@code Duration},
 * which {@code @Scheduled(fixedDelayString = ...)} cannot read in the {@code 5s} form used
 * throughout the configuration. And, more importantly, the watchdog is called here through its
 * injected proxy: scheduling it from inside its own class would invoke the raw instance and
 * silently skip {@code @Transactional}, leaving every status change uncommitted.
 */
@Configuration
public class WatchdogScheduler implements SchedulingConfigurer {

    private final RunWatchdog watchdog;
    private final RunProperties properties;

    public WatchdogScheduler(RunWatchdog watchdog, RunProperties properties) {
        this.watchdog = watchdog;
        this.properties = properties;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar registrar) {
        registrar.addFixedDelayTask(
                new IntervalTask(watchdog::checkActiveRuns, properties.watchdogInterval()));
    }
}
