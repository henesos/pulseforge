package io.pulseforge.controlplane.api;

import io.pulseforge.common.domain.RunStatus;
import java.time.Instant;
import java.util.UUID;

/** Lifecycle view of a run, without the measurements. */
public record RunResponse(
        UUID id,
        UUID scenarioId,
        RunStatus status,
        String statusReason,
        int arrivalRate,
        int durationSeconds,
        int rampUpSeconds,
        int expectedWorkers,
        int finishedWorkers,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt) {}
