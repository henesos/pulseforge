package io.pulseforge.controlplane.api;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Summary view of a stored scenario; the full YAML is a separate endpoint. */
public record ScenarioResponse(
        UUID id,
        String name,
        String target,
        int arrivalRate,
        String duration,
        String rampUp,
        int stepCount,
        List<String> assertions,
        Instant createdAt,
        Instant updatedAt) {}
