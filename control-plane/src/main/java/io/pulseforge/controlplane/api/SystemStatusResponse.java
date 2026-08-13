package io.pulseforge.controlplane.api;

import java.time.Instant;
import java.util.Map;

/**
 * Flat view of every backing service the control plane depends on.
 *
 * @param status      {@code UP} only when every dependency is up
 * @param version     build version of the control plane
 * @param checkedAt   when the snapshot was taken
 * @param liveWorkers size of the fleet right now; a run dispatched at this moment would be split
 *                    into this many shards
 * @param components  dependency name to its reported status
 */
public record SystemStatusResponse(
        String status,
        String version,
        Instant checkedAt,
        int liveWorkers,
        Map<String, String> components) {}
