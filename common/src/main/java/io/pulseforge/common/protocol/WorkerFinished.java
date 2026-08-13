package io.pulseforge.common.protocol;

import java.time.Instant;
import java.util.UUID;

/**
 * Sent once by a worker when its shard of a run has completed.
 *
 * <p>The control plane counts these to decide when a run is over. Waiting for the scheduled end
 * time alone would be wrong: the final snapshots are still in flight, and a worker that died would
 * never be distinguished from one that is merely slow.
 */
public record WorkerFinished(
        UUID runId,
        String workerId,
        int shardIndex,
        Instant finishedAt,
        long issuedRequests,
        long skippedRequests,
        long droppedSamples) {}
