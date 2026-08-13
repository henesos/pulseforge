package io.pulseforge.common.protocol;

import io.pulseforge.common.domain.Scenario;
import java.time.Instant;
import java.util.UUID;

/**
 * Instruction to begin generating load, broadcast to the whole worker fleet.
 *
 * <p>Two fields carry the distribution logic. {@code workerCount} and {@code shardIndex} are not
 * set per worker — the command is a fan-out, and each worker claims a shard on arrival. And
 * {@code startAt} is an absolute wall-clock instant rather than "now": workers receive the message
 * microseconds apart, and a run whose shards begin at different times has a ramp-up curve nobody
 * asked for.
 *
 * @param runId       identity the results will be filed under
 * @param scenario    fully parsed and validated; workers never parse YAML
 * @param startAt     when every worker should issue its first request
 * @param workerCount how many shards the arrival rate is split into
 */
public record StartRunCommand(UUID runId, Scenario scenario, Instant startAt, int workerCount) {

    public StartRunCommand {
        if (runId == null) {
            throw new IllegalArgumentException("runId is required");
        }
        if (scenario == null) {
            throw new IllegalArgumentException("scenario is required");
        }
        if (startAt == null) {
            throw new IllegalArgumentException("startAt is required");
        }
        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must be positive, was " + workerCount);
        }
    }

    /**
     * The share of the global arrival rate this worker is responsible for.
     *
     * <p>Integer division would lose up to {@code workerCount - 1} requests per second, so the
     * remainder is handed to the lowest shard indices — the fleet total always equals the requested
     * rate.
     */
    public double rateForShard(int shardIndex) {
        int base = scenario.load().arrivalRate() / workerCount;
        int remainder = scenario.load().arrivalRate() % workerCount;
        return base + (shardIndex < remainder ? 1 : 0);
    }
}
