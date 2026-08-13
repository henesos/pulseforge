package io.pulseforge.worker.coordination;

import io.pulseforge.common.protocol.RedisKeys;
import java.time.Duration;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * Decides which share of a run this worker is responsible for.
 *
 * <p>Run commands are broadcast to the whole fleet, so every worker sees every command and must
 * work out its own identity within that run. A single atomic {@code INCR} does it: the Nth worker
 * to react claims shard {@code N-1}. There is no coordinator to elect and no window in which two
 * workers can hold the same shard — which would silently double the offered rate and make the
 * results describe a test nobody ran.
 *
 * <p>A worker that arrives after every shard is taken (started mid-run, or the fleet grew between
 * dispatch and delivery) claims an index beyond the expected count and sits the run out. It is
 * better to run with the shards that were planned than to add unaccounted load.
 */
@Component
public class ShardClaim {

    private static final Logger log = LoggerFactory.getLogger(ShardClaim.class);

    /** Claim keys outlive the longest plausible run, then clean themselves up. */
    private static final Duration CLAIM_TTL = Duration.ofHours(6);

    /** Returned when this worker has no shard to run. */
    public static final int NO_SHARD = -1;

    private final StringRedisTemplate redis;

    public ShardClaim(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public int claim(UUID runId, int workerCount) {
        String key = RedisKeys.shardCounter(runId);
        try {
            Long claimed = redis.opsForValue().increment(key);
            if (claimed == null) {
                return NO_SHARD;
            }
            if (claimed == 1L) {
                redis.expire(key, CLAIM_TTL);
            }

            int shardIndex = (int) (claimed - 1);
            if (shardIndex >= workerCount) {
                log.info(
                        "Run {}: no shard available (claimed index {} of {} expected); sitting out",
                        runId,
                        shardIndex,
                        workerCount);
                return NO_SHARD;
            }
            return shardIndex;
        } catch (RuntimeException e) {
            // Without Redis this worker cannot know whether another has already taken shard 0.
            // Generating load anyway risks doubling the rate, so it stands down and the control
            // plane reports the run as DEGRADED for the missing shard.
            log.error("Run {}: could not claim a shard, standing down", runId, e);
            return NO_SHARD;
        }
    }
}
