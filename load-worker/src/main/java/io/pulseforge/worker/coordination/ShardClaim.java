package io.pulseforge.worker.coordination;

import io.pulseforge.common.protocol.RedisKeys;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
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

    /**
     * Increments the run's shard counter and sets its TTL in one round trip.
     *
     * <p>As two calls, a crash between them leaves the counter with no expiry — one leaked key per
     * run, forever. Redis runs a script atomically, so either both happen or neither does.
     */
    private static final RedisScript<Long> CLAIM_SCRIPT =
            new DefaultRedisScript<>(
                    """
                    local claimed = redis.call('INCR', KEYS[1])
                    if claimed == 1 then
                      redis.call('EXPIRE', KEYS[1], ARGV[1])
                    end
                    return claimed
                    """,
                    Long.class);

    public int claim(UUID runId, int workerCount) {
        String key = RedisKeys.shardCounter(runId);
        try {
            Long claimed =
                    redis.execute(
                            CLAIM_SCRIPT,
                            List.of(key),
                            String.valueOf(CLAIM_TTL.toSeconds()));
            if (claimed == null) {
                return NO_SHARD;
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

    /**
     * Hands a claimed shard back, for a worker that claimed one and then failed to start it.
     *
     * <p>Without this the index is consumed by nobody: the run is permanently one shard short, the
     * offered rate falls below what was asked for, and the only signal is a DEGRADED verdict that
     * cannot say why.
     */
    public void release(UUID runId) {
        try {
            redis.opsForValue().decrement(RedisKeys.shardCounter(runId));
        } catch (RuntimeException e) {
            log.error("Run {}: could not release the shard claim", runId, e);
        }
    }
}
