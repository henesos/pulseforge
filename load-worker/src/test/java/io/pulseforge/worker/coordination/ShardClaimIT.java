package io.pulseforge.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;

import io.pulseforge.common.protocol.RedisKeys;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Verifies the claim against a real Redis, because the property that matters is a property of the
 * server: an INCR is atomic, so the Nth worker to react gets shard N-1 and no two workers can hold
 * the same index. Simulated in a mock, that guarantee would be the test's own invention.
 *
 * <p>The same goes for the Lua script's {@code if claimed == 1} guard. Against a stub it is a
 * branch nobody executes; here it is the difference between a key that expires six hours after the
 * run started and one whose lifetime is pushed forward by every worker that joins.
 */
@Testcontainers
class ShardClaimIT {

    private static final Duration CLAIM_TTL = Duration.ofHours(6);

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    private static LettuceConnectionFactory connections;
    private static StringRedisTemplate redis;
    private static ShardClaim claim;

    @BeforeAll
    static void connect() {
        connections =
                new LettuceConnectionFactory(
                        new RedisStandaloneConfiguration(
                                REDIS.getHost(), REDIS.getMappedPort(6379)));
        connections.afterPropertiesSet();
        connections.start();
        redis = new StringRedisTemplate(connections);
        claim = new ShardClaim(redis);
    }

    @AfterAll
    static void disconnect() {
        connections.destroy();
    }

    @Test
    @DisplayName("workers reacting to the same broadcast never land on the same shard")
    void concurrentClaimsAreUnique() throws Exception {
        UUID runId = UUID.randomUUID();
        int workers = 8;
        // A barrier rather than staggered starts: the race this guards against only exists when the
        // broadcast reaches the whole fleet at once, which is exactly how a run command is sent.
        CyclicBarrier startTogether = new CyclicBarrier(workers);

        List<Integer> claimed;
        try (ExecutorService fleet = Executors.newFixedThreadPool(workers)) {
            List<Callable<Integer>> tasks =
                    IntStream.range(0, workers)
                            .<Callable<Integer>>mapToObj(
                                    i ->
                                            () -> {
                                                startTogether.await(10, TimeUnit.SECONDS);
                                                return claim.claim(runId, workers);
                                            })
                            .toList();
            List<Future<Integer>> results = fleet.invokeAll(tasks);
            claimed = results.stream().map(ShardClaimIT::get).toList();
        }

        assertThat(claimed)
                .as("two workers on one shard would double the offered rate with nothing reporting it")
                .containsExactlyInAnyOrderElementsOf(IntStream.range(0, workers).boxed().toList());
    }

    @Test
    @DisplayName("the claim key gets a lifetime, and later claimers do not extend it")
    void expiryIsSetOnceOnTheFirstClaim() {
        UUID runId = UUID.randomUUID();
        String key = RedisKeys.shardCounter(runId);

        claim.claim(runId, 4);

        assertThat(redis.getExpire(key, TimeUnit.SECONDS))
                .as("without an expiry the counter is one leaked key per run, forever")
                .isBetween(CLAIM_TTL.toSeconds() - 60, CLAIM_TTL.toSeconds());

        // Stand in for six hours having nearly elapsed: if a later claim re-issued EXPIRE, the key
        // would jump back to a full lifetime here.
        redis.expire(key, Duration.ofSeconds(60));
        claim.claim(runId, 4);

        assertThat(redis.getExpire(key, TimeUnit.SECONDS)).isLessThanOrEqualTo(60);
    }

    @Test
    @DisplayName("a worker that arrives after every shard is taken sits the run out")
    void latecomerSitsOut() {
        UUID runId = UUID.randomUUID();

        assertThat(claim.claim(runId, 2)).isZero();
        assertThat(claim.claim(runId, 2)).isEqualTo(1);
        assertThat(claim.claim(runId, 2)).isEqualTo(ShardClaim.NO_SHARD);
    }

    @Test
    @DisplayName("a released shard goes back into circulation")
    void releasedShardIsReclaimed() {
        UUID runId = UUID.randomUUID();

        assertThat(claim.claim(runId, 2)).isZero();
        claim.release(runId);

        assertThat(claim.claim(runId, 2))
                .as("a worker that claimed and then failed to start must not leave the run short")
                .isZero();
    }

    private static int get(Future<Integer> future) {
        try {
            return future.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
