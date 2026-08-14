package io.pulseforge.worker.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.pulseforge.common.protocol.RedisKeys;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * A shard claim is the only thing standing between a broadcast run command and two workers deciding
 * they are both shard 0 — which does not fail, it just doubles the offered rate and reports the
 * results of a test nobody asked for.
 *
 * <p>These cases cover what the worker does with the counter's answer: the index arithmetic, the
 * decision to sit a run out, and the two ways Redis can decline to answer. That the counter itself
 * hands out each value exactly once is a property of Redis, and is pinned against a real server in
 * {@link ShardClaimIT}.
 */
class ShardClaimTest {

    private static final UUID RUN_ID = UUID.fromString("3d2f9c1a-25b8-4f0e-9b3a-9a8c1f0e77aa");
    private static final String KEY = RedisKeys.shardCounter(RUN_ID);

    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> values = mock(ValueOperations.class);
    private final AtomicLong counter = new AtomicLong();

    private final ShardClaim claim = new ShardClaim(redis);

    @BeforeEach
    @SuppressWarnings("unchecked")
    void redisCountsLikeAnIncr() {
        when(redis.execute(any(RedisScript.class), anyList(), any()))
                .thenAnswer(invocation -> counter.incrementAndGet());
        when(redis.opsForValue()).thenReturn(values);
        when(values.decrement(anyString())).thenAnswer(invocation -> counter.decrementAndGet());
    }

    @Test
    @DisplayName("consecutive claimers get consecutive shards, never the same one")
    void claimsAreHandedOutInOrder() {
        assertThat(claim.claim(RUN_ID, 4)).isZero();
        assertThat(claim.claim(RUN_ID, 4)).isEqualTo(1);
        assertThat(claim.claim(RUN_ID, 4)).isEqualTo(2);
    }

    @Test
    @DisplayName("a worker that arrives after every shard is taken sits the run out")
    void latecomerSitsOut() {
        claim.claim(RUN_ID, 2);
        claim.claim(RUN_ID, 2);

        assertThat(claim.claim(RUN_ID, 2))
                .as("running the shards that were planned beats adding load nobody accounted for")
                .isEqualTo(ShardClaim.NO_SHARD);
    }

    @Test
    @DisplayName("an unreachable Redis stands the worker down rather than risking a double claim")
    void redisFailureStandsTheWorkerDown() {
        when(redis.execute(any(RedisScript.class), anyList(), any()))
                .thenThrow(new IllegalStateException("connection refused"));

        assertThat(claim.claim(RUN_ID, 4))
                .as("without the counter this worker cannot know whether shard 0 is already taken")
                .isEqualTo(ShardClaim.NO_SHARD);
    }

    @Test
    @DisplayName("a script that returns nothing is treated as no claim, not as shard -1")
    void nullReplyStandsTheWorkerDown() {
        when(redis.execute(any(RedisScript.class), anyList(), any())).thenReturn(null);

        assertThat(claim.claim(RUN_ID, 4)).isEqualTo(ShardClaim.NO_SHARD);
    }

    @Test
    @DisplayName("the claim and its expiry travel as one round trip")
    void claimAndExpiryAreOneCall() {
        claim.claim(RUN_ID, 4);

        ArgumentCaptor<RedisScript<Long>> script = captor();
        ArgumentCaptor<Object> args = ArgumentCaptor.forClass(Object.class);
        verify(redis).execute(script.capture(), eq(List.of(KEY)), args.capture());

        assertThat(script.getValue().getScriptAsString())
                .as("as two calls, a crash in between leaks one key per run, forever")
                .contains("INCR")
                .contains("EXPIRE");
        assertThat(args.getAllValues())
                .as("six hours, in the seconds EXPIRE takes")
                .containsExactly("21600");
    }

    @Test
    @DisplayName("a released shard is handed to the next worker instead of being lost")
    void releaseReturnsTheIndex() {
        assertThat(claim.claim(RUN_ID, 4)).isZero();

        claim.release(RUN_ID);

        verify(values).decrement(KEY);
        assertThat(claim.claim(RUN_ID, 4))
                .as("an index consumed by nobody leaves the run permanently one shard short")
                .isZero();
    }

    @Test
    @DisplayName("a release that cannot reach Redis does not escape into the caller's failure path")
    void releaseFailureIsContained() {
        when(values.decrement(anyString())).thenThrow(new IllegalStateException("connection refused"));

        assertThatCode(() -> claim.release(RUN_ID))
                .as("release is already the cleanup for a failed start; it must not raise a second failure")
                .doesNotThrowAnyException();
    }

    @SuppressWarnings("unchecked")
    private static ArgumentCaptor<RedisScript<Long>> captor() {
        return ArgumentCaptor.forClass(RedisScript.class);
    }
}
