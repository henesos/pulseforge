package io.pulseforge.worker.run;

import static org.assertj.core.api.Assertions.assertThat;

import io.pulseforge.common.domain.LoadProfile;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The arrival schedule is the piece of maths that decides whether the tool measures honestly, so it
 * is pinned down here rather than trusted.
 */
class ArrivalScheduleTest {

    @Test
    @DisplayName("without ramp-up, requests are evenly spaced at the arrival rate")
    void evenSpacingWithoutRampUp() {
        ArrivalSchedule schedule =
                new ArrivalSchedule(
                        new LoadProfile(Duration.ofSeconds(10), Duration.ZERO, 100), 100);

        // 100 req/s means one request every 10ms.
        assertThat(schedule.sendOffsetNanos(0)).isZero();
        assertThat(schedule.sendOffsetNanos(1)).isEqualTo(10_000_000L);
        assertThat(schedule.sendOffsetNanos(100)).isEqualTo(1_000_000_000L);
        assertThat(schedule.totalRequests()).isEqualTo(1000);
    }

    @Test
    @DisplayName("ramp-up delivers exactly half the steady-state volume over its window")
    void rampUpIsLinear() {
        // 0 -> 200 req/s over 10s. The integral of a linear ramp is a triangle: 200 * 10 / 2 = 1000.
        ArrivalSchedule schedule =
                new ArrivalSchedule(
                        new LoadProfile(Duration.ofSeconds(10), Duration.ofSeconds(10), 200), 200);

        assertThat(schedule.totalRequests()).isEqualTo(1000);

        // The last request of the ramp lands at the end of the window, not before it.
        long lastOffset = schedule.sendOffsetNanos(999);
        assertThat(lastOffset).isBetween(9_000_000_000L, 10_000_000_000L);
    }

    @Test
    @DisplayName("during ramp-up the instantaneous rate rises: later requests are closer together")
    void rampUpAccelerates() {
        ArrivalSchedule schedule =
                new ArrivalSchedule(
                        new LoadProfile(Duration.ofSeconds(60), Duration.ofSeconds(30), 500), 500);

        long earlyGap = schedule.sendOffsetNanos(11) - schedule.sendOffsetNanos(10);
        long lateGap = schedule.sendOffsetNanos(2001) - schedule.sendOffsetNanos(2000);

        assertThat(lateGap)
                .as("requests must bunch up as the ramp climbs toward the target rate")
                .isLessThan(earlyGap);
    }

    @Test
    @DisplayName("total volume is ramp triangle plus steady-state rectangle")
    void totalCombinesRampAndSteadyState() {
        // 30s ramp to 400 req/s (= 6000) + 90s steady at 400 (= 36000).
        ArrivalSchedule schedule =
                new ArrivalSchedule(
                        new LoadProfile(Duration.ofSeconds(120), Duration.ofSeconds(30), 400), 400);

        assertThat(schedule.totalRequests()).isEqualTo(42_000);
    }

    @Test
    @DisplayName("a shard's schedule is independent of the fleet-wide rate")
    void shardRateDrivesTheSchedule() {
        // Global 400 req/s split across 4 workers: this shard issues 100/s regardless.
        ArrivalSchedule shard =
                new ArrivalSchedule(
                        new LoadProfile(Duration.ofSeconds(10), Duration.ZERO, 400), 100);

        assertThat(shard.totalRequests()).isEqualTo(1000);
        assertThat(shard.sendOffsetNanos(1)).isEqualTo(10_000_000L);
    }
}
