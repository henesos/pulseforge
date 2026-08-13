package io.pulseforge.worker.run;

import io.pulseforge.common.domain.LoadProfile;
import java.time.Duration;

/**
 * Computes when each request is <em>supposed</em> to be issued.
 *
 * <p>This class is the mechanical answer to coordinated omission. The schedule is derived purely
 * from the load profile and the run's start time — never from when the previous response arrived.
 * A closed-loop generator asks "the last response came back, what now?", which means a slow target
 * receives fewer requests and its worst behaviour goes unmeasured. Here, request <em>n</em> has a
 * send time fixed before the run even starts.
 *
 * <p>With a ramp-up, the rate rises linearly from zero. Integrating that rate gives the number of
 * requests due by time <em>t</em>, and inverting it gives the send time of request <em>n</em> —
 * which is what {@link #sendOffsetNanos(long)} returns.
 */
public class ArrivalSchedule {

    private final double ratePerSecond;
    private final double rampUpSeconds;
    private final double durationSeconds;

    /**
     * @param profile the scenario-wide profile, used for ramp-up shape and duration
     * @param shardRate this worker's share of the global arrival rate
     */
    public ArrivalSchedule(LoadProfile profile, double shardRate) {
        this.ratePerSecond = shardRate;
        this.rampUpSeconds = profile.rampUp().toNanos() / 1_000_000_000.0d;
        this.durationSeconds = profile.duration().toNanos() / 1_000_000_000.0d;
    }

    /**
     * Nanoseconds after run start at which request {@code index} (0-based) should be sent.
     *
     * <p>During ramp-up the instantaneous rate is {@code r · t / T}, so the request count by time
     * {@code t} is the integral {@code r · t² / (2T)}. Inverting for {@code t} gives the closed
     * form below; after the ramp the spacing is constant.
     */
    public long sendOffsetNanos(long index) {
        double requestNumber = index + 1;

        if (rampUpSeconds <= 0) {
            return Math.round(index / ratePerSecond * 1_000_000_000.0d);
        }

        double requestsDuringRamp = ratePerSecond * rampUpSeconds / 2.0d;

        if (requestNumber <= requestsDuringRamp) {
            double seconds = Math.sqrt(2.0d * index * rampUpSeconds / ratePerSecond);
            return Math.round(seconds * 1_000_000_000.0d);
        }

        double secondsAfterRamp = (index - requestsDuringRamp) / ratePerSecond;
        return Math.round((rampUpSeconds + secondsAfterRamp) * 1_000_000_000.0d);
    }

    /** Total requests this shard should issue over the whole run. */
    public long totalRequests() {
        double rampRequests = ratePerSecond * Math.min(rampUpSeconds, durationSeconds) / 2.0d;
        double steadySeconds = Math.max(0, durationSeconds - rampUpSeconds);
        return Math.round(rampRequests + ratePerSecond * steadySeconds);
    }

    public Duration duration() {
        return Duration.ofNanos(Math.round(durationSeconds * 1_000_000_000.0d));
    }

    public double ratePerSecond() {
        return ratePerSecond;
    }
}
