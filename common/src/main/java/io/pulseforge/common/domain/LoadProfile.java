package io.pulseforge.common.domain;

import java.time.Duration;
import java.util.Objects;

/**
 * How much load to apply and for how long.
 *
 * <p>The model is <em>open-loop</em>: {@code arrivalRate} is a request rate the generator tries to
 * honour regardless of how fast the target responds. It is not a virtual-user count. See the
 * coordinated-omission section of the README for why.
 *
 * <p>This record only carries and validates the profile. The ramp itself is applied by
 * {@code ArrivalSchedule} in the worker, which inverts the integral of the rising rate to get each
 * request's send time — a shape that has to be computed per request, not sampled per instant.
 */
public record LoadProfile(Duration duration, Duration rampUp, int arrivalRate) {

    public LoadProfile {
        Objects.requireNonNull(duration, "duration must not be null");
        rampUp = rampUp == null ? Duration.ZERO : rampUp;
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("duration must be positive, was " + duration);
        }
        if (rampUp.isNegative()) {
            throw new IllegalArgumentException("rampUp must not be negative, was " + rampUp);
        }
        if (rampUp.compareTo(duration) > 0) {
            throw new IllegalArgumentException("rampUp " + rampUp + " exceeds duration " + duration);
        }
        if (arrivalRate <= 0) {
            throw new IllegalArgumentException("arrivalRate must be positive, was " + arrivalRate);
        }
    }

}
