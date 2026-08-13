package io.pulseforge.common.domain;

/** Measurable quantity an assertion can be expressed against. */
public enum AssertionType {

    /** Latency percentile in milliseconds, e.g. {@code p95 < 250ms}. */
    PERCENTILE,

    /** Share of failed requests as a percentage, e.g. {@code errorRate < 1%}. */
    ERROR_RATE,

    /** Achieved requests per second, e.g. {@code throughput > 380}. */
    THROUGHPUT
}
