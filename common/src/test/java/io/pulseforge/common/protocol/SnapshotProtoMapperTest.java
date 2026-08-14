package io.pulseforge.common.protocol;

import static org.assertj.core.api.Assertions.assertThat;

import io.pulseforge.common.metrics.HistogramCodec;
import java.time.Instant;
import java.util.UUID;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The gRPC transport is the only path where a snapshot changes shape in flight, so this is where a
 * field can go missing without anything failing: a value dropped by the mapper reappears as a zero,
 * which reads like a real measurement rather than a bug.
 *
 * <p>The histogram gets its own attention. It crosses as raw bytes on this path and as base64 on the
 * NATS path, and a mis-round-tripped payload does not throw — it decodes into different percentiles.
 */
class SnapshotProtoMapperTest {

    private static final UUID RUN_ID = UUID.fromString("6b3f1f4e-6f6a-4a67-9e17-3f2b7f2a1c55");

    @Test
    @DisplayName("every field survives the round trip")
    void roundTripPreservesEveryField() {
        HistogramSnapshot original = snapshot(histogramOf(120, 900, 4_500));

        HistogramSnapshot returned = SnapshotProtoMapper.fromProto(SnapshotProtoMapper.toProto(original));

        assertThat(returned)
                .as("a field lost in the mapper comes back as a plausible zero, not as an error")
                .isEqualTo(original);
    }

    @Test
    @DisplayName("the distribution itself is intact, not merely a string that copied across")
    void roundTripPreservesTheDistribution() {
        Histogram source = histogramOf(1_000, 2_000, 3_000, 4_000, 250_000);
        HistogramSnapshot original = snapshot(source);

        HistogramSnapshot returned = SnapshotProtoMapper.fromProto(SnapshotProtoMapper.toProto(original));
        Histogram decoded = HistogramCodec.decode(returned.histogramBase64());

        assertThat(decoded.getTotalCount()).isEqualTo(source.getTotalCount());
        assertThat(decoded.getMaxValue()).isEqualTo(source.getMaxValue());
        assertThat(decoded.getValueAtPercentile(99.0))
                .as("percentiles are what the ingestor merges; nothing else about the payload matters")
                .isEqualTo(source.getValueAtPercentile(99.0));
    }

    @Test
    @DisplayName("the wire carries raw histogram bytes, not the base64 the JSON path needs")
    void protoCarriesRawBytes() {
        HistogramSnapshot original = snapshot(histogramOf(120, 900, 4_500));

        io.pulseforge.grpc.HistogramSnapshot proto = SnapshotProtoMapper.toProto(original);

        assertThat(proto.getHistogram().size())
                .as("base64 costs a third more, and the gRPC wire is already binary")
                .isLessThan(original.histogramBase64().length());
        assertThat(proto.getHistogram().toByteArray())
                .isEqualTo(java.util.Base64.getDecoder().decode(original.histogramBase64()));
    }

    @Test
    @DisplayName("window instants are carried at millisecond precision")
    void windowInstantsAreTruncatedToMillis() {
        // Instant.now() has sub-millisecond precision on Linux, and the proto field is millis. The
        // truncation is the contract: ClickHouse stores these windows at millisecond resolution
        // anyway, so pinning it here stops a future reader from reading the loss as a bug.
        Instant start = Instant.parse("2026-01-01T00:00:00.123456789Z");
        HistogramSnapshot original =
                new HistogramSnapshot(
                        RUN_ID,
                        "worker-1",
                        "list-products",
                        start,
                        start.plusMillis(1_000),
                        10,
                        1,
                        0,
                        0,
                        120,
                        4_500,
                        20_000,
                        HistogramCodec.encode(histogramOf(120, 900, 4_500)));

        HistogramSnapshot returned = SnapshotProtoMapper.fromProto(SnapshotProtoMapper.toProto(original));

        assertThat(returned.windowStart()).isEqualTo(Instant.parse("2026-01-01T00:00:00.123Z"));
        assertThat(returned.windowEnd()).isEqualTo(Instant.parse("2026-01-01T00:00:01.123Z"));
    }

    @Test
    @DisplayName("a window with drops but no requests keeps its counters")
    void anEmptyWindowStillCarriesItsCounters() {
        // The one shape a "nothing happened, skip it" mapper would quietly lose: the interval where
        // the only thing to report is that measurements were thrown away.
        HistogramSnapshot original =
                new HistogramSnapshot(
                        RUN_ID,
                        "worker-1",
                        "list-products",
                        Instant.EPOCH,
                        Instant.EPOCH.plusSeconds(1),
                        0,
                        0,
                        42,
                        7,
                        0,
                        0,
                        0,
                        HistogramCodec.encode(HistogramCodec.newHistogram()));

        HistogramSnapshot returned = SnapshotProtoMapper.fromProto(SnapshotProtoMapper.toProto(original));

        assertThat(returned.droppedSamples()).isEqualTo(42);
        assertThat(returned.skippedRequests()).isEqualTo(7);
        assertThat(returned).isEqualTo(original);
    }

    private static HistogramSnapshot snapshot(Histogram histogram) {
        return new HistogramSnapshot(
                RUN_ID,
                "worker-1",
                "list-products",
                Instant.parse("2026-01-01T00:00:00Z"),
                Instant.parse("2026-01-01T00:00:01Z"),
                histogram.getTotalCount(),
                1,
                3,
                5,
                histogram.getMinValue(),
                histogram.getMaxValue(),
                20_000,
                HistogramCodec.encode(histogram));
    }

    private static Histogram histogramOf(long... latenciesMicros) {
        Histogram histogram = HistogramCodec.newHistogram();
        for (long latency : latenciesMicros) {
            histogram.recordValue(latency);
        }
        return histogram;
    }
}
