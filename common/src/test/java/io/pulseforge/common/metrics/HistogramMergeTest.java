package io.pulseforge.common.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins down the claim the whole result pipeline rests on: merging distributions gives the true
 * percentile, and averaging per-shard percentiles does not.
 */
class HistogramMergeTest {

    @Test
    @DisplayName("the average of per-worker p99s is not the p99 of the population")
    void averagingPercentilesIsWrong() {
        // Two workers with very different latency profiles, as happens whenever load is uneven.
        Histogram fastWorker = HistogramCodec.newHistogram();
        for (int i = 0; i < 9_900; i++) {
            fastWorker.recordValue(1_000);
        }
        for (int i = 0; i < 100; i++) {
            fastWorker.recordValue(5_000);
        }

        Histogram slowWorker = HistogramCodec.newHistogram();
        for (int i = 0; i < 100; i++) {
            slowWorker.recordValue(200_000);
        }

        double averagedP99 =
                (fastWorker.getValueAtPercentile(99) + slowWorker.getValueAtPercentile(99)) / 2.0;

        Histogram merged = HistogramCodec.newHistogram();
        merged.add(fastWorker);
        merged.add(slowWorker);
        double mergedP99 = merged.getValueAtPercentile(99);

        // The true p99 of 10 100 samples is the 9 999th, which still sits in the fast worker's
        // 5ms bucket -- the 100 slow samples live above the 99th percentile entirely.
        assertThat(mergedP99).isCloseTo(5_000, org.assertj.core.data.Offset.offset(50.0));

        // Averaging reports ~100ms for a population whose real p99 is ~5ms: a 20x error, in the
        // pessimistic direction here and optimistic when the traffic split is reversed. Either way
        // the magnitude of the error is unbounded and cannot be corrected after the fact.
        assertThat(averagedP99)
                .as("averaging per-worker percentiles is not an approximation, it is a different number")
                .isGreaterThan(mergedP99 * 10);
    }

    @Test
    @DisplayName("encode/decode round-trips the distribution exactly")
    void roundTripsThroughTheWireFormat() {
        Histogram original = HistogramCodec.newHistogram();
        for (int i = 1; i <= 10_000; i++) {
            original.recordValue(i);
        }

        Histogram decoded = HistogramCodec.decode(HistogramCodec.encode(original));

        assertThat(decoded.getTotalCount()).isEqualTo(original.getTotalCount());
        assertThat(decoded.getValueAtPercentile(50)).isEqualTo(original.getValueAtPercentile(50));
        assertThat(decoded.getValueAtPercentile(99)).isEqualTo(original.getValueAtPercentile(99));
        assertThat(decoded.getMaxValue()).isEqualTo(original.getMaxValue());
    }

    @Test
    @DisplayName("merged bucket counts preserve the total sample count")
    void mergePreservesSampleCount() {
        Histogram merged = HistogramCodec.newHistogram();
        long expected = 0;
        for (int worker = 0; worker < 5; worker++) {
            Histogram shard = HistogramCodec.newHistogram();
            for (int i = 0; i < 1_000; i++) {
                shard.recordValue(1_000L + worker * 500L);
            }
            expected += shard.getTotalCount();
            merged.add(HistogramCodec.decode(HistogramCodec.encode(shard)));
        }
        assertThat(merged.getTotalCount()).isEqualTo(expected);
    }
}
