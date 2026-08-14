package io.pulseforge.ingestor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.pulseforge.common.protocol.HistogramSnapshot;
import io.pulseforge.ingestor.config.IngestorProperties;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The inbound queue drops rather than pushing back on workers, which is the right call — blocking
 * here would stall the generators and invalidate the run being measured. The cost is that a
 * snapshot dies inside this process, and this is the only moment anything still knows which run it
 * belonged to and how many measurements were in it.
 *
 * <p>Lose that and the run reports a full set of confident percentiles over a population it never
 * saw: the failure the whole backpressure design exists to prevent, reintroduced one hop later.
 */
class SnapshotBufferTest {

    private static final UUID RUN_ID = UUID.randomUUID();

    private final SnapshotWriter writer = mock(SnapshotWriter.class);
    private final IngestLossLedger losses = mock(IngestLossLedger.class);

    @Test
    @DisplayName("a snapshot the queue refuses is attributed to its run before it is let go")
    void anOverflowingQueueRecordsWhatItLost() {
        SnapshotBuffer buffer = buffer(1);

        assertThat(buffer.offer(snapshot(400))).isTrue();
        assertThat(buffer.offer(snapshot(900))).isFalse();

        verify(losses).record(snapshot(900), IngestLossLedger.Reason.QUEUE_FULL);
        assertThat(buffer.droppedCount()).isEqualTo(1);
        assertThat(buffer.receivedCount())
                .as("received counts everything offered, including what was refused")
                .isEqualTo(2);
    }

    @Test
    @DisplayName("a snapshot that fits is not recorded as a loss")
    void anAcceptedSnapshotIsNotALoss() {
        SnapshotBuffer buffer = buffer(4);

        assertThat(buffer.offer(snapshot(400))).isTrue();

        verify(losses, never()).record(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
        assertThat(buffer.droppedCount()).isZero();
    }

    private SnapshotBuffer buffer(int capacity) {
        return new SnapshotBuffer(
                writer,
                losses,
                new IngestorProperties(500, Duration.ofSeconds(2), capacity, false, false, 9090));
    }

    /** Deterministic in every field, so the recorded loss can be matched by equality. */
    private static HistogramSnapshot snapshot(long requestCount) {
        return new HistogramSnapshot(
                RUN_ID,
                "worker-1",
                "list-products",
                Instant.EPOCH,
                Instant.EPOCH.plusSeconds(1),
                requestCount,
                0,
                3,
                7,
                1_000,
                2_000,
                requestCount * 1_500,
                "");
    }
}
