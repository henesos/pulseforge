package io.pulseforge.common.protocol;

import com.google.protobuf.ByteString;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;

/**
 * Converts between the internal snapshot record and its protobuf form.
 *
 * <p>Kept as an explicit mapper rather than making the generated type the domain type. The proto is
 * a transport contract with its own compatibility rules; letting it leak into the rest of the
 * codebase would make every field change a wire-format decision.
 *
 * <p>One asymmetry worth noting: the NATS path carries the histogram base64-encoded because the
 * payload is JSON, while the gRPC path carries raw bytes. Encoding twice would waste roughly a
 * third of the payload for nothing.
 */
public final class SnapshotProtoMapper {

    private SnapshotProtoMapper() {
        throw new AssertionError("utility class");
    }

    public static io.pulseforge.grpc.HistogramSnapshot toProto(HistogramSnapshot snapshot) {
        return io.pulseforge.grpc.HistogramSnapshot.newBuilder()
                .setRunId(snapshot.runId().toString())
                .setWorkerId(snapshot.workerId())
                .setStepName(snapshot.stepName())
                .setWindowStartMillis(snapshot.windowStart().toEpochMilli())
                .setWindowEndMillis(snapshot.windowEnd().toEpochMilli())
                .setRequestCount(snapshot.requestCount())
                .setErrorCount(snapshot.errorCount())
                .setDroppedSamples(snapshot.droppedSamples())
                .setSkippedRequests(snapshot.skippedRequests())
                .setMinMicros(snapshot.minMicros())
                .setMaxMicros(snapshot.maxMicros())
                .setSumMicros(snapshot.sumMicros())
                .setHistogram(
                        ByteString.copyFrom(
                                Base64.getDecoder().decode(snapshot.histogramBase64())))
                .build();
    }

    public static HistogramSnapshot fromProto(io.pulseforge.grpc.HistogramSnapshot proto) {
        return new HistogramSnapshot(
                UUID.fromString(proto.getRunId()),
                proto.getWorkerId(),
                proto.getStepName(),
                Instant.ofEpochMilli(proto.getWindowStartMillis()),
                Instant.ofEpochMilli(proto.getWindowEndMillis()),
                proto.getRequestCount(),
                proto.getErrorCount(),
                proto.getDroppedSamples(),
                proto.getSkippedRequests(),
                proto.getMinMicros(),
                proto.getMaxMicros(),
                proto.getSumMicros(),
                Base64.getEncoder().encodeToString(proto.getHistogram().toByteArray()));
    }
}
