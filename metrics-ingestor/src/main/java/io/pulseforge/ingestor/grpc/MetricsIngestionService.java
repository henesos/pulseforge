package io.pulseforge.ingestor.grpc;

import io.grpc.stub.StreamObserver;
import io.pulseforge.common.protocol.SnapshotProtoMapper;
import io.pulseforge.grpc.IngestSummary;
import io.pulseforge.grpc.MetricsIngestionGrpc;
import io.pulseforge.ingestor.SnapshotBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC entry point for the snapshot stream.
 *
 * <p>Snapshots land in the same bounded buffer the NATS path uses, so both transports share one
 * batching and writing implementation and cannot drift apart in behaviour. The transport is a
 * delivery detail; what happens to a snapshot afterwards must not depend on how it arrived.
 *
 * <p>A malformed message is counted and skipped rather than failing the stream: one bad snapshot
 * should not cost a worker the rest of its run's measurements.
 */
public class MetricsIngestionService extends MetricsIngestionGrpc.MetricsIngestionImplBase {

    private static final Logger log = LoggerFactory.getLogger(MetricsIngestionService.class);

    private final SnapshotBuffer buffer;

    public MetricsIngestionService(SnapshotBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public StreamObserver<io.pulseforge.grpc.HistogramSnapshot> streamSnapshots(
            StreamObserver<IngestSummary> responseObserver) {

        return new StreamObserver<>() {
            private long accepted;
            private long rejected;

            @Override
            public void onNext(io.pulseforge.grpc.HistogramSnapshot proto) {
                try {
                    if (buffer.offer(SnapshotProtoMapper.fromProto(proto))) {
                        accepted++;
                    } else {
                        rejected++;
                    }
                } catch (RuntimeException e) {
                    rejected++;
                    log.error("Discarding unreadable snapshot from {}", proto.getWorkerId(), e);
                }
            }

            @Override
            public void onError(Throwable error) {
                // A worker that dies mid-run drops its stream; that is expected and is the
                // control plane's problem to classify, not an ingestor failure.
                log.warn(
                        "Snapshot stream closed with an error after {} accepted: {}",
                        accepted,
                        error.getMessage());
            }

            @Override
            public void onCompleted() {
                responseObserver.onNext(
                        IngestSummary.newBuilder()
                                .setAccepted(accepted)
                                .setRejected(rejected)
                                .build());
                responseObserver.onCompleted();
                if (rejected > 0) {
                    log.warn("Stream finished: {} accepted, {} rejected", accepted, rejected);
                } else {
                    log.debug("Stream finished: {} accepted", accepted);
                }
            }
        };
    }
}
