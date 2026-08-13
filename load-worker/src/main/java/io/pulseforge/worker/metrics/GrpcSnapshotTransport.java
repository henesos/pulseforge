package io.pulseforge.worker.metrics;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.pulseforge.common.protocol.HistogramSnapshot;
import io.pulseforge.common.protocol.SnapshotProtoMapper;
import io.pulseforge.grpc.IngestSummary;
import io.pulseforge.grpc.MetricsIngestionGrpc;
import jakarta.annotation.PreDestroy;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streams snapshots to the ingestor over gRPC.
 *
 * <p>One client-streamed call is held open per run rather than one call per snapshot: at a
 * one-second interval across several steps, per-message call setup would cost more than the
 * payload. The stream also gives the worker something NATS cannot — an acknowledgement, so it
 * learns whether its measurements actually landed.
 *
 * <p>Streams are per run and keyed by run id, because a worker can legitimately be executing more
 * than one run at a time and their snapshots must not interleave on one stream that a single
 * failure would take down.
 */
public class GrpcSnapshotTransport implements SnapshotTransport {

    private static final Logger log = LoggerFactory.getLogger(GrpcSnapshotTransport.class);

    private final ManagedChannel channel;
    private final MetricsIngestionGrpc.MetricsIngestionStub stub;
    private final Map<UUID, StreamObserver<io.pulseforge.grpc.HistogramSnapshot>> streams =
            new ConcurrentHashMap<>();

    public GrpcSnapshotTransport(String target) {
        this.channel =
                ManagedChannelBuilder.forTarget(target)
                        // The ingestor is a peer inside the deployment, not a public endpoint.
                        .usePlaintext()
                        .build();
        this.stub = MetricsIngestionGrpc.newStub(channel);
        log.info("gRPC snapshot transport targeting {}", target);
    }

    @Override
    public void send(HistogramSnapshot snapshot) {
        try {
            streams.computeIfAbsent(snapshot.runId(), this::openStream)
                    .onNext(SnapshotProtoMapper.toProto(snapshot));
        } catch (RuntimeException e) {
            // A broken stream is dropped so the next snapshot opens a fresh one, rather than
            // wedging the run on a connection that will never recover.
            streams.remove(snapshot.runId());
            log.error("Failed to stream snapshot for run {}", snapshot.runId(), e);
        }
    }

    private StreamObserver<io.pulseforge.grpc.HistogramSnapshot> openStream(UUID runId) {
        return stub.streamSnapshots(
                new StreamObserver<>() {
                    @Override
                    public void onNext(IngestSummary summary) {
                        if (summary.getRejected() > 0) {
                            log.warn(
                                    "Run {}: ingestor rejected {} of {} snapshots",
                                    runId,
                                    summary.getRejected(),
                                    summary.getAccepted() + summary.getRejected());
                        } else {
                            log.debug(
                                    "Run {}: ingestor accepted {} snapshots",
                                    runId,
                                    summary.getAccepted());
                        }
                    }

                    @Override
                    public void onError(Throwable error) {
                        streams.remove(runId);
                        log.error("Run {}: snapshot stream failed: {}", runId, error.getMessage());
                    }

                    @Override
                    public void onCompleted() {
                        streams.remove(runId);
                    }
                });
    }

    @Override
    public void runFinished(UUID runId) {
        StreamObserver<io.pulseforge.grpc.HistogramSnapshot> stream = streams.remove(runId);
        if (stream != null) {
            try {
                // Half-closing is what triggers the ingestor's summary; without it the worker
                // never finds out whether its final snapshots were stored.
                stream.onCompleted();
            } catch (RuntimeException e) {
                log.debug("Run {}: stream was already closed", runId);
            }
        }
    }

    @Override
    public String name() {
        return "grpc";
    }

    @PreDestroy
    public void shutdown() {
        streams.values().forEach(StreamObserver::onCompleted);
        streams.clear();
        channel.shutdown();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }
}
