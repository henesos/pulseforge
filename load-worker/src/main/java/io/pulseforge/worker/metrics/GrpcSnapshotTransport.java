package io.pulseforge.worker.metrics;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
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
import java.util.concurrent.atomic.AtomicBoolean;
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
 *
 * <p>Sends are flow controlled. {@code onNext} on a stream the transport is not ready to write
 * buffers inside gRPC without limit, so an ingestor slower than the fleet would be paid for in the
 * worker's heap — growing quietly until the JVM that is supposed to be generating load starts
 * collecting garbage instead, which corrupts the very measurement in flight. Blocking is not the
 * alternative: this is called from the aggregator thread, which is also the thread recording
 * samples. So a congested stream drops the snapshot and says so, and the loop folds the loss into
 * the run's reported counters.
 */
public class GrpcSnapshotTransport implements SnapshotTransport {

    private static final Logger log = LoggerFactory.getLogger(GrpcSnapshotTransport.class);

    private final ManagedChannel channel;
    private final MetricsIngestionGrpc.MetricsIngestionStub stub;
    private final Map<UUID, RunStream> streams = new ConcurrentHashMap<>();

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
    public boolean send(HistogramSnapshot snapshot) {
        try {
            RunStream stream = streams.computeIfAbsent(snapshot.runId(), this::openStream);
            if (!stream.writable()) {
                log.warn(
                        "Run {}: stream to the ingestor is congested, dropping the {} window",
                        snapshot.runId(),
                        snapshot.stepName());
                return false;
            }
            stream.observer().onNext(SnapshotProtoMapper.toProto(snapshot));
            return true;
        } catch (RuntimeException e) {
            // A broken stream is dropped so the next snapshot opens a fresh one, rather than
            // wedging the run on a connection that will never recover.
            streams.remove(snapshot.runId());
            log.error("Failed to stream snapshot for run {}", snapshot.runId(), e);
            return false;
        }
    }

    /**
     * One run's stream, plus the fact that decides whether {@code isReady() == false} means
     * congested or merely not connected yet.
     *
     * <p>A call is not ready until its transport is, so a brand new stream reports false for as
     * long as the connection takes to establish. Treating that as congestion would drop the opening
     * windows of every run — the ones that describe the ramp — for a stream that was about to be
     * perfectly healthy. Until a stream has been ready once, its sends go through and gRPC buffers
     * them; after that, an unready stream is genuinely a consumer that cannot keep up.
     */
    private record RunStream(
            StreamObserver<io.pulseforge.grpc.HistogramSnapshot> observer,
            ClientCallStreamObserver<io.pulseforge.grpc.HistogramSnapshot> flowControl,
            AtomicBoolean everReady) {

        boolean writable() {
            if (flowControl == null) {
                return true;
            }
            if (flowControl.isReady()) {
                everReady.set(true);
                return true;
            }
            return !everReady.get();
        }
    }

    private RunStream openStream(UUID runId) {
        StreamObserver<io.pulseforge.grpc.HistogramSnapshot> observer = startCall(runId);
        ClientCallStreamObserver<io.pulseforge.grpc.HistogramSnapshot> flowControl =
                observer instanceof ClientCallStreamObserver<io.pulseforge.grpc.HistogramSnapshot> c
                        ? c
                        : null;
        if (flowControl == null) {
            // Every gRPC client-streaming call returns one; a stub that does not is a test double
            // or a future version, and losing flow control is not a reason to lose the run.
            log.warn("Run {}: stream offers no flow control, sending without it", runId);
        }
        return new RunStream(observer, flowControl, new AtomicBoolean());
    }

    private StreamObserver<io.pulseforge.grpc.HistogramSnapshot> startCall(UUID runId) {
        return stub.streamSnapshots(
                new StreamObserver<>() {
                    @Override
                    public void onNext(IngestSummary summary) {
                        // Logged rather than folded into the run's dropped-sample count, for two
                        // reasons that are both about honesty. The summary is sent when the stream
                        // half-closes, which is after the final snapshot has already been shipped —
                        // there is nothing left to carry it. And it counts snapshots, not
                        // measurements: the worker cannot know how many requests were inside the
                        // ones the ingestor threw away, so any number it reported would be
                        // invented. The ingestor holds those snapshots when it drops them, so that
                        // is where the count belongs.
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
        RunStream stream = streams.remove(runId);
        if (stream != null) {
            try {
                // Half-closing is what triggers the ingestor's summary; without it the worker
                // never finds out whether its final snapshots were stored.
                stream.observer().onCompleted();
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
        streams.values().forEach(stream -> stream.observer().onCompleted());
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
