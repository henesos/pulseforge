package io.pulseforge.worker.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.pulseforge.common.metrics.HistogramCodec;
import io.pulseforge.common.protocol.HistogramSnapshot;
import io.pulseforge.grpc.IngestSummary;
import io.pulseforge.grpc.MetricsIngestionGrpc;
import java.io.IOException;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * A worker that streams faster than the ingestor can read has to lose something: either the
 * measurement it is holding, or the heap it needs to keep generating load. gRPC's default answer is
 * the second one — {@code onNext} on a stream that is not ready buffers inside the client without
 * limit — and it is the wrong one here, because a worker that starts collecting garbage under
 * memory pressure distorts the very latency it is reporting.
 *
 * <p>Blocking is not available as an alternative: this is called from the aggregator thread, which
 * is also the thread recording samples into histograms.
 *
 * <p>Tested against a real gRPC server rather than a mocked stub, because readiness is a property of
 * the transport's flow-control window. A stubbed {@code isReady()} would only confirm that the code
 * calls the method it was written to call.
 */
class GrpcSnapshotTransportTest {

    /** Well past what it takes to exhaust a connection window; a cap, not an expectation. */
    private static final int SEND_CEILING = 20_000;

    private Server server;
    private GrpcSnapshotTransport transport;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (transport != null) {
            transport.shutdown();
        }
        if (server != null) {
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("a stream the ingestor has stopped reading refuses snapshots instead of buffering them")
    void aCongestedStreamRefusesRatherThanBuffers() throws Exception {
        AtomicInteger receivedByServer = start(new SilentIngestor());
        UUID runId = UUID.randomUUID();

        int sent = 0;
        boolean refused = false;
        while (sent < SEND_CEILING) {
            if (!transport.send(snapshot(runId))) {
                refused = true;
                break;
            }
            sent++;
        }

        assertThat(refused)
                .as(
                        "%d snapshots went out and the transport still claimed the stream was writable; "
                                + "every one past the window is sitting in the worker's heap",
                        sent)
                .isTrue();
        assertThat(receivedByServer.get())
                .as("the server never read a message, so nothing should have been consumed")
                .isZero();
    }

    @Test
    @DisplayName("a run's opening windows are not sacrificed to a stream that is merely connecting")
    void aNewStreamIsNotMistakenForACongestedOne() throws Exception {
        start(new ReadingIngestor());
        UUID runId = UUID.randomUUID();

        // The very first send happens while the connection is still being established, which is
        // exactly when isReady() is false for a reason that has nothing to do with congestion.
        assertThat(transport.send(snapshot(runId)))
                .as("dropping here would cost every run the window that describes its ramp")
                .isTrue();
    }

    @Test
    @DisplayName("a healthy stream keeps accepting snapshots")
    void aReadStreamStaysWritable() throws Exception {
        AtomicInteger receivedByServer = start(new ReadingIngestor());
        UUID runId = UUID.randomUUID();

        for (int i = 0; i < 200; i++) {
            assertThat(transport.send(snapshot(runId))).isTrue();
        }
        transport.runFinished(runId);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (receivedByServer.get() < 200 && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(receivedByServer.get())
                .as("flow control must not cost a snapshot when the consumer is keeping up")
                .isEqualTo(200);
    }

    private AtomicInteger start(Ingestor service) throws IOException {
        server = ServerBuilder.forPort(0).addService(service).build().start();
        transport = new GrpcSnapshotTransport("localhost:" + server.getPort());
        return service.received;
    }

    private abstract static class Ingestor extends MetricsIngestionGrpc.MetricsIngestionImplBase {
        final AtomicInteger received = new AtomicInteger();
    }

    /** Accepts the call and then never reads from it, which is what a saturated ingestor looks like. */
    private static final class SilentIngestor extends Ingestor {
        @Override
        public StreamObserver<io.pulseforge.grpc.HistogramSnapshot> streamSnapshots(
                StreamObserver<IngestSummary> responseObserver) {
            ((ServerCallStreamObserver<IngestSummary>) responseObserver).disableAutoRequest();
            return new StreamObserver<>() {
                @Override
                public void onNext(io.pulseforge.grpc.HistogramSnapshot value) {
                    received.incrementAndGet();
                }

                @Override
                public void onError(Throwable t) {}

                @Override
                public void onCompleted() {}
            };
        }
    }

    /** Reads everything, as an ingestor with capacity does. */
    private static final class ReadingIngestor extends Ingestor {
        @Override
        public StreamObserver<io.pulseforge.grpc.HistogramSnapshot> streamSnapshots(
                StreamObserver<IngestSummary> responseObserver) {
            return new StreamObserver<>() {
                @Override
                public void onNext(io.pulseforge.grpc.HistogramSnapshot value) {
                    received.incrementAndGet();
                }

                @Override
                public void onError(Throwable t) {}

                @Override
                public void onCompleted() {
                    responseObserver.onNext(
                            IngestSummary.newBuilder().setAccepted(received.get()).build());
                    responseObserver.onCompleted();
                }
            };
        }
    }

    private static HistogramSnapshot snapshot(UUID runId) {
        Histogram histogram = HistogramCodec.newHistogram();
        for (int i = 0; i < 200; i++) {
            histogram.recordValue(1_000 + i);
        }
        return new HistogramSnapshot(
                runId,
                "worker-1",
                "list-products",
                Instant.now(),
                Instant.now().plusSeconds(1),
                histogram.getTotalCount(),
                0,
                0,
                0,
                histogram.getMinValue(),
                histogram.getMaxValue(),
                histogram.getTotalCount() * 1_000,
                HistogramCodec.encode(histogram));
    }
}
