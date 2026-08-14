package io.pulseforge.worker.metrics;

import io.nats.client.Connection;
import io.pulseforge.common.protocol.HistogramSnapshot;
import io.pulseforge.common.protocol.NatsSubjects;
import io.pulseforge.common.serde.JsonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes snapshots to NATS.
 *
 * <p>Fire and forget. The worker does not learn whether the ingestor stored the snapshot, which is
 * the accepted cost of not coupling the fleet to an ingestor address: workers can be started
 * before ingestors, and an ingestor can be restarted mid-run without any worker noticing.
 */
public class NatsSnapshotTransport implements SnapshotTransport {

    private static final Logger log = LoggerFactory.getLogger(NatsSnapshotTransport.class);

    private final Connection nats;

    public NatsSnapshotTransport(Connection nats) {
        this.nats = nats;
    }

    @Override
    public boolean send(HistogramSnapshot snapshot) {
        try {
            nats.publish(NatsSubjects.METRICS_SNAPSHOTS, JsonCodec.encode(snapshot));
            // True means "handed to the client", not "stored". Fire and forget cannot promise more,
            // and claiming otherwise here would be worse than admitting the limit.
            return true;
        } catch (RuntimeException e) {
            // Losing a snapshot must not kill the run; the gap shows up in the results.
            log.error(
                    "Failed to publish snapshot for run {} step {}",
                    snapshot.runId(),
                    snapshot.stepName(),
                    e);
            return false;
        }
    }

    @Override
    public String name() {
        return "nats";
    }
}
