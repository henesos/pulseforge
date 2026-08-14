package io.pulseforge.controlplane.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import io.pulseforge.common.domain.RunStatus;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Completion accounting decides whether a lost shard is reported. NATS delivers at least once, so
 * the difference between counting messages and counting workers is the difference between catching
 * that loss and reporting a clean pass over it.
 */
class TestRunEntityTest {

    @Test
    @DisplayName("a redelivered completion does not advance the count")
    void redeliveryIsIdempotent() {
        TestRunEntity run = run(3);

        assertThat(run.recordWorkerFinished("worker-a")).isTrue();
        assertThat(run.recordWorkerFinished("worker-a"))
                .as("same worker, second delivery")
                .isFalse();
        assertThat(run.recordWorkerFinished("worker-b")).isTrue();

        assertThat(run.getFinishedWorkers())
                .as("two distinct workers reported, not three messages")
                .isEqualTo(2);
        assertThat(run.getFinishedWorkerIds()).containsExactlyInAnyOrder("worker-a", "worker-b");
    }

    @Test
    @DisplayName("three redeliveries of one worker never look like a complete run")
    void redeliveryCannotMaskALostShard() {
        TestRunEntity run = run(3);

        run.recordWorkerFinished("worker-a");
        run.recordWorkerFinished("worker-a");
        run.recordWorkerFinished("worker-a");

        assertThat(run.getFinishedWorkers()).isLessThan(run.getExpectedWorkers());
    }

    @Test
    @DisplayName("degradation is recorded without ending the run")
    void degradationDoesNotTerminate() {
        TestRunEntity run = run(3);
        run.markRunning(Instant.EPOCH);

        run.recordDegradation("1 of 3 worker shards stopped reporting");

        assertThat(run.isDegraded()).isTrue();
        assertThat(run.getStatus())
                .as("the run keeps generating load for the duration that was asked for")
                .isEqualTo(RunStatus.RUNNING);
        assertThat(run.getFinishedAt()).isNull();
    }

    @Test
    @DisplayName("the first reason recorded is the one reported")
    void degradationKeepsTheFirstReason() {
        TestRunEntity run = run(3);

        run.recordDegradation("first");
        run.recordDegradation("second");

        assertThat(run.getDegradedReason()).isEqualTo("first");
    }

    @Test
    @DisplayName("the all-shards-reported instant is set once and not moved")
    void allShardsReportedIsStable() {
        TestRunEntity run = run(1);
        Instant first = Instant.EPOCH.plusSeconds(60);

        run.markAllShardsReported(first);
        run.markAllShardsReported(first.plusSeconds(30));

        assertThat(run.getAllShardsReportedAt())
                .as("the settle window is measured from the first report, not the latest")
                .isEqualTo(first);
    }

    private static TestRunEntity run(int expectedWorkers) {
        return new TestRunEntity(
                UUID.randomUUID(), UUID.randomUUID(), 250, 60, 5, expectedWorkers, Instant.EPOCH);
    }
}
