package io.pulseforge.controlplane.persistence;

import io.pulseforge.common.domain.RunStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/** Persistence view of one execution of a scenario. */
@Entity
@Table(name = "test_runs")
public class TestRunEntity {

    /** Worker ids are configuration values, so a delimiter they cannot contain is enough. */
    private static final char SEPARATOR = ',';

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "scenario_id", nullable = false)
    private UUID scenarioId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private RunStatus status;

    @Column(name = "arrival_rate", nullable = false)
    private int arrivalRate;

    @Column(name = "duration_seconds", nullable = false)
    private int durationSeconds;

    @Column(name = "ramp_up_seconds", nullable = false)
    private int rampUpSeconds;

    @Column(name = "expected_workers", nullable = false)
    private int expectedWorkers;

    @Column(name = "finished_workers", nullable = false)
    private int finishedWorkers;

    @Column(name = "finished_worker_ids", columnDefinition = "text")
    private String finishedWorkerIds;

    @Column(name = "all_shards_reported_at")
    private Instant allShardsReportedAt;

    @Column(name = "degraded_reason", columnDefinition = "text")
    private String degradedReason;

    @Column(name = "status_reason", columnDefinition = "text")
    private String statusReason;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "started_at")
    private Instant startedAt;

    @Column(name = "finished_at")
    private Instant finishedAt;

    protected TestRunEntity() {
        // Required by JPA.
    }

    public TestRunEntity(
            UUID id,
            UUID scenarioId,
            int arrivalRate,
            int durationSeconds,
            int rampUpSeconds,
            int expectedWorkers,
            Instant now) {
        this.id = id;
        this.scenarioId = scenarioId;
        this.status = RunStatus.PENDING;
        this.arrivalRate = arrivalRate;
        this.durationSeconds = durationSeconds;
        this.rampUpSeconds = rampUpSeconds;
        this.expectedWorkers = expectedWorkers;
        this.finishedWorkers = 0;
        this.createdAt = now;
    }

    public void markRunning(Instant startedAt) {
        this.status = RunStatus.RUNNING;
        this.startedAt = startedAt;
    }

    /**
     * Terminates the run. {@code reason} is required for anything other than a clean completion —
     * a DEGRADED or FAILED run with no explanation is not actionable.
     */
    public void terminate(RunStatus status, String reason, Instant finishedAt) {
        if (!status.isTerminal()) {
            throw new IllegalArgumentException(status + " is not a terminal status");
        }
        this.status = status;
        this.statusReason = reason;
        this.finishedAt = finishedAt;
    }

    /**
     * Records that {@code workerId} finished its shard. Returns false if this worker had already
     * reported, in which case nothing changed.
     *
     * <p>Identities rather than a count. NATS delivers at least once, so a counter can reach the
     * expected total on redelivered messages while a shard is still missing — which would report
     * COMPLETED for exactly the loss the watchdog exists to catch.
     */
    public boolean recordWorkerFinished(String workerId) {
        if (workerId == null || workerId.isBlank()) {
            throw new IllegalArgumentException("workerId must not be blank");
        }
        if (workerId.indexOf(SEPARATOR) >= 0) {
            throw new IllegalArgumentException(
                    "workerId must not contain '" + SEPARATOR + "', was " + workerId);
        }
        Set<String> ids = getFinishedWorkerIds();
        if (!ids.add(workerId)) {
            return false;
        }
        this.finishedWorkerIds = String.join(String.valueOf(SEPARATOR), ids);
        this.finishedWorkers = ids.size();
        return true;
    }

    /**
     * Notes that the run lost a shard, without ending it.
     *
     * <p>The run keeps generating load for the duration that was asked for and carries this reason
     * into its terminal status. Ending it here instead would publish partial numbers as final while
     * the surviving shards were still working.
     */
    public void recordDegradation(String reason) {
        if (this.degradedReason == null) {
            this.degradedReason = reason;
        }
    }

    /**
     * Marks the instant every expected shard had reported. The run is closed later: its final
     * snapshots are still travelling to ClickHouse when the last worker announces itself.
     */
    public void markAllShardsReported(Instant at) {
        if (this.allShardsReportedAt == null) {
            this.allShardsReportedAt = at;
        }
    }

    public UUID getId() {
        return id;
    }

    public UUID getScenarioId() {
        return scenarioId;
    }

    public RunStatus getStatus() {
        return status;
    }

    public int getArrivalRate() {
        return arrivalRate;
    }

    public int getDurationSeconds() {
        return durationSeconds;
    }

    public int getRampUpSeconds() {
        return rampUpSeconds;
    }

    public int getExpectedWorkers() {
        return expectedWorkers;
    }

    public int getFinishedWorkers() {
        return finishedWorkers;
    }

    /** Mutable view; changes are only persisted through {@link #recordWorkerFinished(String)}. */
    public Set<String> getFinishedWorkerIds() {
        if (finishedWorkerIds == null || finishedWorkerIds.isBlank()) {
            return new LinkedHashSet<>();
        }
        return Arrays.stream(finishedWorkerIds.split(String.valueOf(SEPARATOR)))
                .filter(id -> !id.isBlank())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public Instant getAllShardsReportedAt() {
        return allShardsReportedAt;
    }

    public String getDegradedReason() {
        return degradedReason;
    }

    public boolean isDegraded() {
        return degradedReason != null;
    }

    public String getStatusReason() {
        return statusReason;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getStartedAt() {
        return startedAt;
    }

    public Instant getFinishedAt() {
        return finishedAt;
    }
}
