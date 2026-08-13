package io.pulseforge.controlplane.persistence;

import io.pulseforge.common.domain.RunStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/** Persistence view of one execution of a scenario. */
@Entity
@Table(name = "test_runs")
public class TestRunEntity {

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

    public int recordWorkerFinished() {
        return ++this.finishedWorkers;
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
