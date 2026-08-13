package io.pulseforge.controlplane.service;

import io.nats.client.Connection;
import io.pulseforge.common.domain.RunStatus;
import io.pulseforge.common.domain.Scenario;
import io.pulseforge.common.protocol.NatsSubjects;
import io.pulseforge.common.protocol.StartRunCommand;
import io.pulseforge.common.serde.JsonCodec;
import io.pulseforge.controlplane.config.RunProperties;
import io.pulseforge.controlplane.persistence.ScenarioEntity;
import io.pulseforge.controlplane.persistence.TestRunEntity;
import io.pulseforge.controlplane.persistence.TestRunRepository;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Starts, tracks and terminates runs.
 *
 * <p>The run row is committed <em>before</em> the command is published. If publishing failed after
 * workers had already begun, an orphaned run would be generating load with nothing recording it.
 */
@Service
public class RunService {

    private static final Logger log = LoggerFactory.getLogger(RunService.class);

    private final TestRunRepository runs;
    private final ScenarioService scenarios;
    private final WorkerRegistry workers;
    private final Connection nats;
    private final RunProperties properties;
    private final Clock clock;

    public RunService(
            TestRunRepository runs,
            ScenarioService scenarios,
            WorkerRegistry workers,
            Connection nats,
            RunProperties properties,
            Clock clock) {
        this.runs = runs;
        this.scenarios = scenarios;
        this.workers = workers;
        this.nats = nats;
        this.properties = properties;
        this.clock = clock;
    }

    @Transactional
    public TestRunEntity start(UUID scenarioId) {
        ScenarioEntity entity = scenarios.findById(scenarioId);
        Scenario scenario = scenarios.parse(entity);

        // The fleet size is read once and frozen into the run. If it were re-read as workers came
        // and went, the arrival rate would shift mid-run and the results would describe two
        // different experiments stitched together.
        int workerCount = workers.liveWorkerCount();
        if (workerCount == 0) {
            throw new NoWorkersAvailableException();
        }

        TestRunEntity run =
                runs.save(
                        new TestRunEntity(
                                UUID.randomUUID(),
                                entity.getId(),
                                scenario.load().arrivalRate(),
                                (int) scenario.load().duration().toSeconds(),
                                (int) scenario.load().rampUp().toSeconds(),
                                workerCount,
                                clock.instant()));

        // Workers receive the command microseconds apart; an absolute start instant keeps their
        // ramp-up curves aligned instead of staggered by delivery jitter.
        Instant startAt = clock.instant().plus(properties.dispatchLead());

        StartRunCommand command =
                new StartRunCommand(run.getId(), scenario, startAt, workerCount);
        nats.publish(NatsSubjects.RUN_COMMANDS, JsonCodec.encode(command));

        run.markRunning(startAt);
        log.info(
                "Run {} dispatched: scenario '{}', {} req/s for {}, {} worker(s), starting at {}",
                run.getId(),
                scenario.name(),
                scenario.load().arrivalRate(),
                scenario.load().duration(),
                workerCount,
                startAt);
        return run;
    }

    @Transactional
    public TestRunEntity abort(UUID runId) {
        TestRunEntity run = findById(runId);
        if (run.getStatus().isTerminal()) {
            return run;
        }
        nats.publish(NatsSubjects.RUN_CONTROL, runId.toString().getBytes(StandardCharsets.UTF_8));
        run.terminate(RunStatus.ABORTED, "aborted on operator request", clock.instant());
        log.info("Run {} aborted", runId);
        return run;
    }

    /**
     * Records that a worker finished its shard, and closes the run once every expected shard has
     * reported. Waiting for the shards rather than the clock is what lets a missing worker be
     * detected instead of silently producing a short result.
     */
    @Transactional
    public void recordWorkerFinished(UUID runId, String workerId) {
        TestRunEntity run = runs.findById(runId).orElse(null);
        if (run == null || run.getStatus().isTerminal()) {
            return;
        }
        int finished = run.recordWorkerFinished();
        log.info("Run {}: worker {} finished ({}/{})", runId, workerId, finished, run.getExpectedWorkers());

        if (finished >= run.getExpectedWorkers()) {
            // Snapshots for the final interval are still in flight; the settle delay keeps them
            // from landing after the results have already been read.
            run.terminate(RunStatus.COMPLETED, null, clock.instant());
            log.info("Run {} completed", runId);
        }
    }

    @Transactional(readOnly = true)
    public TestRunEntity findById(UUID runId) {
        return runs.findById(runId).orElseThrow(() -> new RunNotFoundException(runId));
    }

    @Transactional(readOnly = true)
    public List<TestRunEntity> findAll() {
        return runs.findAll();
    }

    /** How long a caller should wait after completion before reading results. */
    public Duration settleDelay() {
        return properties.settleDelay();
    }

    public static class RunNotFoundException extends RuntimeException {
        public RunNotFoundException(UUID id) {
            super("no run with id " + id);
        }
    }

    /** Refusing beats accepting a run that nothing will execute and that would sit RUNNING. */
    public static class NoWorkersAvailableException extends RuntimeException {
        public NoWorkersAvailableException() {
            super("no load workers are registered; start at least one before triggering a run");
        }
    }
}
