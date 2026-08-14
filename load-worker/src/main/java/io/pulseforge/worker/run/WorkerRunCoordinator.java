package io.pulseforge.worker.run;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.pulseforge.common.protocol.NatsSubjects;
import io.pulseforge.common.protocol.StartRunCommand;
import io.pulseforge.common.protocol.WorkerFinished;
import io.pulseforge.common.serde.JsonCodec;
import io.pulseforge.worker.config.WorkerProperties;
import io.pulseforge.worker.coordination.RunLiveness;
import io.pulseforge.worker.coordination.ShardClaim;
import io.pulseforge.worker.http.RequestExecutor;
import io.pulseforge.worker.metrics.MetricPipeline;
import io.pulseforge.worker.metrics.SnapshotTransport;
import jakarta.annotation.PreDestroy;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Subscribes to run commands and owns the lifecycle of every run on this worker.
 *
 * <p>Commands are fan-out, not work-queue: every worker receives every command. A queue group would
 * hand the command to exactly one worker, which is the opposite of what a distributed load
 * generator needs. Each worker instead claims a shard index and generates its own share.
 *
 * <p>The shard index is claimed from Redis, so each worker generates a distinct slice of the
 * requested rate and a worker arriving after every shard is taken sits the run out.
 */
@Component
public class WorkerRunCoordinator {

    private static final Logger log = LoggerFactory.getLogger(WorkerRunCoordinator.class);

    /** How long completion waits on the aggregator's final snapshot before giving up on it. */
    private static final Duration FLUSH_TIMEOUT = Duration.ofSeconds(10);

    private final Connection nats;
    private final WorkerProperties properties;
    private final HttpClient httpClient;
    private final ShardClaim shardClaim;
    private final RunLiveness runLiveness;
    private final SnapshotTransport snapshotTransport;
    private final ConcurrentHashMap<UUID, ActiveRun> activeRuns = new ConcurrentHashMap<>();

    private Dispatcher dispatcher;

    public WorkerRunCoordinator(
            Connection nats,
            WorkerProperties properties,
            HttpClient httpClient,
            ShardClaim shardClaim,
            RunLiveness runLiveness,
            SnapshotTransport snapshotTransport) {
        this.nats = nats;
        this.properties = properties;
        this.httpClient = httpClient;
        this.shardClaim = shardClaim;
        this.runLiveness = runLiveness;
        this.snapshotTransport = snapshotTransport;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void subscribe() {
        dispatcher = nats.createDispatcher(message -> {});
        dispatcher.subscribe(NatsSubjects.RUN_COMMANDS, this::onStartCommand);
        dispatcher.subscribe(NatsSubjects.RUN_CONTROL, this::onControlCommand);
        log.info(
                "Worker '{}' listening on {} (snapshot every {}, queue capacity {})",
                properties.id(),
                NatsSubjects.RUN_COMMANDS,
                properties.snapshotInterval(),
                properties.metricQueueCapacity());
    }

    private void onStartCommand(io.nats.client.Message message) {
        StartRunCommand command;
        try {
            command = JsonCodec.decode(message.getData(), StartRunCommand.class);
        } catch (RuntimeException e) {
            log.error("Discarding unreadable run command", e);
            return;
        }

        if (activeRuns.containsKey(command.runId())) {
            log.warn("Ignoring duplicate command for run {}", command.runId());
            return;
        }

        int shardIndex = shardClaim.claim(command.runId(), command.workerCount());
        if (shardIndex == ShardClaim.NO_SHARD) {
            return;
        }
        try {
            startRun(command, shardIndex);
        } catch (RuntimeException e) {
            // The shard is claimed but nothing is running it. Handing it back lets another worker
            // take it; leaving it consumed would silently shrink the fleet by one for this run.
            log.error("Run {}: failed to start shard {}, releasing it", command.runId(), shardIndex, e);
            activeRuns.remove(command.runId());
            runLiveness.stop(command.runId(), properties.id());
            shardClaim.release(command.runId());
        }
    }

    private void onControlCommand(io.nats.client.Message message) {
        try {
            UUID runId = UUID.fromString(new String(message.getData(), StandardCharsets.UTF_8));
            ActiveRun run = activeRuns.get(runId);
            if (run != null) {
                log.info("Abort requested for run {}", runId);
                run.execution().stop();
            }
        } catch (IllegalArgumentException e) {
            log.warn("Ignoring malformed control message", e);
        }
    }

    private void startRun(StartRunCommand command, int shardIndex) {
        MetricPipeline pipeline = new MetricPipeline(properties.metricQueueCapacity());
        StepSelector stepSelector = new StepSelector(command.scenario());
        ArrivalSchedule schedule =
                new ArrivalSchedule(command.scenario().load(), command.rateForShard(shardIndex));

        RequestExecutor requestExecutor =
                new RequestExecutor(
                        httpClient, command.scenario().target(), properties.requestTimeout());

        RunExecution execution =
                new RunExecution(
                        command,
                        shardIndex,
                        schedule,
                        stepSelector,
                        requestExecutor,
                        pipeline,
                        properties.maxConcurrentRequests());

        MetricAggregatorLoop aggregator =
                new MetricAggregatorLoop(
                        command.runId(),
                        properties.id(),
                        properties.snapshotInterval(),
                        pipeline,
                        stepSelector,
                        snapshotTransport,
                        execution);

        ActiveRun activeRun = new ActiveRun(execution, aggregator, pipeline);
        activeRuns.put(command.runId(), activeRun);
        runLiveness.start(command.runId(), properties.id(), properties.heartbeatInterval());

        log.info(
                "Run {}: claimed shard {} of {}, generating {} req/s",
                command.runId(),
                shardIndex,
                command.workerCount(),
                String.format("%.1f", command.rateForShard(shardIndex)));

        // Named after the run so a thread dump from a worker executing several is readable.
        String shortRunId = command.runId().toString().substring(0, 8);

        Thread aggregatorThread = new Thread(aggregator, "aggregate-" + shortRunId);
        // Deliberately not raised: the generator is the thread that must not be descheduled, and
        // giving the aggregator the same priority would put it in competition with what it serves.
        aggregatorThread.start();

        Thread generatorThread =
                new Thread(
                        () -> executeAndReport(command, shardIndex, activeRun),
                        "generate-" + shortRunId);
        // A late request is measurement error, so the generator outranks background work.
        generatorThread.setPriority(Thread.MAX_PRIORITY);
        generatorThread.start();
    }

    private void executeAndReport(StartRunCommand command, int shardIndex, ActiveRun activeRun) {
        try {
            activeRun.execution().run();
        } catch (RuntimeException e) {
            log.error("Run {} failed on worker {}", command.runId(), properties.id(), e);
        } finally {
            // Order matters: the aggregator must finish flushing before completion is announced.
            // stop() only asks it to; the control plane closes the run on the message published
            // below, so announcing first would let results be read a full interval short.
            activeRun.aggregator().stop();
            awaitFinalFlush(command.runId(), activeRun);
            activeRuns.remove(command.runId());
            // Liveness is cleared before the completion notice so the control plane can never see
            // "finished" and "still alive" at the same time.
            runLiveness.stop(command.runId(), properties.id());
            announceFinished(command, shardIndex, activeRun);
        }
    }

    private void awaitFinalFlush(UUID runId, ActiveRun activeRun) {
        try {
            if (!activeRun.aggregator().awaitFinalFlush(FLUSH_TIMEOUT)) {
                log.warn(
                        "Run {}: aggregator did not flush within {}; the final interval may be missing",
                        runId,
                        FLUSH_TIMEOUT);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void announceFinished(StartRunCommand command, int shardIndex, ActiveRun activeRun) {
        WorkerFinished finished =
                new WorkerFinished(
                        command.runId(),
                        properties.id(),
                        shardIndex,
                        Instant.now(),
                        activeRun.execution().issuedRequests(),
                        activeRun.execution().skippedRequests(),
                        activeRun.pipeline().droppedSamples());
        try {
            nats.publish(NatsSubjects.RUN_WORKER_FINISHED, JsonCodec.encode(finished));
        } catch (RuntimeException e) {
            log.error("Failed to announce completion of run {}", command.runId(), e);
        }
    }

    /**
     * Stops generating, then gives each run's aggregator a bounded window to ship what it already
     * measured. The NATS dispatcher is closed last: closing it first would leave the completion
     * messages unpublished and every in-progress run reported as DEGRADED.
     */
    @PreDestroy
    public void shutdown() {
        activeRuns.forEach(
                (runId, run) -> {
                    run.execution().stop();
                    run.aggregator().stop();
                });
        activeRuns.forEach(this::awaitFinalFlush);
        if (dispatcher != null) {
            nats.closeDispatcher(dispatcher);
        }
    }

    private record ActiveRun(
            RunExecution execution, MetricAggregatorLoop aggregator, MetricPipeline pipeline) {}
}
