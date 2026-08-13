package io.pulseforge.worker.run;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.pulseforge.common.protocol.NatsSubjects;
import io.pulseforge.common.protocol.StartRunCommand;
import io.pulseforge.common.protocol.WorkerFinished;
import io.pulseforge.common.serde.JsonCodec;
import io.pulseforge.worker.config.WorkerProperties;
import io.pulseforge.worker.http.RequestExecutor;
import io.pulseforge.worker.metrics.MetricPipeline;
import jakarta.annotation.PreDestroy;
import java.net.http.HttpClient;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
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
 * <p>In Phase 2 the shard index is always 0 with a worker count of 1. Phase 3 replaces this with a
 * Redis-backed claim so a fleet divides the rate between them.
 */
@Component
public class WorkerRunCoordinator {

    private static final Logger log = LoggerFactory.getLogger(WorkerRunCoordinator.class);

    private final Connection nats;
    private final WorkerProperties properties;
    private final HttpClient httpClient;
    private final ConcurrentHashMap<UUID, ActiveRun> activeRuns = new ConcurrentHashMap<>();
    private final AtomicInteger runThreadCounter = new AtomicInteger();

    private Dispatcher dispatcher;

    public WorkerRunCoordinator(
            Connection nats, WorkerProperties properties, HttpClient httpClient) {
        this.nats = nats;
        this.properties = properties;
        this.httpClient = httpClient;
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

        int shardIndex = 0;
        startRun(command, shardIndex);
    }

    private void onControlCommand(io.nats.client.Message message) {
        try {
            UUID runId = UUID.fromString(new String(message.getData()));
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
                        nats,
                        execution);

        ActiveRun activeRun = new ActiveRun(execution, aggregator, pipeline);
        activeRuns.put(command.runId(), activeRun);

        ThreadFactory factory = runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("run-" + runThreadCounter.incrementAndGet());
            // The generator must not be descheduled behind background work; a late request is
            // measurement error.
            thread.setPriority(Thread.MAX_PRIORITY);
            return thread;
        };

        factory.newThread(aggregator).start();
        factory.newThread(() -> executeAndReport(command, shardIndex, activeRun)).start();
    }

    private void executeAndReport(StartRunCommand command, int shardIndex, ActiveRun activeRun) {
        try {
            activeRun.execution().run();
        } catch (RuntimeException e) {
            log.error("Run {} failed on worker {}", command.runId(), properties.id(), e);
        } finally {
            // Order matters: the aggregator must flush after the generator is done, or the last
            // interval of samples is lost.
            activeRun.aggregator().stop();
            activeRuns.remove(command.runId());
            announceFinished(command, shardIndex, activeRun);
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

    @PreDestroy
    public void shutdown() {
        activeRuns.values().forEach(run -> run.execution().stop());
        if (dispatcher != null) {
            nats.closeDispatcher(dispatcher);
        }
    }

    private record ActiveRun(
            RunExecution execution, MetricAggregatorLoop aggregator, MetricPipeline pipeline) {}
}
