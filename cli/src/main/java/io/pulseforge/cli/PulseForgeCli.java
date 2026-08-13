package io.pulseforge.cli;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.Files;
import java.nio.file.Path;
import io.pulseforge.common.scenario.DurationSyntax;
import java.time.Duration;
import java.time.Instant;

/**
 * Runs a scenario and turns the verdict into an exit code.
 *
 * <pre>
 *   pulseforge run &lt;scenario.yaml&gt; [--control-plane URL] [--poll DURATION] [--timeout DURATION]
 * </pre>
 *
 * <p>This is what makes the tool usable in CI. A pipeline step that fails on a latency regression
 * needs exactly one thing from a load tester: a non-zero exit code with a readable explanation
 * above it.
 */
public class PulseForgeCli {

    private static final String DEFAULT_CONTROL_PLANE = "http://localhost:8080";
    private static final Duration DEFAULT_POLL = Duration.ofSeconds(2);

    /**
     * Plain {@code main}, with no Spring context. The module is packaged by the Boot plugin so it
     * ships through the same Dockerfile as the services, but a CLI that starts a container to make
     * four HTTP calls would be paying startup time for nothing.
     */
    public static void main(String[] args) {
        System.exit(new PulseForgeCli().run(args).code());
    }

    ExitCode run(String[] args) {
        if (args.length < 2 || !"run".equals(args[0])) {
            usage();
            return ExitCode.ERROR;
        }

        Path scenario = Path.of(args[1]);
        if (!Files.isReadable(scenario)) {
            System.err.println("cannot read scenario file: " + scenario);
            return ExitCode.ERROR;
        }

        String controlPlane = option(args, "--control-plane", DEFAULT_CONTROL_PLANE);
        Duration poll = duration(option(args, "--poll", "2s"), DEFAULT_POLL);
        Duration timeout = duration(option(args, "--timeout", "1h"), Duration.ofHours(1));

        try {
            return execute(new ControlPlaneClient(controlPlane), scenario, poll, timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("interrupted");
            return ExitCode.ERROR;
        } catch (Exception e) {
            System.err.println("error: " + e.getMessage());
            return ExitCode.ERROR;
        }
    }

    private ExitCode execute(
            ControlPlaneClient client, Path scenario, Duration poll, Duration timeout)
            throws Exception {

        JsonNode submitted = client.submitScenario(scenario);
        String scenarioId = submitted.get("id").asText();
        System.out.printf(
                "scenario '%s' -> %s (%d req/s for %s, ramp %s)%n",
                submitted.get("name").asText(),
                scenarioId,
                submitted.get("arrivalRate").asInt(),
                submitted.get("duration").asText(),
                submitted.get("rampUp").asText());

        JsonNode started = client.startRun(scenarioId);
        String runId = started.get("id").asText();
        System.out.printf(
                "run %s started on %d worker(s)%n",
                runId, started.get("expectedWorkers").asInt());

        String status = awaitTerminal(client, runId, poll, timeout);
        System.out.printf("run %s%n", status);

        JsonNode payload = client.results(runId);
        new ResultsPrinter(System.out).print(payload);

        return verdict(payload, status);
    }

    private String awaitTerminal(
            ControlPlaneClient client, String runId, Duration poll, Duration timeout)
            throws Exception {
        Instant deadline = Instant.now().plus(timeout);

        while (Instant.now().isBefore(deadline)) {
            String status = client.run(runId).get("status").asText();
            if (!"PENDING".equals(status) && !"RUNNING".equals(status)) {
                return status;
            }
            Thread.sleep(poll.toMillis());
        }
        throw new IllegalStateException("run did not finish within " + timeout);
    }

    /**
     * A degraded or aborted run outranks the assertions. Reporting PASS for a run that lost a
     * worker would be the exact failure this project is built to avoid — the assertions can hold
     * comfortably on a test that never fully ran.
     */
    private ExitCode verdict(JsonNode payload, String status) {
        if (!"COMPLETED".equals(status)) {
            return ExitCode.DEGRADED;
        }
        if (!payload.get("complete").asBoolean()) {
            return ExitCode.DEGRADED;
        }
        JsonNode verdict = payload.get("verdict");
        if (verdict == null || verdict.isNull()) {
            return ExitCode.PASS;
        }
        return verdict.get("passed").asBoolean() ? ExitCode.PASS : ExitCode.ASSERTION_FAILED;
    }

    private static String option(String[] args, String name, String fallback) {
        for (int i = 0; i < args.length - 1; i++) {
            if (name.equals(args[i])) {
                return args[i + 1];
            }
        }
        return fallback;
    }

    private static Duration duration(String value, Duration fallback) {
        try {
            return DurationSyntax.parse(value);
        } catch (IllegalArgumentException e) {
            return fallback;
        }
    }

    private static void usage() {
        System.err.println(
                """
                usage: pulseforge run <scenario.yaml> [options]

                options:
                  --control-plane URL   control plane base URL (default http://localhost:8080)
                  --poll DURATION       how often to check the run (default 2s)
                  --timeout DURATION    give up waiting after this long (default 1h)

                exit codes:
                  0  all assertions passed on a complete measurement
                  1  an assertion failed
                  2  the run was degraded: a worker was lost, or samples were dropped
                  3  usage error, unreachable control plane, or invalid scenario
                """);
    }
}
