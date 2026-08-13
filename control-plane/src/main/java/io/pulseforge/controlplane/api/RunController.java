package io.pulseforge.controlplane.api;

import io.pulseforge.common.domain.Scenario;
import io.pulseforge.controlplane.persistence.TestRunEntity;
import io.pulseforge.controlplane.results.AssertionEvaluator;
import io.pulseforge.controlplane.results.RunResultService;
import io.pulseforge.controlplane.results.RunResults;
import io.pulseforge.controlplane.service.RunService;
import io.pulseforge.controlplane.service.ScenarioService;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Run lifecycle and results. */
@RestController
@RequestMapping("/api/v1/runs")
public class RunController {

    private final RunService runs;
    private final ScenarioService scenarios;
    private final RunResultService results;
    private final AssertionEvaluator assertions;

    public RunController(
            RunService runs,
            ScenarioService scenarios,
            RunResultService results,
            AssertionEvaluator assertions) {
        this.runs = runs;
        this.scenarios = scenarios;
        this.results = results;
        this.assertions = assertions;
    }

    @PostMapping
    public ResponseEntity<RunResponse> start(@RequestParam("scenarioId") UUID scenarioId) {
        TestRunEntity run = runs.start(scenarioId);
        return ResponseEntity.created(URI.create("/api/v1/runs/" + run.getId()))
                .body(toResponse(run));
    }

    @GetMapping
    public List<RunResponse> list() {
        return runs.findAll().stream().map(RunController::toResponse).toList();
    }

    @GetMapping("/{id}")
    public RunResponse get(@PathVariable UUID id) {
        return toResponse(runs.findById(id));
    }

    @DeleteMapping("/{id}")
    public RunResponse abort(@PathVariable UUID id) {
        return toResponse(runs.abort(id));
    }

    /**
     * Measured results plus the PASS/FAIL verdict.
     *
     * <p>Returns 202 while the run is still in progress: the numbers exist but describe an
     * unfinished experiment, and returning them as final would invite someone to gate a deploy on
     * a half-completed test.
     */
    @GetMapping("/{id}/results")
    public ResponseEntity<RunResultsResponse> results(@PathVariable UUID id) {
        TestRunEntity run = runs.findById(id);
        RunResults measured = results.resultsFor(run);

        Scenario scenario = scenarios.parse(scenarios.findById(run.getScenarioId()));
        AssertionEvaluator.Verdict verdict =
                scenario.assertions().isEmpty()
                        ? null
                        : assertions.evaluate(scenario.assertions(), measured);

        RunResultsResponse body =
                new RunResultsResponse(measured, verdict, measured.isComplete());

        return run.getStatus().isTerminal()
                ? ResponseEntity.ok(body)
                : ResponseEntity.accepted().body(body);
    }

    private static RunResponse toResponse(TestRunEntity run) {
        return new RunResponse(
                run.getId(),
                run.getScenarioId(),
                run.getStatus(),
                run.getStatusReason(),
                run.getArrivalRate(),
                run.getDurationSeconds(),
                run.getRampUpSeconds(),
                run.getExpectedWorkers(),
                run.getFinishedWorkers(),
                run.getCreatedAt(),
                run.getStartedAt(),
                run.getFinishedAt());
    }
}
