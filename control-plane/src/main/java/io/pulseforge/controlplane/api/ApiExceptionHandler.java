package io.pulseforge.controlplane.api;

import io.pulseforge.common.scenario.ScenarioParser;
import io.pulseforge.controlplane.results.AssertionEvaluator;
import io.pulseforge.controlplane.service.RunService;
import io.pulseforge.controlplane.service.ScenarioService;
import io.pulseforge.controlplane.service.WorkerRegistry;
import java.time.Instant;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Maps domain failures to HTTP.
 *
 * <p>An invalid scenario is a 400 with the offending field named, not a 500 stack trace: the
 * operator who submitted it is the one who can fix it.
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(ScenarioParser.InvalidScenarioException.class)
    public ProblemDetail onInvalidScenario(ScenarioParser.InvalidScenarioException e) {
        return problem(HttpStatus.BAD_REQUEST, "Invalid scenario", e.getMessage());
    }

    @ExceptionHandler(AssertionEvaluator.UnsupportedPercentileException.class)
    public ProblemDetail onUnsupportedPercentile(
            AssertionEvaluator.UnsupportedPercentileException e) {
        return problem(HttpStatus.BAD_REQUEST, "Unsupported assertion", e.getMessage());
    }

    @ExceptionHandler(ScenarioService.ScenarioNotFoundException.class)
    public ProblemDetail onScenarioNotFound(ScenarioService.ScenarioNotFoundException e) {
        return problem(HttpStatus.NOT_FOUND, "Scenario not found", e.getMessage());
    }

    @ExceptionHandler(RunService.RunNotFoundException.class)
    public ProblemDetail onRunNotFound(RunService.RunNotFoundException e) {
        return problem(HttpStatus.NOT_FOUND, "Run not found", e.getMessage());
    }

    @ExceptionHandler(RunService.NoWorkersAvailableException.class)
    public ProblemDetail onNoWorkers(RunService.NoWorkersAvailableException e) {
        return problem(HttpStatus.CONFLICT, "No workers available", e.getMessage());
    }

    /**
     * Distinct from "no workers available" on purpose: an unreadable coordination store is an
     * infrastructure fault, and reporting it as an empty fleet sends the operator to the wrong box.
     */
    @ExceptionHandler(WorkerRegistry.RegistryUnavailableException.class)
    public ProblemDetail onRegistryUnavailable(WorkerRegistry.RegistryUnavailableException e) {
        return problem(
                HttpStatus.SERVICE_UNAVAILABLE, "Coordination store unavailable", e.getMessage());
    }

    private static ProblemDetail problem(HttpStatus status, String title, String detail) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setTitle(title);
        problem.setProperty("timestamp", Instant.now());
        return problem;
    }
}
