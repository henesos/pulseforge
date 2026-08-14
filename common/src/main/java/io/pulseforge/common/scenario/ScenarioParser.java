package io.pulseforge.common.scenario;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.pulseforge.common.domain.Assertion;
import io.pulseforge.common.domain.HttpMethod;
import io.pulseforge.common.domain.LoadProfile;
import io.pulseforge.common.domain.Scenario;
import io.pulseforge.common.domain.ScenarioStep;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Turns scenario YAML into a validated {@link Scenario}.
 *
 * <p>Every failure mode is reported as a {@link InvalidScenarioException} naming the offending
 * field. A load test that silently runs a subtly different scenario than the one on disk is worse
 * than one that refuses to start.
 */
public final class ScenarioParser {

    // Unknown properties are a hard failure: `rampup:` for `rampUp:` would otherwise be dropped
    // without a word, and the run would apply no ramp at all while reporting success.
    private static final ObjectMapper YAML =
            new ObjectMapper(new YAMLFactory())
                    .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private ScenarioParser() {
        throw new AssertionError("utility class");
    }

    public static Scenario parse(String yaml) {
        ScenarioYaml raw;
        try {
            raw = YAML.readValue(yaml, ScenarioYaml.class);
        } catch (IOException e) {
            throw new InvalidScenarioException("scenario is not valid YAML: " + e.getMessage(), e);
        }
        if (raw == null) {
            throw new InvalidScenarioException("scenario is empty");
        }

        try {
            return new Scenario(
                    raw.name(),
                    normaliseTarget(raw.target()),
                    loadProfile(raw),
                    steps(raw),
                    assertions(raw));
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new InvalidScenarioException(e.getMessage(), e);
        }
    }

    private static String normaliseTarget(String target) {
        if (target == null || target.isBlank()) {
            throw new InvalidScenarioException("'target' is required");
        }
        String trimmed = target.trim();
        if (!trimmed.startsWith("http://") && !trimmed.startsWith("https://")) {
            throw new InvalidScenarioException(
                    "'target' must be an absolute http(s) URL, was: " + target);
        }
        // Steps contribute the leading slash; a trailing one here would produce '//api/fast'.
        return trimmed.endsWith("/") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
    }

    private static LoadProfile loadProfile(ScenarioYaml raw) {
        if (raw.duration() == null) {
            throw new InvalidScenarioException("'duration' is required");
        }
        if (raw.arrivalRate() == null) {
            throw new InvalidScenarioException("'arrivalRate' is required");
        }
        Duration duration = DurationSyntax.parse(raw.duration());
        Duration rampUp = raw.rampUp() == null ? Duration.ZERO : DurationSyntax.parse(raw.rampUp());
        return new LoadProfile(duration, rampUp, raw.arrivalRate());
    }

    private static List<ScenarioStep> steps(ScenarioYaml raw) {
        if (raw.steps() == null || raw.steps().isEmpty()) {
            throw new InvalidScenarioException("'steps' must declare at least one request");
        }
        List<ScenarioStep> steps = new ArrayList<>(raw.steps().size());
        Set<String> names = new HashSet<>();
        for (int i = 0; i < raw.steps().size(); i++) {
            ScenarioYaml.StepYaml step = raw.steps().get(i);
            ScenarioStep parsed;
            try {
                parsed =
                        new ScenarioStep(
                                step.name(),
                                method(step.method()),
                                step.path(),
                                step.weight() == null ? 1 : step.weight(),
                                step.body(),
                                step.headers());
            } catch (InvalidScenarioException e) {
                // Already carries its own message; only the index is missing.
                throw new InvalidScenarioException("steps[" + i + "]: " + e.getMessage(), e);
            } catch (IllegalArgumentException | NullPointerException e) {
                throw new InvalidScenarioException("steps[" + i + "]: " + e.getMessage(), e);
            }
            // Results are grouped by step name, so two steps sharing one would be reported as a
            // single merged row — a step would vanish from the breakdown with no warning.
            if (!names.add(parsed.name())) {
                throw new InvalidScenarioException(
                        "steps[" + i + "]: duplicate step name '" + parsed.name() + "'");
            }
            steps.add(parsed);
        }
        return steps;
    }

    private static HttpMethod method(String method) {
        if (method == null || method.isBlank()) {
            return HttpMethod.GET;
        }
        try {
            return HttpMethod.valueOf(method.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new InvalidScenarioException("unsupported HTTP method: " + method, e);
        }
    }

    private static List<Assertion> assertions(ScenarioYaml raw) {
        if (raw.assertions() == null) {
            return List.of();
        }
        List<Assertion> assertions = new ArrayList<>(raw.assertions().size());
        for (int i = 0; i < raw.assertions().size(); i++) {
            try {
                assertions.add(AssertionSyntax.parse(raw.assertions().get(i)));
            } catch (IllegalArgumentException e) {
                throw new InvalidScenarioException("assertions[" + i + "]: " + e.getMessage(), e);
            }
        }
        return assertions;
    }

    /** Raised when a scenario cannot be turned into something runnable. */
    public static class InvalidScenarioException extends RuntimeException {
        public InvalidScenarioException(String message) {
            super(message);
        }

        public InvalidScenarioException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
