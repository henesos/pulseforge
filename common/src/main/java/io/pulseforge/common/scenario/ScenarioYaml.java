package io.pulseforge.common.scenario;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;

/**
 * Literal shape of the scenario file, before any validation or unit conversion.
 *
 * <p>This exists so the transport format can carry raw strings ({@code "120s"}, {@code "p95 <
 * 250ms"}) while the domain model carries real {@link java.time.Duration} and
 * {@link io.pulseforge.common.domain.Assertion} values. Keeping them separate means a YAML change
 * cannot reach into the domain, and the domain stays free of Jackson annotations.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
record ScenarioYaml(
        String name,
        String target,
        String duration,
        String rampUp,
        Integer arrivalRate,
        List<StepYaml> steps,
        List<String> assertions) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    record StepYaml(
            String name,
            String method,
            String path,
            Integer weight,
            String body,
            Map<String, String> headers) {}
}
