package io.pulseforge.common.domain;

import java.util.List;
import java.util.Objects;

/**
 * A complete, executable load definition: what to call, how hard, and what "good" means.
 *
 * <p>This is a plain value object on purpose. It carries no JPA, Jackson or Spring annotations so
 * that persistence and transport concerns stay in their own layers and the domain can be unit
 * tested without a container.
 */
public record Scenario(
        String name, String target, LoadProfile load, List<ScenarioStep> steps,
        List<Assertion> assertions) {

    public Scenario {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("scenario name must not be blank");
        }
        if (target == null || target.isBlank()) {
            throw new IllegalArgumentException("scenario target must not be blank");
        }
        Objects.requireNonNull(load, "load profile must not be null");
        if (steps == null || steps.isEmpty()) {
            throw new IllegalArgumentException("scenario must declare at least one step");
        }
        steps = List.copyOf(steps);
        assertions = assertions == null ? List.of() : List.copyOf(assertions);
    }

    public int totalWeight() {
        return steps.stream().mapToInt(ScenarioStep::weight).sum();
    }
}
