package io.pulseforge.common.domain;

import java.util.Map;
import java.util.Objects;

/**
 * One weighted request definition inside a scenario.
 *
 * <p>{@code weight} is relative, not a percentage: weights are normalised against the sum of all
 * steps, so a scenario stays valid when a step is added without rebalancing the others.
 */
public record ScenarioStep(
        String name,
        HttpMethod method,
        String path,
        int weight,
        String body,
        Map<String, String> headers) {

    public ScenarioStep {
        Objects.requireNonNull(method, "method must not be null");
        Objects.requireNonNull(path, "path must not be null");
        if (weight <= 0) {
            throw new IllegalArgumentException("step weight must be positive, was " + weight);
        }
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("step path must start with '/', was " + path);
        }
        name = (name == null || name.isBlank()) ? method + " " + path : name;
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }
}
