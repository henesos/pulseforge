package io.pulseforge.controlplane.api;

import io.pulseforge.common.domain.Assertion;
import io.pulseforge.common.domain.Scenario;
import io.pulseforge.common.scenario.DurationSyntax;
import io.pulseforge.controlplane.persistence.ScenarioEntity;
import io.pulseforge.controlplane.service.ScenarioService;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Scenario CRUD.
 *
 * <p>Submission accepts raw YAML rather than JSON. Scenarios are written by hand and live in git
 * next to the service they exercise; asking an operator to JSON-encode one would be friction with
 * no benefit.
 */
@RestController
@RequestMapping("/api/v1/scenarios")
public class ScenarioController {

    private final ScenarioService scenarios;

    public ScenarioController(ScenarioService scenarios) {
        this.scenarios = scenarios;
    }

    @PostMapping(consumes = {"application/x-yaml", "text/yaml", MediaType.TEXT_PLAIN_VALUE})
    public ResponseEntity<ScenarioResponse> submit(@RequestBody String yaml) {
        ScenarioEntity saved = scenarios.save(yaml);
        return ResponseEntity.created(URI.create("/api/v1/scenarios/" + saved.getId()))
                .body(toResponse(saved));
    }

    @GetMapping
    public List<ScenarioResponse> list() {
        return scenarios.findAll().stream().map(this::toResponse).toList();
    }

    @GetMapping("/{id}")
    public ScenarioResponse get(@PathVariable UUID id) {
        return toResponse(scenarios.findById(id));
    }

    /** The stored YAML, byte for byte as it was submitted. */
    @GetMapping(value = "/{id}/definition", produces = MediaType.TEXT_PLAIN_VALUE)
    public String definition(@PathVariable UUID id) {
        return scenarios.findById(id).getDefinition();
    }

    private ScenarioResponse toResponse(ScenarioEntity entity) {
        Scenario parsed = scenarios.parse(entity);
        return new ScenarioResponse(
                entity.getId(),
                parsed.name(),
                parsed.target(),
                parsed.load().arrivalRate(),
                // Echoed in the scenario's own syntax rather than ISO-8601, so what comes back
                // matches what was submitted.
                DurationSyntax.format(parsed.load().duration()),
                DurationSyntax.format(parsed.load().rampUp()),
                parsed.steps().size(),
                parsed.assertions().stream().map(Assertion::describe).toList(),
                entity.getCreatedAt(),
                entity.getUpdatedAt());
    }
}
