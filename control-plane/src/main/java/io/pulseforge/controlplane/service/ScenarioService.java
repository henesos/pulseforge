package io.pulseforge.controlplane.service;

import io.pulseforge.common.domain.Scenario;
import io.pulseforge.common.scenario.ScenarioParser;
import io.pulseforge.controlplane.persistence.ScenarioEntity;
import io.pulseforge.controlplane.persistence.ScenarioRepository;
import java.time.Clock;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Stores and retrieves scenarios.
 *
 * <p>Submitted YAML is parsed before it is persisted, so an invalid scenario is rejected at
 * submission time rather than at 3am when someone triggers the run. The YAML itself is what gets
 * stored — see {@link ScenarioEntity}.
 */
@Service
public class ScenarioService {

    private final ScenarioRepository repository;
    private final Clock clock;

    public ScenarioService(ScenarioRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }

    /** Creates the scenario, or replaces the definition if one with the same name exists. */
    @Transactional
    public ScenarioEntity save(String yaml) {
        Scenario parsed = ScenarioParser.parse(yaml);

        return repository
                .findByName(parsed.name())
                .map(
                        existing -> {
                            existing.updateDefinition(yaml, clock.instant());
                            return existing;
                        })
                .orElseGet(
                        () ->
                                repository.save(
                                        new ScenarioEntity(
                                                UUID.randomUUID(),
                                                parsed.name(),
                                                yaml,
                                                clock.instant())));
    }

    @Transactional(readOnly = true)
    public List<ScenarioEntity> findAll() {
        return repository.findAll();
    }

    @Transactional(readOnly = true)
    public ScenarioEntity findById(UUID id) {
        return repository.findById(id).orElseThrow(() -> new ScenarioNotFoundException(id));
    }

    /** Re-parses a stored definition. Kept in one place so parsing rules cannot diverge. */
    public Scenario parse(ScenarioEntity entity) {
        return ScenarioParser.parse(entity.getDefinition());
    }

    public static class ScenarioNotFoundException extends RuntimeException {
        public ScenarioNotFoundException(UUID id) {
            super("no scenario with id " + id);
        }
    }
}
