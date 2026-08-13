package io.pulseforge.controlplane.persistence;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ScenarioRepository extends JpaRepository<ScenarioEntity, UUID> {

    Optional<ScenarioEntity> findByName(String name);
}
