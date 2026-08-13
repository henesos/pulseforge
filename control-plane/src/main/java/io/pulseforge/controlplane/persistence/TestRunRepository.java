package io.pulseforge.controlplane.persistence;

import io.pulseforge.common.domain.RunStatus;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TestRunRepository extends JpaRepository<TestRunEntity, UUID> {

    List<TestRunEntity> findByStatusIn(List<RunStatus> statuses);

    List<TestRunEntity> findByScenarioIdOrderByCreatedAtDesc(UUID scenarioId);
}
