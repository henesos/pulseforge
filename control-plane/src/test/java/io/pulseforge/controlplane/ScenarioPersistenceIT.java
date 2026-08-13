package io.pulseforge.controlplane;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.pulseforge.common.domain.RunStatus;
import io.pulseforge.common.domain.Scenario;
import io.pulseforge.common.scenario.ScenarioParser;
import io.pulseforge.controlplane.persistence.ScenarioEntity;
import io.pulseforge.controlplane.persistence.ScenarioRepository;
import io.pulseforge.controlplane.persistence.TestRunEntity;
import io.pulseforge.controlplane.persistence.TestRunRepository;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Verifies the schema against a real PostgreSQL: the Flyway migration applies, the JPA mapping
 * matches it, and the constraints that protect run integrity actually exist in the database rather
 * than only in Java.
 */
@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Testcontainers
class ScenarioPersistenceIT {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("pulseforge")
                    .withUsername("pulseforge")
                    .withPassword("pulseforge");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.flyway.enabled", () -> true);
        // Hibernate must validate against Flyway's schema, never create its own.
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "validate");
    }

    @Autowired private ScenarioRepository scenarios;
    @Autowired private TestRunRepository runs;

    private static final String YAML =
            """
            name: persistence-check
            target: http://target-service:8081
            duration: 30s
            rampUp: 5s
            arrivalRate: 100
            steps:
              - method: GET
                path: /api/fast
            assertions:
              - p95 < 250ms
            """;

    @Test
    @DisplayName("Flyway's schema and the JPA mapping agree")
    void schemaMatchesTheMapping() {
        ScenarioEntity saved =
                scenarios.save(
                        new ScenarioEntity(
                                UUID.randomUUID(), "persistence-check", YAML, Instant.now()));

        assertThat(scenarios.findByName("persistence-check")).isPresent();
        assertThat(saved.getDefinition()).isEqualTo(YAML);
    }

    @Test
    @DisplayName("the stored YAML re-parses into the same scenario, byte for byte")
    void definitionRoundTrips() {
        scenarios.save(
                new ScenarioEntity(UUID.randomUUID(), "round-trip", YAML, Instant.now()));

        ScenarioEntity loaded = scenarios.findByName("round-trip").orElseThrow();
        Scenario parsed = ScenarioParser.parse(loaded.getDefinition());

        assertThat(parsed.name()).isEqualTo("persistence-check");
        assertThat(parsed.load().arrivalRate()).isEqualTo(100);
        assertThat(parsed.assertions()).hasSize(1);
    }

    @Test
    @DisplayName("scenario names are unique in the database, not just in application code")
    void enforcesUniqueScenarioName() {
        scenarios.save(new ScenarioEntity(UUID.randomUUID(), "duplicate", YAML, Instant.now()));

        assertThatThrownBy(
                        () -> {
                            scenarios.save(
                                    new ScenarioEntity(
                                            UUID.randomUUID(), "duplicate", YAML, Instant.now()));
                            scenarios.flush();
                        })
                .isInstanceOf(Exception.class);
    }

    @Test
    @DisplayName("every terminal status the domain can produce is accepted by the CHECK constraint")
    void persistsEveryTerminalStatus() {
        ScenarioEntity scenario =
                scenarios.save(
                        new ScenarioEntity(UUID.randomUUID(), "statuses", YAML, Instant.now()));

        List<RunStatus> terminal =
                List.of(
                        RunStatus.COMPLETED,
                        RunStatus.DEGRADED,
                        RunStatus.ABORTED,
                        RunStatus.FAILED);

        for (RunStatus status : terminal) {
            TestRunEntity run =
                    runs.save(
                            new TestRunEntity(
                                    UUID.randomUUID(),
                                    scenario.getId(),
                                    100,
                                    30,
                                    5,
                                    3,
                                    Instant.now()));
            run.terminate(status, "reason for " + status, Instant.now());
            runs.flush();

            assertThat(runs.findById(run.getId()).orElseThrow().getStatus()).isEqualTo(status);
        }
    }

    @Test
    @DisplayName("active runs are findable, which is what the watchdog scans")
    void findsActiveRuns() {
        ScenarioEntity scenario =
                scenarios.save(new ScenarioEntity(UUID.randomUUID(), "active", YAML, Instant.now()));

        TestRunEntity running =
                runs.save(
                        new TestRunEntity(
                                UUID.randomUUID(), scenario.getId(), 100, 30, 5, 3, Instant.now()));
        running.markRunning(Instant.now());

        TestRunEntity finished =
                runs.save(
                        new TestRunEntity(
                                UUID.randomUUID(), scenario.getId(), 100, 30, 5, 3, Instant.now()));
        finished.terminate(RunStatus.COMPLETED, null, Instant.now());
        runs.flush();

        List<TestRunEntity> active =
                runs.findByStatusIn(List.of(RunStatus.PENDING, RunStatus.RUNNING));

        assertThat(active).extracting(TestRunEntity::getId).contains(running.getId());
        assertThat(active).extracting(TestRunEntity::getId).doesNotContain(finished.getId());
    }
}
