package io.pulseforge.controlplane.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * Persistence view of a scenario.
 *
 * <p>The parsed {@link io.pulseforge.common.domain.Scenario} is deliberately <em>not</em> mapped
 * field by field. The YAML is stored verbatim and re-parsed on read, so a run can be reproduced
 * byte-for-byte later and the domain model stays free to evolve without a migration for every
 * field.
 */
@Entity
@Table(name = "scenarios")
public class ScenarioEntity {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "name", nullable = false, unique = true, length = 200)
    private String name;

    @Column(name = "definition", nullable = false, columnDefinition = "text")
    private String definition;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected ScenarioEntity() {
        // Required by JPA.
    }

    public ScenarioEntity(UUID id, String name, String definition, Instant now) {
        this.id = id;
        this.name = name;
        this.definition = definition;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public void updateDefinition(String definition, Instant now) {
        this.definition = definition;
        this.updatedAt = now;
    }

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDefinition() {
        return definition;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
