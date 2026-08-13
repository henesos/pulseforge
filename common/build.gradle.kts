// Shared domain model, wire protocol and the NATS plumbing every service repeats.
//
// Note the split: `io.pulseforge.common.domain` is plain Java with no framework annotations and is
// unit-testable without a container, while `io.pulseforge.common.nats` is explicitly Spring-aware
// infrastructure that services opt into via @Import.
plugins {
    `java-library`
}

dependencies {
    api("com.fasterxml.jackson.core:jackson-databind")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    api("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml")

    api("org.springframework.boot:spring-boot-starter")
    api("org.springframework.boot:spring-boot-starter-actuator")
    api(libs.jnats)
    // Both the worker (produces) and the ingestor (merges) need the same histogram encoding.
    api(libs.hdrhistogram)
}
