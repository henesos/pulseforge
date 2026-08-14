plugins {
    alias(libs.plugins.spring.boot)
}

dependencies {
    implementation(project(":common"))

    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-data-redis")

    implementation(libs.jnats)
    implementation(libs.hdrhistogram)

    // The shard claim's guarantee is Redis's atomicity, not the worker's code, so it is verified
    // against a real server rather than a stub.
    testImplementation("org.testcontainers:junit-jupiter")
}
