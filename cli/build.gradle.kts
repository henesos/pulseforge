// The CI entry point: runs a scenario and reflects the verdict in the process exit code.
//
// Packaged as a Spring Boot jar purely so it shares the project's single Dockerfile and build
// pipeline; it starts no web server and no application context beyond what a CommandLineRunner
// needs.
plugins {
    alias(libs.plugins.spring.boot)
}

dependencies {
    implementation(project(":common"))
    implementation("org.springframework.boot:spring-boot-starter")
}
