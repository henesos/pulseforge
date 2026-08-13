// Fault-injecting HTTP target. Keeps the whole project self-contained:
// load is never pointed at a third-party host.
plugins {
    alias(libs.plugins.spring.boot)
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-validation")
}
