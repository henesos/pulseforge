plugins {
    alias(libs.plugins.spring.boot)
}

dependencies {
    implementation(project(":common"))

    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")

    implementation(libs.jnats)
    implementation(libs.hdrhistogram)
    // Note: the `all` classifier of clickhouse-jdbc 0.6.5 is *not* an uber jar — it ships only the
    // `com.clickhouse.jdbc` package and fails at runtime with NoClassDefFoundError on
    // ClickHouseClient. The plain artifact plus an explicit transport is the working combination.
    implementation(libs.clickhouse.jdbc)
    runtimeOnly(libs.clickhouse.http.client)
    // The driver picks these up reflectively: httpclient5 for connection pooling (it silently
    // degrades to HttpURLConnection without it), lz4 for the wire compression it enables by
    // default on inserts.
    runtimeOnly("org.apache.httpcomponents.client5:httpclient5")
    runtimeOnly(libs.lz4.java)

    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:clickhouse")
}
