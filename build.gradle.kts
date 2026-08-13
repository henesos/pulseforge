import io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension
import org.springframework.boot.gradle.plugin.SpringBootPlugin

plugins {
    java
    alias(libs.plugins.spring.boot) apply false
    alias(libs.plugins.spring.dependency.management) apply false
}

allprojects {
    group = "io.pulseforge"
    version = "0.1.0"
}

// The `libs` accessor is not registered on subprojects while `subprojects {}` is being
// evaluated, so the catalog values are captured here in root scope.
val javaToolchainVersion = libs.versions.java.get()
val testcontainersVersion = libs.versions.testcontainers.get()

subprojects {
    apply(plugin = "java")
    apply(plugin = "io.spring.dependency-management")

    repositories {
        mavenCentral()
    }

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(javaToolchainVersion))
        }
    }

    the<DependencyManagementExtension>().apply {
        imports {
            mavenBom(SpringBootPlugin.BOM_COORDINATES)
            mavenBom("org.testcontainers:testcontainers-bom:$testcontainersVersion")
        }
    }

    dependencies {
        "testImplementation"("org.springframework.boot:spring-boot-starter-test")
        "testRuntimeOnly"("org.junit.platform:junit-platform-launcher")
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        // Keeps constructor parameter names available for Jackson record binding.
        options.compilerArgs.add("-parameters")
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
        }
    }

    // Integration tests (*IT) need a Docker daemon for Testcontainers, so they are kept out of the
    // default `test` task: `gradle build` must stay runnable anywhere, and a fast unit-test loop is
    // worth more than the convenience of one command.
    tasks.named<Test>("test") {
        exclude("**/*IT.class")
    }

    // Captured here: inside the task-configuration block the receiver is the Test task, whose
    // extension container has no SourceSetContainer.
    val testSourceSet = the<SourceSetContainer>()["test"]

    tasks.register<Test>("integrationTest") {
        description = "Runs Testcontainers-backed integration tests."
        group = "verification"
        testClassesDirs = testSourceSet.output.classesDirs
        classpath = testSourceSet.runtimeClasspath
        include("**/*IT.class")
        shouldRunAfter(tasks.named("test"))
    }
}
