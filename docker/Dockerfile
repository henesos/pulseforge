# syntax=docker/dockerfile:1.7
#
# One Dockerfile for every service. The module to package is selected with
# `--build-arg MODULE=<gradle-module>`, which keeps five near-identical Dockerfiles from drifting
# apart. Build and runtime JDKs live only in these layers: nothing has to be installed on the host.
#
# The COPY lists below name each module explicitly. A new Gradle module must be added to both, or
# `--build-arg MODULE=<it>` fails with an unhelpful Gradle error about an unknown project.

FROM gradle:8.11-jdk21 AS build
WORKDIR /workspace

# Build scripts first: dependency resolution is cached independently of source changes.
COPY settings.gradle.kts build.gradle.kts ./
COPY gradle/libs.versions.toml gradle/libs.versions.toml
COPY common/build.gradle.kts common/build.gradle.kts
COPY control-plane/build.gradle.kts control-plane/build.gradle.kts
COPY load-worker/build.gradle.kts load-worker/build.gradle.kts
COPY metrics-ingestor/build.gradle.kts metrics-ingestor/build.gradle.kts
COPY target-service/build.gradle.kts target-service/build.gradle.kts
COPY cli/build.gradle.kts cli/build.gradle.kts

COPY common common
COPY control-plane control-plane
COPY load-worker load-worker
COPY metrics-ingestor metrics-ingestor
COPY target-service target-service
COPY cli cli

ARG MODULE
RUN --mount=type=cache,target=/home/gradle/.gradle \
    test -n "$MODULE" || { echo "MODULE build-arg is required"; exit 1; } && \
    gradle --no-daemon ":${MODULE}:bootJar" -x test && \
    cp "${MODULE}/build/libs/${MODULE}-"*.jar /workspace/app.jar


FROM eclipse-temurin:21-jre-alpine AS runtime

# wget is busybox-provided and is what the compose healthchecks call.
RUN addgroup -S pulseforge && adduser -S pulseforge -G pulseforge
WORKDIR /app
COPY --from=build /workspace/app.jar app.jar
USER pulseforge

ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0"
# `"$@"` with the `--` placeholder is what forwards `docker run <image> run scenario.yaml` to the
# jar. Without it `sh -c` binds those words to $0/$1 and drops them, which the services never
# noticed because they take no arguments — but the CLI is nothing but arguments.
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar \"$@\"", "--"]
