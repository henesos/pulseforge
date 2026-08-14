<!-- bmad:context -->
<!-- Verified 2026-08-14 against ec828c9. Managed by bmad-project-context; edits inside this block are replaced on refresh. Keep anything you want preserved outside the markers. -->

## pulseforge

Distributed HTTP load-testing engine: a control plane shards a scenario across a worker fleet,
workers generate open-loop load and ship HdrHistogram snapshots, and percentiles are merged in
ClickHouse. Java 21, Gradle multi-module, Spring Boot, Docker Compose. The design rationale — and
every number quoted alongside it — lives in `README.md`.

## Where things are

- Run lifecycle, and the decision to finish a run before marking it DEGRADED:
  `control-plane/src/main/java/io/pulseforge/controlplane/service/RunWatchdog.java`
- Schema changes: `control-plane/src/main/resources/db/migration/`

## Running and verifying

- `./gradlew` needs a **host JDK 21**. No toolchain download repository is configured, so an older
  JDK fails with `No locally installed toolchains match`. Either build through the container —
  README § Development, including the one-time `chown` of the `pulseforge-gradle` volume — or point
  Gradle at an unpacked JDK: `-Porg.gradle.java.installations.paths=/path/to/jdk-21`.
- `integrationTest` needs a Docker daemon the JVM can reach on published ports. On WSL, start Docker
  Desktop from the Windows side and it works; the whole suite runs locally. Set
  `TESTCONTAINERS_RYUK_DISABLED=true` as CI does — the resource reaper's socket handshake is a known
  way for container startup to hang here with no output.
- Before hand-rolling a behavioural check, read the `e2e` job in `.github/workflows/ci.yml` — it
  already brings the stack up, runs the CLI against it and gates on the exit code. Note the job
  name is `e2e`: a `grep '^  [a-z-]*:'` over the workflow will not list it, and concluding it does
  not exist from that is a mistake this repository has already had made in it once.

## Conventions that differ from defaults

- A new Gradle module must be added to **both** COPY lists in `docker/Dockerfile`. It names every
  module explicitly, and a missing one fails only at image build.
- New scenario YAML keys must be declared in `ScenarioYaml`, which rejects unknown properties — an
  undeclared key is a 400, not a silently ignored default.
- Assertions answer p50, p95 and p99 only; `Assertion`'s constructor refuses anything else at
  submission time.
- Numbers quoted in `README.md` are measured from real runs, not illustrative. Re-run and paste
  actual output rather than editing a figure.
- `pulseforge.ingest_losses` is created by the ingestor at startup, not by
  `docker/clickhouse/init/01-schema.sql`. That script runs only when a ClickHouse volume is first
  created, so a table added there never appears in a deployment that already has data.
- A test is not finished until it has been mutation-checked: break the behaviour it claims to hold
  and confirm that test — and only that test — fails. Every suite added since PR #1 was verified
  this way, and it has caught tests that passed for the wrong reason.

## Known pitfalls

- A Spring test in `control-plane` loads NATS through the `@Import` on `ControlPlaneApplication`
  even under `@DataJpaTest`, and the client retries forever — mock the `Connection`, or the test
  hangs with no output and no failure.
- Testcontainers' ClickHouse defaults (`default`, empty password) are rejected by
  clickhouse-server 24.8; set credentials explicitly or the container never starts.

<!-- /bmad:context -->
