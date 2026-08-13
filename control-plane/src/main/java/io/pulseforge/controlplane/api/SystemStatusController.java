package io.pulseforge.controlplane.api;

import io.pulseforge.controlplane.service.WorkerRegistry;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.CompositeHealth;
import org.springframework.boot.actuate.health.HealthComponent;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.health.Status;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * One call that answers "is this deployment ready to run a load test?".
 *
 * <p>Actuator already knows the answer; this endpoint reshapes it into a stable public contract so
 * the API is not coupled to the actuator response format, and returns 503 when anything is down so
 * a smoke test can rely on the HTTP status alone.
 */
@RestController
@RequestMapping("/api/v1/system")
public class SystemStatusController {

    private final HealthEndpoint healthEndpoint;
    private final WorkerRegistry workers;
    private final Clock clock;
    private final String version;

    public SystemStatusController(
            HealthEndpoint healthEndpoint,
            WorkerRegistry workers,
            Clock clock,
            @Value("${pulseforge.version:0.1.0}") String version) {
        this.healthEndpoint = healthEndpoint;
        this.workers = workers;
        this.clock = clock;
        this.version = version;
    }

    @GetMapping("/status")
    public ResponseEntity<SystemStatusResponse> status() {
        HealthComponent health = healthEndpoint.health();

        Map<String, String> components = new LinkedHashMap<>();
        if (health instanceof CompositeHealth composite) {
            composite
                    .getComponents()
                    .forEach((name, component) -> components.put(name, component.getStatus().getCode()));
        }

        SystemStatusResponse body =
                new SystemStatusResponse(
                        health.getStatus().getCode(),
                        version,
                        clock.instant(),
                        workers.liveWorkerCount(),
                        components);

        HttpStatus httpStatus =
                Status.UP.equals(health.getStatus())
                        ? HttpStatus.OK
                        : HttpStatus.SERVICE_UNAVAILABLE;
        return ResponseEntity.status(httpStatus).body(body);
    }
}
