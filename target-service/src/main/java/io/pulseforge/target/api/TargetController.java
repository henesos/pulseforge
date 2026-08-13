package io.pulseforge.target.api;

import io.pulseforge.target.config.FaultInjectionProperties;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * The system under test.
 *
 * <p>Three endpoints with distinct latency and failure shapes, so a run can demonstrate percentile
 * tails and error-rate assertions without pointing load at anyone else's infrastructure. The
 * service runs on virtual threads, so a slow endpoint parks its carrier instead of exhausting a
 * fixed request-thread pool — otherwise the target would be measuring its own queueing rather than
 * the injected delay.
 */
@RestController
@RequestMapping("/api")
public class TargetController {

    private static final Logger log = LoggerFactory.getLogger(TargetController.class);

    private final FaultInjectionProperties faults;
    private final AtomicLong servedRequests = new AtomicLong();

    public TargetController(FaultInjectionProperties faults) {
        this.faults = faults;
    }

    @GetMapping("/fast")
    public Map<String, Object> fast() {
        sleep(faults.fast().baseDelay(), faults.fast().jitter());
        return payload("fast");
    }

    /** Accepts POST as well, because the sample scenario submits a body to this path. */
    @RequestMapping(path = "/slow", method = {RequestMethod.GET, RequestMethod.POST})
    public Map<String, Object> slow(@RequestBody(required = false) String body) {
        sleep(faults.slow().baseDelay(), faults.slow().jitter());
        Map<String, Object> response = payload("slow");
        response.put("bodyBytes", body == null ? 0 : body.length());
        return response;
    }

    @GetMapping("/flaky")
    public ResponseEntity<Map<String, Object>> flaky() {
        sleep(faults.flaky().baseDelay(), Duration.ZERO);
        if (ThreadLocalRandom.current().nextDouble(100.0d) < faults.flaky().errorRatePercent()) {
            return ResponseEntity.status(503).body(payload("flaky-error"));
        }
        return ResponseEntity.ok(payload("flaky"));
    }

    private Map<String, Object> payload(String endpoint) {
        Map<String, Object> response = new HashMap<>();
        response.put("endpoint", endpoint);
        response.put("served", servedRequests.incrementAndGet());
        return response;
    }

    /**
     * Blocks the (virtual) request thread for {@code base} plus a uniform jitter. Jitter widens the
     * latency distribution so p50 and p99 do not collapse onto the same value.
     */
    private void sleep(Duration base, Duration jitter) {
        long millis = base.toMillis();
        if (!jitter.isZero()) {
            millis += ThreadLocalRandom.current().nextLong(jitter.toMillis() + 1);
        }
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("request interrupted while simulating {}ms of work", millis);
        }
    }
}
