package io.pulseforge.cli;

import com.fasterxml.jackson.databind.JsonNode;
import io.pulseforge.common.serde.JsonCodec;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * Thin HTTP client for the control plane's public API.
 *
 * <p>Speaks the same REST endpoints a human would curl, rather than sharing internal types. That
 * keeps the CLI honest: if it works, the documented API works.
 */
public class ControlPlaneClient {

    private final HttpClient http =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private final String baseUrl;

    public ControlPlaneClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }

    public JsonNode submitScenario(Path scenarioFile) throws IOException, InterruptedException {
        String yaml = Files.readString(scenarioFile, StandardCharsets.UTF_8);
        HttpRequest request =
                HttpRequest.newBuilder(URI.create(baseUrl + "/api/v1/scenarios"))
                        .header("Content-Type", "application/x-yaml")
                        .POST(HttpRequest.BodyPublishers.ofString(yaml))
                        .build();
        return send(request, "submit scenario");
    }

    public JsonNode startRun(String scenarioId) throws IOException, InterruptedException {
        HttpRequest request =
                HttpRequest.newBuilder(
                                URI.create(baseUrl + "/api/v1/runs?scenarioId=" + scenarioId))
                        .POST(HttpRequest.BodyPublishers.noBody())
                        .build();
        return send(request, "start run");
    }

    public JsonNode run(String runId) throws IOException, InterruptedException {
        return send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/api/v1/runs/" + runId)).GET().build(),
                "read run");
    }

    public JsonNode results(String runId) throws IOException, InterruptedException {
        return send(
                HttpRequest.newBuilder(URI.create(baseUrl + "/api/v1/runs/" + runId + "/results"))
                        .GET()
                        .build(),
                "read results");
    }

    private JsonNode send(HttpRequest request, String what)
            throws IOException, InterruptedException {
        HttpResponse<String> response =
                http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

        // 202 is a normal answer from the results endpoint while a run is still in progress.
        if (response.statusCode() >= 300) {
            throw new ControlPlaneException(
                    "failed to %s: HTTP %d %s".formatted(what, response.statusCode(), response.body()));
        }
        return JsonCodec.mapper().readTree(response.body());
    }

    /** Raised for any non-success response, carrying the server's own explanation. */
    public static class ControlPlaneException extends RuntimeException {
        public ControlPlaneException(String message) {
            super(message);
        }
    }
}
