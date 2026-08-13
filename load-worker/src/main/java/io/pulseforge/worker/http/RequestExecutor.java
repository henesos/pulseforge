package io.pulseforge.worker.http;

import io.pulseforge.common.domain.ScenarioStep;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Issues one scenario step against the target.
 *
 * <p>Requests are sent asynchronously: the generator thread must never wait on a response, or the
 * arrival schedule would drift with the target's latency — the closed-loop behaviour this project
 * exists to avoid.
 */
public class RequestExecutor {

    private final HttpClient httpClient;
    private final String targetBaseUrl;
    private final Duration requestTimeout;

    public RequestExecutor(HttpClient httpClient, String targetBaseUrl, Duration requestTimeout) {
        this.httpClient = httpClient;
        this.targetBaseUrl = targetBaseUrl;
        this.requestTimeout = requestTimeout;
    }

    public CompletableFuture<HttpResponse<Void>> execute(ScenarioStep step) {
        HttpRequest.Builder builder =
                HttpRequest.newBuilder(URI.create(targetBaseUrl + step.path()))
                        .timeout(requestTimeout);

        for (Map.Entry<String, String> header : step.headers().entrySet()) {
            builder.header(header.getKey(), header.getValue());
        }

        HttpRequest.BodyPublisher body =
                step.body() == null
                        ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(step.body());
        builder.method(step.method().name(), body);

        // Response bodies are discarded: this measures the target, and buffering payloads would
        // make the generator's own allocation rate part of the measurement.
        return httpClient.sendAsync(builder.build(), HttpResponse.BodyHandlers.discarding());
    }
}
