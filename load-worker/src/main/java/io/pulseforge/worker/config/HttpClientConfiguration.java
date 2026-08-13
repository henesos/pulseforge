package io.pulseforge.worker.config;

import java.net.http.HttpClient;
import java.util.concurrent.Executors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The HTTP client used to generate load.
 *
 * <p>Backed by virtual threads: an open-loop generator can have tens of thousands of requests in
 * flight against a slow target, and a platform-thread pool that size would spend more time
 * context-switching than measuring. Redirects are disabled so a 302 is recorded as what the target
 * actually returned rather than silently followed into a second, unmeasured request.
 */
@Configuration
public class HttpClientConfiguration {

    @Bean
    public HttpClient loadGeneratorHttpClient(WorkerProperties properties) {
        return HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .connectTimeout(properties.connectTimeout())
                .followRedirects(HttpClient.Redirect.NEVER)
                .version(HttpClient.Version.HTTP_1_1)
                .build();
    }
}
