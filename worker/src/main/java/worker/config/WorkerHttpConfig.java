package worker.config;

import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

/**
 * HTTP client configuration for worker outbound calls.
 */
@Configuration
public class WorkerHttpConfig {
    @Bean
    public RestTemplate workerRestTemplate(RestTemplateBuilder builder, WorkerProperties properties) {
        return builder
                .setConnectTimeout(Duration.ofMillis(properties.getHttpConnectTimeoutMs()))
                .setReadTimeout(Duration.ofMillis(properties.getHttpReadTimeoutMs()))
                .build();
    }
}
