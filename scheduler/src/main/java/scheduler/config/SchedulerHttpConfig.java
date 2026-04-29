package scheduler.config;

import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;

/**
 * HTTP client configuration for scheduler direct worker calls.
 */
@Configuration
public class SchedulerHttpConfig {
    @Bean
    public RestTemplate schedulerRestTemplate(RestTemplateBuilder builder, ScheduleJobProperties properties) {
        return builder
                .setConnectTimeout(Duration.ofMillis(properties.getHttpConnectTimeoutMs()))
                .setReadTimeout(Duration.ofMillis(properties.getHttpReadTimeoutMs()))
                .build();
    }
}
