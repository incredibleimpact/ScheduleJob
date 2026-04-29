package preheater.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the standalone preheater.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "schedulejob.preheater")
public class PreheaterProperties {
    private long preheatIntervalMs = 200L;
}
