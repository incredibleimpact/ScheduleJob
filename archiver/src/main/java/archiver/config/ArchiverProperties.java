package archiver.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for standalone archiving.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "schedulejob.archiver")
public class ArchiverProperties {
    private int archiveAfterMinutes = 1440;
    private String archiveDir = "data/cold-job";
    private String archiveCron = "0 0 2 * * *";
}
