package worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;
import worker.config.WorkerProperties;

/**
 * Worker service entry point.
 */
@EnableScheduling
@SpringBootApplication
@EnableConfigurationProperties(WorkerProperties.class)
public class WorkerMainApplication {
    public static void main(String[] args) {
        SpringApplication.run(WorkerMainApplication.class, args);
    }
}
