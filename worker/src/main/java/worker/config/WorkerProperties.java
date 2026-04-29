package worker.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for a worker host.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "schedulejob.worker")
public class WorkerProperties {
    private long heartbeatIntervalMs = 1000L;
    private long executeDelayMinMs = 5L;
    private long executeDelayMaxMs = 10L;
    private double successRate = 1.0D;
    private int callbackMaxAttempts = 3;
    private long callbackRetryDelayMs = 300L;
    private String schedulerCallbackBaseUrl;
    private String host = "127.0.0.1:8400";
    private int maxLoad = 100000;
    private int executorCorePoolSize = 200;
    private int executorMaxPoolSize = 400;
    private int executorQueueCapacity = 100000;
    private long httpConnectTimeoutMs = 2000L;
    private long httpReadTimeoutMs = 5000L;
}
