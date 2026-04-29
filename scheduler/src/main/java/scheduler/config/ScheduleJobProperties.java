package scheduler.config;


import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for scheduled jobs.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "schedulejob.scheduler")
public class ScheduleJobProperties {
    private int shardCount = 128;
    private String instanceId;
    private int preheatMinutes = 20;
    private int scanBatchSize = 20000;
    private int claimBatchSize = 4000;
    private int dispatchCorePoolSize = 128;
    private int dispatchMaxPoolSize = 256;
    private int dispatchQueueCapacity = 100000;
    private int retryDelaySeconds = 3;
    private long claimTimeoutMs = 60000L;
    private long executorHeartbeatTimeoutMs = 6000L;
    private long memberTtlMs = 6000L;
    private long memberHeartbeatIntervalMs = 2000L;
    private long leaseTtlMs = 6000L;
    private long leaseRenewIntervalMs = 2000L;
    private long recoverIntervalMs = 2000L;
    private long dispatchIntervalMs = 20L;
    private String commandStreamKey = "schedulejob:stream:commands";
    private String commandConsumerGroup = "schedulejob-scheduler";
    private int commandConsumerThreadCount = 32;
    private int commandReadBatchSize = 1000;
    private long commandReadBlockTimeoutMs = 100L;
    private long httpConnectTimeoutMs = 2000L;
    private long httpReadTimeoutMs = 5000L;
    private boolean benchmarkNoWorkerMode = false;
    private String benchmarkExecutorHost = "benchmark-local";
}
