package common;


import lombok.Getter;
import lombok.Setter;

/**
 * Execution request for a worker job.
 */
@Getter
@Setter
public class WorkerExecuteRequest {
    private Long jobId;
    private String jobName;
    private String routeKey;
    private String shardKey;
    private String payloadJson;
    private Integer priority;
    private String executorHost;
}
