package common;


import lombok.Getter;
import lombok.Setter;

/**
 * Callback request for job execution results.
 */
@Getter
@Setter
public class JobExecutionCallbackRequest {
    private String executorHost;
    private String shardKey;
    private Boolean success;
    private String message;
}
