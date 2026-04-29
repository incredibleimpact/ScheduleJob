package common;


import lombok.Getter;
import lombok.Setter;

/**
 * Execution response for a worker job.
 */
@Getter
@Setter
public class WorkerExecuteResponse {
    private Boolean accepted;
    private String executorHost;
    private String message;
}
