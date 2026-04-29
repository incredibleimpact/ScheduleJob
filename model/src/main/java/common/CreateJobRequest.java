package common;

import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

/**
 * Request model for creating a scheduled job.
 */
@Getter
@Setter
public class CreateJobRequest {
    private String jobName;
    private String routeKey;
    private String payloadJson;
    private LocalDateTime executeTime;
    private Integer priority;
    private Integer maxRetryTimes;
}
