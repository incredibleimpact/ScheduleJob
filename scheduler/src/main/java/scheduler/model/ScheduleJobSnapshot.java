package scheduler.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

/**
 * Snapshot model for a scheduled job.
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class ScheduleJobSnapshot {
    private Long id;
    private String jobName;
    private String routeKey;
    private String shardKey;
    private String payloadJson;
    private LocalDateTime executeTime;
    private Integer priority;
    private Integer retryTimes;
    private Integer maxRetryTimes;
    private Long creatorId;
    private String status;
    private String dispatchTarget;
    private String dispatchNote;
}
