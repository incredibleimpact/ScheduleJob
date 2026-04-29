package scheduler.domain;


import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

/**
 * Persistence model for a scheduled job.
 */
@Getter
@Setter
public class ScheduleJobDO {
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
    private Integer hotCached;
    private String status;
    private String claimOwner;
    private LocalDateTime claimTime;
    private LocalDateTime claimDeadline;
    private String dispatchTarget;
    private String dispatchNote;
    private String archivePath;
    private LocalDateTime createTime;
    private LocalDateTime updateTime;
}
