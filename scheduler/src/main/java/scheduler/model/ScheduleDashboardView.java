package scheduler.model;

import lombok.Getter;
import lombok.Setter;

/**
 * Dashboard view for scheduled jobs.
 */
@Getter
@Setter
public class ScheduleDashboardView {
    private Long totalJobs;
    private Long waitingJobs;
    private Long claimedJobs;
    private Long runningJobs;
    private Long pausedJobs;
    private Long hotJobs;
    private Long dispatchedJobs;
    private Long failedJobs;
    private Long archivedJobs;
    private Long redisQueueSize;
    private Long redisInflightSize;
}
