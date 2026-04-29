package scheduler.task;


import lombok.RequiredArgsConstructor;
import scheduler.service.ScheduleJobService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Scheduled maintenance task for jobs.
 */
@Component
@RequiredArgsConstructor
public class ScheduleJobMaintenanceTask {
    private final ScheduleJobService scheduleJobService;

    @Scheduled(initialDelay = 1000L, fixedDelayString = "${schedulejob.scheduler.member-heartbeat-interval-ms:5000}")
    public void refreshShardLeases() {
        scheduleJobService.refreshShardLeases();
    }

    @Scheduled(fixedDelayString = "${schedulejob.scheduler.dispatch-interval-ms:5000}")
    public void dispatch() {
        scheduleJobService.dispatchDueJobs();
    }

    @Scheduled(fixedDelayString = "${schedulejob.scheduler.recover-interval-ms:2000}")
    public void recoverTimedOutJobs() {
        scheduleJobService.recoverTimedOutJobs();
    }
}
