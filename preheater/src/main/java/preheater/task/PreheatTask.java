package preheater.task;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import scheduler.preheat.ScheduleJobPreheatService;

/**
 * Scheduled task for standalone preheating.
 */
@Component
@RequiredArgsConstructor
public class PreheatTask {
    private final ScheduleJobPreheatService scheduleJobPreheatService;

    @Scheduled(fixedDelayString = "${schedulejob.preheater.preheat-interval-ms:15000}")
    public void preheat() {
        scheduleJobPreheatService.preheatHotJobs();
    }
}
