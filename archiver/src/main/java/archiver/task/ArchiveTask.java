package archiver.task;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import archiver.service.ScheduleJobArchiverService;

/**
 * Scheduled task for standalone archiving.
 */
@Component
@RequiredArgsConstructor
public class ArchiveTask {
    private final ScheduleJobArchiverService scheduleJobArchiverService;

    @Scheduled(cron = "${schedulejob.archiver.archive-cron:0 0 2 * * *}")
    public void archive() {
        scheduleJobArchiverService.archiveOnce();
    }
}
