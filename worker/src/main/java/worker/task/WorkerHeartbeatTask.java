package worker.task;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import worker.service.WorkerService;

/**
 * Scheduled heartbeat task for the worker host.
 */
@Component
@RequiredArgsConstructor
public class WorkerHeartbeatTask {
    private final WorkerService workerService;

    @Scheduled(initialDelay = 1000L, fixedDelayString = "${schedulejob.worker.heartbeat-interval-ms:5000}")
    public void heartbeat() {
        workerService.heartbeatAllNodes();
    }
}
