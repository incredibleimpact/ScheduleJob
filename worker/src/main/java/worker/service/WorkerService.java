package worker.service;

import common.R;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import common.JobExecutionCallbackRequest;
import common.WorkerExecuteRequest;
import common.WorkerExecuteResponse;
import worker.config.WorkerProperties;
import worker.model.ExecutorHeartbeatRequest;
import worker.remote.SchedulerCallbackRemoteClient;
import worker.support.ExecutorRegistrySupport;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Worker service implementation.
 */
@Service
public class WorkerService {
    private static final Logger log = LoggerFactory.getLogger(WorkerService.class);

    private final WorkerProperties properties;
    private final ExecutorRegistrySupport executorRegistrySupport;
    private final SchedulerCallbackRemoteClient schedulerCallbackRemoteClient;
    private final Executor workerExecutionExecutor;
    private final String workerHost;

    public WorkerService(WorkerProperties properties,
                         ExecutorRegistrySupport executorRegistrySupport,
                         SchedulerCallbackRemoteClient schedulerCallbackRemoteClient,
                         @Qualifier("workerExecutionExecutor") Executor workerExecutionExecutor) {
        this.properties = properties;
        this.executorRegistrySupport = executorRegistrySupport;
        this.schedulerCallbackRemoteClient = schedulerCallbackRemoteClient;
        this.workerExecutionExecutor = workerExecutionExecutor;
        if (!StringUtils.hasText(properties.getHost())) {
            throw new IllegalArgumentException("schedulejob.worker.host cannot be blank");
        }
        if (properties.getMaxLoad() <= 0) {
            throw new IllegalArgumentException("schedulejob.worker.max-load must be greater than 0");
        }
        this.workerHost = properties.getHost().trim();
    }

    public void heartbeatAllNodes() {
        ExecutorHeartbeatRequest request = new ExecutorHeartbeatRequest();
        request.setHost(workerHost);
        request.setMaxLoad(properties.getMaxLoad());
        try {
            executorRegistrySupport.heartbeat(request);
        } catch (Exception e) {
            log.warn("worker heartbeat failed, host={}", workerHost, e);
        }
    }

    public WorkerExecuteResponse execute(WorkerExecuteRequest request) {
        validateRequest(request);
        String executorHost = request.getExecutorHost().trim();
        if (!workerHost.equals(executorHost)) {
            throw new IllegalArgumentException("worker host does not match request: " + executorHost);
        }
        try {
            workerExecutionExecutor.execute(() -> runExecution(request));
        } catch (RejectedExecutionException e) {
            throw new IllegalArgumentException("worker execution queue is full");
        }
        WorkerExecuteResponse response = new WorkerExecuteResponse();
        response.setAccepted(Boolean.TRUE);
        response.setExecutorHost(workerHost);
        response.setMessage("worker accepted job[" + request.getJobId() + "]");
        return response;
    }

    private void runExecution(WorkerExecuteRequest request) {
        sleepQuietly(randomDelayMs());
        // Business handlers must implement idempotency because the scheduler provides at-least-once delivery.
        ExecutionResult result = executeBusinessTask(request);
        JobExecutionCallbackRequest callbackRequest = new JobExecutionCallbackRequest();
        callbackRequest.setExecutorHost(request.getExecutorHost());
        callbackRequest.setShardKey(request.getShardKey());
        callbackRequest.setSuccess(result.success());
        callbackRequest.setMessage(result.message());
        boolean callbackSucceeded = callbackWithRetry(request.getJobId(), callbackRequest);
        if (!callbackSucceeded) {
            log.error("worker callback failed after retries, jobId={}, executorHost={}",
                    request.getJobId(), request.getExecutorHost());
        }
    }

    private ExecutionResult executeBusinessTask(WorkerExecuteRequest request) {
        return simulateGenericTask(request);
    }

    private ExecutionResult simulateGenericTask(WorkerExecuteRequest request) {
        boolean success = ThreadLocalRandom.current().nextDouble() < clampSuccessRate(properties.getSuccessRate());
        return new ExecutionResult(success, success
                ? "worker finished routeKey=" + request.getRouteKey()
                : "worker failed routeKey=" + request.getRouteKey());
    }

    private boolean callbackWithRetry(Long jobId, JobExecutionCallbackRequest callbackRequest) {
        int maxAttempts = Math.max(properties.getCallbackMaxAttempts(), 1);
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                R response = schedulerCallbackRemoteClient.completeJob(jobId, callbackRequest);
                if (response != null && response.getCode() != null && response.getCode() == 200) {
                    return true;
                }
                log.warn("worker callback rejected, jobId={}, attempt={}, message={}",
                        jobId, attempt, response == null ? "null" : response.getMessage());
            } catch (Exception e) {
                log.warn("worker callback failed, jobId={}, attempt={}", jobId, attempt, e);
            }
            if (attempt < maxAttempts) {
                sleepQuietly(properties.getCallbackRetryDelayMs());
            }
        }
        return false;
    }

    private void validateRequest(WorkerExecuteRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("worker request cannot be null");
        }
        if (request.getJobId() == null) {
            throw new IllegalArgumentException("jobId cannot be null");
        }
        if (!StringUtils.hasText(request.getExecutorHost())) {
            throw new IllegalArgumentException("executorHost cannot be blank");
        }
    }

    private long randomDelayMs() {
        long minDelay = Math.max(properties.getExecuteDelayMinMs(), 0L);
        long maxDelay = Math.max(properties.getExecuteDelayMaxMs(), minDelay);
        if (maxDelay == minDelay) {
            return minDelay;
        }
        return ThreadLocalRandom.current().nextLong(minDelay, maxDelay + 1L);
    }

    private double clampSuccessRate(double successRate) {
        if (successRate < 0D) {
            return 0D;
        }
        if (successRate > 1D) {
            return 1D;
        }
        return successRate;
    }

    private void sleepQuietly(long delayMs) {
        try {
            //Thread.sleep(delayMs);
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private record ExecutionResult(boolean success, String message) {
    }
}
