package worker.remote;

import common.R;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import common.JobExecutionCallbackRequest;
import worker.config.WorkerProperties;

/**
 * HTTP client for scheduler callback operations.
 */
@Component
@RequiredArgsConstructor
public class SchedulerCallbackRemoteClient {
    private final RestTemplate workerRestTemplate;
    private final WorkerProperties workerProperties;

    public R completeJob(Long jobId, JobExecutionCallbackRequest request) {
        String baseUrl = callbackBaseUrl();
        String url = UriComponentsBuilder.fromHttpUrl(baseUrl)
                .path("/jobs/{jobId}/complete")
                .buildAndExpand(jobId)
                .toUriString();
        return workerRestTemplate.postForObject(url, request, R.class);
    }

    private String callbackBaseUrl() {
        if (!StringUtils.hasText(workerProperties.getSchedulerCallbackBaseUrl())) {
            throw new IllegalStateException("schedulejob.worker.scheduler-callback-base-url is required");
        }
        return normalizeBaseUrl(workerProperties.getSchedulerCallbackBaseUrl());
    }

    private String normalizeBaseUrl(String baseUrl) {
        if (!StringUtils.hasText(baseUrl)) {
            throw new IllegalStateException("scheduler callback base url is blank");
        }
        String normalized = baseUrl.trim();
        if (!normalized.startsWith("http://") && !normalized.startsWith("https://")) {
            normalized = "http://" + normalized;
        }
        if (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }
}
