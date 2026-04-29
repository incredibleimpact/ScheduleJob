package scheduler.remote;

import common.R;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;
import common.WorkerExecuteRequest;

/**
 * HTTP client for direct worker dispatch.
 */
@Component
@RequiredArgsConstructor
public class WorkerRemoteClient {
    private final RestTemplate schedulerRestTemplate;

    public R execute(WorkerExecuteRequest request, String executorHost) {
        return schedulerRestTemplate.postForObject(resolveExecuteUrl(executorHost), request, R.class);
    }

    private String resolveExecuteUrl(String executorHost) {
        return normalizeBaseUrl(executorHost) + "/worker/jobs/execute";
    }

    private String normalizeBaseUrl(String baseUrl) {
        if (!StringUtils.hasText(baseUrl)) {
            throw new IllegalStateException("worker executor host is blank");
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
