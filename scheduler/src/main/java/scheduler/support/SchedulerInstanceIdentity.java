package scheduler.support;

import lombok.Getter;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import scheduler.config.ScheduleJobProperties;

import java.net.InetAddress;
import java.util.UUID;

/**
 * Provides a single resolved scheduler instance id for the current process.
 */
@Getter
@Component
public class SchedulerInstanceIdentity {
    private final String instanceId;

    public SchedulerInstanceIdentity(ScheduleJobProperties properties, Environment environment) {
        this.instanceId = resolveInstanceId(properties.getInstanceId(), environment.getProperty("server.port"));
    }

    private String resolveInstanceId(String configuredInstanceId, String serverPort) {
        if (StringUtils.hasText(configuredInstanceId)) {
            return configuredInstanceId.trim();
        }
        try {
            String host = InetAddress.getLocalHost().getHostName();
            if (StringUtils.hasText(serverPort)) {
                return "scheduler-" + host + "-" + serverPort.trim();
            }
            return "scheduler-" + host + "-" + UUID.randomUUID().toString().substring(0, 8);
        } catch (Exception e) {
            return "scheduler-" + UUID.randomUUID().toString().substring(0, 8);
        }
    }
}
