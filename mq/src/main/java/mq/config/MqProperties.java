package mq.config;

import common.ScheduleJobConstants;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the MQ ingress module.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "schedulejob.mq")
public class MqProperties {
    private String streamKey = ScheduleJobConstants.REDIS_COMMAND_STREAM_KEY;
}
