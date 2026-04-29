package common;

import lombok.Getter;
import lombok.Setter;

/**
 * Command payload stored in Redis Stream for scheduler-side processing.
 */
@Getter
@Setter
public class JobCommandMessage {
    private JobCommandType commandType;
    private Long jobId;
    private Long creatorId;
    private String bodyJson;
}
