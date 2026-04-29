package worker.model;

import lombok.Getter;
import lombok.Setter;

/**
 * Internal heartbeat data for a worker host.
 */
@Getter
@Setter
public class ExecutorHeartbeatRequest {
    private String host;
    private Integer maxLoad;
}
