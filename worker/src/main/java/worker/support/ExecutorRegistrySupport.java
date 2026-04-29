package worker.support;

import common.ScheduleJobConstants;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import worker.model.ExecutorHeartbeatRequest;

import java.util.List;

/**
 * Redis registry for worker heartbeat data.
 */
@Component
public class ExecutorRegistrySupport {
    private static final DefaultRedisScript<Long> HEARTBEAT_SCRIPT = new DefaultRedisScript<>(
            """
                    local loadKey = KEYS[1]
                    local aliveKey = KEYS[2]
                    local loadStepKey = KEYS[3]
                    local host = ARGV[1]
                    local nowMillis = tonumber(ARGV[2])
                    local loadStep = tonumber(ARGV[3])
                    if not redis.call('ZSCORE', loadKey, host) then
                        redis.call('ZADD', loadKey, 0, host)
                    end
                    redis.call('ZADD', aliveKey, nowMillis, host)
                    redis.call('HSET', loadStepKey, host, loadStep)
                    return 1
                    """,
            Long.class
    );

    private final StringRedisTemplate stringRedisTemplate;

    public ExecutorRegistrySupport(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void heartbeat(ExecutorHeartbeatRequest request) {
        if (request == null || !StringUtils.hasText(request.getHost())) {
            throw new IllegalArgumentException("host cannot be blank");
        }
        if (request.getMaxLoad() == null || request.getMaxLoad() <= 0) {
            throw new IllegalArgumentException("maxLoad must be greater than 0");
        }
        String host = request.getHost().trim();
        long nowMillis = System.currentTimeMillis();
        double loadStep = 1D / request.getMaxLoad();
        stringRedisTemplate.execute(
                HEARTBEAT_SCRIPT,
                List.of(
                        ScheduleJobConstants.REDIS_EXECUTOR_LOAD_KEY,
                        ScheduleJobConstants.REDIS_EXECUTOR_ALIVE_KEY,
                        ScheduleJobConstants.REDIS_EXECUTOR_LOAD_STEP_KEY
                ),
                host,
                String.valueOf(nowMillis),
                String.valueOf(loadStep)
        );
    }
}
