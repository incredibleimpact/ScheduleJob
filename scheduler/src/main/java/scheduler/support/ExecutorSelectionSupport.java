package scheduler.support;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import common.ScheduleJobConstants;

import java.util.Collections;
import java.util.List;

/**
 * Redis-based executor reservation and release support.
 */
@Component
public class ExecutorSelectionSupport {
    private static final DefaultRedisScript<List> RESERVE_BATCH_SCRIPT = new DefaultRedisScript<>(
            """
                    local loadKey = KEYS[1]
                    local aliveKey = KEYS[2]
                    local loadStepKey = KEYS[3]
                    local requestCount = tonumber(ARGV[1])
                    local heartbeatDeadline = tonumber(ARGV[2])
                    local result = {}
                    local expiredHosts = redis.call('ZRANGEBYSCORE', aliveKey, '-inf', '(' .. heartbeatDeadline)
                    for idx = 1, #expiredHosts do
                        local host = expiredHosts[idx]
                        redis.call('ZREM', aliveKey, host)
                        redis.call('ZREM', loadKey, host)
                        redis.call('HDEL', loadStepKey, host)
                    end
                    for i = 1, requestCount do
                        local tuples = redis.call('ZRANGE', loadKey, 0, -1, 'WITHSCORES')
                        local selectedHost = nil
                        local selectedRatio = nil
                        local selectedLoadStep = nil
                        for idx = 1, #tuples, 2 do
                            local host = tuples[idx]
                            local currentRatio = tonumber(tuples[idx + 1])
                            local heartbeat = tonumber(redis.call('ZSCORE', aliveKey, host) or '0')
                            if heartbeat < heartbeatDeadline then
                                redis.call('ZREM', aliveKey, host)
                                redis.call('ZREM', loadKey, host)
                                redis.call('HDEL', loadStepKey, host)
                            else
                                local loadStep = tonumber(redis.call('HGET', loadStepKey, host) or '0')
                                if loadStep <= 0 then
                                    redis.call('ZREM', aliveKey, host)
                                    redis.call('ZREM', loadKey, host)
                                    redis.call('HDEL', loadStepKey, host)
                                elseif (currentRatio + loadStep) < 1 then
                                    selectedHost = host
                                    selectedRatio = currentRatio
                                    selectedLoadStep = loadStep
                                    break
                                end
                            end
                        end
                        if not selectedHost then
                            break
                        end
                        local nextRatio = selectedRatio + selectedLoadStep
                        redis.call('ZADD', loadKey, nextRatio, selectedHost)
                        table.insert(result, selectedHost)
                    end
                    return result
                    """,
            List.class
    );

    private static final DefaultRedisScript<Long> RELEASE_EXECUTOR_SCRIPT = new DefaultRedisScript<>(
            """
                    local loadKey = KEYS[1]
                    local loadStepKey = KEYS[2]
                    local host = ARGV[1]
                    local currentScore = redis.call('ZSCORE', loadKey, host)
                    if not currentScore then
                        return 0
                    end
                    local loadStep = tonumber(redis.call('HGET', loadStepKey, host) or '0')
                    if loadStep <= 0 then
                        return 0
                    end
                    local nextRatio = tonumber(currentScore or '0') - loadStep
                    if nextRatio < 0 then
                        nextRatio = 0
                    end
                    redis.call('ZADD', loadKey, nextRatio, host)
                    return 1
                    """,
            Long.class
    );

    private final StringRedisTemplate stringRedisTemplate;

    public ExecutorSelectionSupport(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public List<String> reserveBatch(int requestCount, long heartbeatTimeoutMs) {
        if (requestCount <= 0) {
            return Collections.emptyList();
        }
        long heartbeatDeadline = System.currentTimeMillis() - heartbeatTimeoutMs;
        List<String> values = stringRedisTemplate.execute(
                RESERVE_BATCH_SCRIPT,
                List.of(
                        ScheduleJobConstants.REDIS_EXECUTOR_LOAD_KEY,
                        ScheduleJobConstants.REDIS_EXECUTOR_ALIVE_KEY,
                        ScheduleJobConstants.REDIS_EXECUTOR_LOAD_STEP_KEY
                ),
                String.valueOf(requestCount),
                String.valueOf(heartbeatDeadline)
        );
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        return values.stream()
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .filter(StringUtils::hasText)
                .toList();
    }

    public void release(String host) {
        if (!StringUtils.hasText(host)) {
            return;
        }
        stringRedisTemplate.execute(
                RELEASE_EXECUTOR_SCRIPT,
                List.of(
                        ScheduleJobConstants.REDIS_EXECUTOR_LOAD_KEY,
                        ScheduleJobConstants.REDIS_EXECUTOR_LOAD_STEP_KEY
                ),
                host.trim()
        );
    }

}
