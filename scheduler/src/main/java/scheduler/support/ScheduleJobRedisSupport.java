package scheduler.support;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import common.ScheduleJobConstants;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import scheduler.model.ScheduleJobSnapshot;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Redis helper for scheduler queue, lease and membership operations.
 */
@Component
public class ScheduleJobRedisSupport {
    private static final DefaultRedisScript<Long> ACQUIRE_OR_RENEW_LEASE_SCRIPT = new DefaultRedisScript<>(
            """
                    local key = KEYS[1]
                    local owner = ARGV[1]
                    local ttl = tonumber(ARGV[2])
                    local current = redis.call('GET', key)
                    if current == owner then
                        redis.call('PEXPIRE', key, ttl)
                        return 1
                    end
                    local created = redis.call('SET', key, owner, 'NX', 'PX', ttl)
                    if created then
                        return 1
                    end
                    return 0
                    """,
            Long.class
    );
    private static final DefaultRedisScript<Long> RELEASE_LEASE_SCRIPT = new DefaultRedisScript<>(
            """
                    local key = KEYS[1]
                    local owner = ARGV[1]
                    if redis.call('GET', key) == owner then
                        return redis.call('DEL', key)
                    end
                    return 0
                    """,
            Long.class
    );
    private static final DefaultRedisScript<List> CLAIM_DUE_JOB_DETAILS_SCRIPT = new DefaultRedisScript<>(
            """
                    local readyKey = KEYS[1]
                    local inflightKey = KEYS[2]
                    local detailKey = KEYS[3]
                    local now = tonumber(ARGV[1])
                    local limit = tonumber(ARGV[2])
                    local deadline = tonumber(ARGV[3])
                    local jobs = redis.call('ZRANGEBYSCORE', readyKey, '-inf', now, 'LIMIT', 0, limit)
                    local claimed = {}
                    for _, jobId in ipairs(jobs) do
                        local readyScore = redis.call('ZSCORE', readyKey, jobId)
                        if readyScore and redis.call('ZREM', readyKey, jobId) == 1 then
                            local detail = redis.call('HGET', detailKey, jobId)
                            if detail then
                                redis.call('ZADD', inflightKey, deadline, jobId)
                                table.insert(claimed, jobId)
                                table.insert(claimed, detail)
                            else
                                redis.call('ZADD', readyKey, readyScore, jobId)
                            end
                        end
                    end
                    return claimed
                    """,
            List.class
    );
    private static final DefaultRedisScript<List> RECOVER_EXPIRED_JOBS_SCRIPT = new DefaultRedisScript<>(
            """
                    local readyKey = KEYS[1]
                    local inflightKey = KEYS[2]
                    local now = tonumber(ARGV[1])
                    local limit = tonumber(ARGV[2])
                    local expired = redis.call('ZRANGEBYSCORE', inflightKey, '-inf', now, 'LIMIT', 0, limit)
                    local recovered = {}
                    for _, jobId in ipairs(expired) do
                        if redis.call('ZREM', inflightKey, jobId) == 1 then
                            redis.call('ZADD', readyKey, now, jobId)
                            table.insert(recovered, jobId)
                        end
                    end
                    return recovered
                    """,
            List.class
    );
    private static final DefaultRedisScript<Long> UPDATE_INFLIGHT_SNAPSHOT_SCRIPT = new DefaultRedisScript<>(
            """
                    local inflightKey = KEYS[1]
                    local detailKey = KEYS[2]
                    local jobId = ARGV[1]
                    local content = ARGV[2]
                    if not redis.call('ZSCORE', inflightKey, jobId) then
                        return 0
                    end
                    if redis.call('HEXISTS', detailKey, jobId) == 0 then
                        return 0
                    end
                    redis.call('HSET', detailKey, jobId, content)
                    return 1
                    """,
            Long.class
    );

    private final StringRedisTemplate stringRedisTemplate;
    private final ObjectMapper objectMapper;

    public ScheduleJobRedisSupport(StringRedisTemplate stringRedisTemplate, ObjectMapper objectMapper) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.objectMapper = objectMapper;
    }

    public void registerSchedulerMember(String instanceId, long memberTtlMs) {
        long expireAt = System.currentTimeMillis() + memberTtlMs;
        stringRedisTemplate.opsForZSet().add(ScheduleJobConstants.REDIS_SCHEDULER_MEMBER_KEY, instanceId, expireAt);
        stringRedisTemplate.opsForZSet()
                .removeRangeByScore(ScheduleJobConstants.REDIS_SCHEDULER_MEMBER_KEY, Double.NEGATIVE_INFINITY, System.currentTimeMillis());
    }

    public List<String> listActiveSchedulerMembers() {
        Set<String> values = stringRedisTemplate.opsForZSet()
                .rangeByScore(ScheduleJobConstants.REDIS_SCHEDULER_MEMBER_KEY, System.currentTimeMillis(), Double.POSITIVE_INFINITY);
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        return values.stream().sorted().toList();
    }

    public boolean acquireOrRenewShardLease(int shard, String instanceId, long leaseTtlMs) {
        Long result = stringRedisTemplate.execute(
                ACQUIRE_OR_RENEW_LEASE_SCRIPT,
                Collections.singletonList(ScheduleJobConstants.leaseKey(shard)),
                instanceId,
                String.valueOf(leaseTtlMs)
        );
        return result != null && result > 0;
    }

    public void releaseShardLease(int shard, String instanceId) {
        stringRedisTemplate.execute(
                RELEASE_LEASE_SCRIPT,
                Collections.singletonList(ScheduleJobConstants.leaseKey(shard)),
                instanceId
        );
    }

    public List<ScheduleJobSnapshot> claimDueJobSnapshots(int shard, long nowMillis, int limit, long deadlineMillis) {
        List<?> values = stringRedisTemplate.execute(
                CLAIM_DUE_JOB_DETAILS_SCRIPT,
                List.of(
                        ScheduleJobConstants.readyKey(shard),
                        ScheduleJobConstants.inflightKey(shard),
                        ScheduleJobConstants.detailKey(shard)
                ),
                String.valueOf(nowMillis),
                String.valueOf(limit),
                String.valueOf(deadlineMillis)
        );
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        List<ScheduleJobSnapshot> snapshots = new ArrayList<>(values.size() / 2);
        for (int index = 0; index + 1 < values.size(); index += 2) {
            Object jobIdValue = values.get(index);
            Object detailValue = values.get(index + 1);
            if (!(jobIdValue instanceof String jobIdText) || !(detailValue instanceof String content)
                    || !StringUtils.hasText(content)) {
                continue;
            }
            ScheduleJobSnapshot snapshot = deserializeSnapshot(content);
            if (snapshot.getId() == null) {
                snapshot.setId(Long.valueOf(jobIdText));
            }
            snapshots.add(snapshot);
        }
        return snapshots;
    }

    public List<String> recoverExpiredInflightJobs(int shard, long nowMillis, int limit) {
        List<String> jobIds = stringRedisTemplate.execute(
                RECOVER_EXPIRED_JOBS_SCRIPT,
                List.of(ScheduleJobConstants.readyKey(shard), ScheduleJobConstants.inflightKey(shard)),
                String.valueOf(nowMillis),
                String.valueOf(limit)
        );
        return jobIds == null ? Collections.emptyList() : jobIds;
    }

    public ScheduleJobSnapshot loadSnapshot(Long jobId, int shard) {
        Object json = stringRedisTemplate.opsForHash().get(ScheduleJobConstants.detailKey(shard), String.valueOf(jobId));
        if (!(json instanceof String content) || !StringUtils.hasText(content)) {
            return null;
        }
        return deserializeSnapshot(content);
    }

    public boolean isReady(Long jobId, int shard) {
        Double score = stringRedisTemplate.opsForZSet()
                .score(ScheduleJobConstants.readyKey(shard), String.valueOf(jobId));
        return score != null;
    }

    public boolean isInflight(Long jobId, int shard) {
        Double score = stringRedisTemplate.opsForZSet()
                .score(ScheduleJobConstants.inflightKey(shard), String.valueOf(jobId));
        return score != null;
    }

    public void cacheJob(ScheduleJobSnapshot snapshot, LocalDateTime executeTime, int shard) {
        cacheDetail(snapshot, shard);
        stringRedisTemplate.opsForZSet().add(
                ScheduleJobConstants.readyKey(shard),
                String.valueOf(snapshot.getId()),
                toEpochMilli(executeTime)
        );
    }

    public void cacheDetail(ScheduleJobSnapshot snapshot, int shard) {
        cacheDetails(List.of(snapshot), shard);
    }

    public void cacheDetails(List<ScheduleJobSnapshot> snapshots, int shard) {
        if (snapshots == null || snapshots.isEmpty()) {
            return;
        }
        Map<String, String> values = new LinkedHashMap<>(snapshots.size());
        for (ScheduleJobSnapshot snapshot : snapshots) {
            if (snapshot == null || snapshot.getId() == null) {
                continue;
            }
            values.put(String.valueOf(snapshot.getId()), serializeSnapshot(snapshot));
        }
        if (values.isEmpty()) {
            return;
        }
        try {
            stringRedisTemplate.opsForHash().putAll(ScheduleJobConstants.detailKey(shard), values);
        } catch (Exception e) {
            throw new IllegalStateException("failed to cache job snapshots", e);
        }
    }

    public boolean updateInflightSnapshot(ScheduleJobSnapshot snapshot, int shard) {
        if (snapshot == null || snapshot.getId() == null) {
            return false;
        }
        Long updated = stringRedisTemplate.execute(
                UPDATE_INFLIGHT_SNAPSHOT_SCRIPT,
                List.of(ScheduleJobConstants.inflightKey(shard), ScheduleJobConstants.detailKey(shard)),
                String.valueOf(snapshot.getId()),
                serializeSnapshot(snapshot)
        );
        return updated != null && updated > 0;
    }

    public void removeDetail(Long jobId, int shard) {
        stringRedisTemplate.opsForHash().delete(ScheduleJobConstants.detailKey(shard), String.valueOf(jobId));
    }

    public void removeFromReady(Long jobId, int shard) {
        stringRedisTemplate.opsForZSet().remove(ScheduleJobConstants.readyKey(shard), String.valueOf(jobId));
    }

    public void removeFromInflight(Long jobId, int shard) {
        stringRedisTemplate.opsForZSet().remove(ScheduleJobConstants.inflightKey(shard), String.valueOf(jobId));
    }

    public long readySize(int shard) {
        Long size = stringRedisTemplate.opsForZSet().zCard(ScheduleJobConstants.readyKey(shard));
        return size == null ? 0L : size;
    }

    public long inflightSize(int shard) {
        Long size = stringRedisTemplate.opsForZSet().zCard(ScheduleJobConstants.inflightKey(shard));
        return size == null ? 0L : size;
    }

    public void ensureCommandConsumerGroup(String streamKey, String groupName) {
        if (!Boolean.TRUE.equals(stringRedisTemplate.hasKey(streamKey))) {
            stringRedisTemplate.opsForStream().add(
                    StreamRecords.mapBacked(Map.of("_bootstrap", "1")).withStreamKey(streamKey)
            );
        }
        try {
            stringRedisTemplate.opsForStream().createGroup(streamKey, ReadOffset.latest(), groupName);
        } catch (Exception e) {
            if (!containsBusyGroup(e)) {
                throw e;
            }
        }
    }

    private boolean containsBusyGroup(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && message.contains("BUSYGROUP")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private double toEpochMilli(LocalDateTime time) {
        return time.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    private ScheduleJobSnapshot deserializeSnapshot(String content) {
        try {
            return objectMapper.readValue(content, ScheduleJobSnapshot.class);
        } catch (IOException e) {
            throw new IllegalStateException("failed to deserialize cached job snapshot", e);
        }
    }

    private String serializeSnapshot(ScheduleJobSnapshot snapshot) {
        try {
            return objectMapper.writeValueAsString(snapshot);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize job snapshot", e);
        }
    }
}
