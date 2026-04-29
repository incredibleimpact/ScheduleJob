package scheduler.preheat;

import org.springframework.stereotype.Component;
import scheduler.config.ScheduleJobProperties;
import scheduler.domain.ScheduleJobDO;
import scheduler.mapper.ScheduleJobMapper;
import scheduler.model.ScheduleJobSnapshot;
import scheduler.support.ScheduleJobRedisSupport;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Shared service for preheating near-future jobs into Redis.
 */
@Component
public class ScheduleJobPreheatService {
    private final ScheduleJobMapper scheduleJobMapper;
    private final ScheduleJobRedisSupport scheduleJobRedisSupport;
    private final ScheduleJobProperties properties;

    public ScheduleJobPreheatService(ScheduleJobMapper scheduleJobMapper,
                                     ScheduleJobRedisSupport scheduleJobRedisSupport,
                                     ScheduleJobProperties properties) {
        this.scheduleJobMapper = scheduleJobMapper;
        this.scheduleJobRedisSupport = scheduleJobRedisSupport;
        this.properties = properties;
    }

    public int preheatHotJobs() {
        LocalDateTime now = LocalDateTime.now();
        int repaired = repairMissingHotJobs(now);
        List<ScheduleJobDO> jobs = scheduleJobMapper.selectPreheatJobs(
                now.minusMinutes(1),
                now.plusMinutes(properties.getPreheatMinutes()),
                properties.getScanBatchSize()
        );
        for (ScheduleJobDO job : jobs) {
            ScheduleJobSnapshot snapshot = toSnapshot(job);
            int shard = resolveShard(snapshot.getShardKey());
            scheduleJobRedisSupport.cacheJob(snapshot, job.getExecuteTime(), shard);
            scheduleJobMapper.markHotCached(job.getId(), 1);
        }
        return repaired + jobs.size();
    }

    private int repairMissingHotJobs(LocalDateTime now) {
        List<ScheduleJobDO> jobs = scheduleJobMapper.selectHotCachedWaitingJobs(
                now.plusMinutes(properties.getPreheatMinutes()),
                properties.getScanBatchSize()
        );
        int repaired = 0;
        for (ScheduleJobDO job : jobs) {
            int shard = resolveShard(job.getShardKey());
            if (scheduleJobRedisSupport.isReady(job.getId(), shard)
                    || scheduleJobRedisSupport.isInflight(job.getId(), shard)) {
                continue;
            }
            ScheduleJobSnapshot snapshot = toSnapshot(job);
            scheduleJobRedisSupport.cacheJob(snapshot, job.getExecuteTime(), shard);
            repaired++;
        }
        return repaired;
    }

    private int resolveShard(String shardKey) {
        return Math.floorMod(shardKey.hashCode(), properties.getShardCount());
    }

    private ScheduleJobSnapshot toSnapshot(ScheduleJobDO job) {
        ScheduleJobSnapshot snapshot = new ScheduleJobSnapshot();
        snapshot.setId(job.getId());
        snapshot.setJobName(job.getJobName());
        snapshot.setRouteKey(job.getRouteKey());
        snapshot.setShardKey(job.getShardKey());
        snapshot.setPayloadJson(job.getPayloadJson());
        snapshot.setExecuteTime(job.getExecuteTime());
        snapshot.setPriority(job.getPriority());
        snapshot.setRetryTimes(job.getRetryTimes());
        snapshot.setMaxRetryTimes(job.getMaxRetryTimes());
        snapshot.setCreatorId(job.getCreatorId());
        snapshot.setStatus(job.getStatus());
        snapshot.setDispatchTarget(job.getDispatchTarget());
        snapshot.setDispatchNote(job.getDispatchNote());
        return snapshot;
    }
}
