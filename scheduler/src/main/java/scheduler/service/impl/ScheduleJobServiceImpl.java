package scheduler.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import common.CreateJobRequest;
import common.JobExecutionCallbackRequest;
import common.R;
import common.WorkerExecuteRequest;
import common.WorkerExecuteResponse;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import scheduler.config.ScheduleJobProperties;
import scheduler.domain.ScheduleJobDO;
import scheduler.mapper.ScheduleJobMapper;
import scheduler.model.JobStatus;
import scheduler.model.ScheduleDashboardView;
import scheduler.model.ScheduleJobSnapshot;
import scheduler.preheat.ScheduleJobPreheatService;
import scheduler.remote.WorkerRemoteClient;
import scheduler.service.ScheduleJobService;
import scheduler.support.ExecutorSelectionSupport;
import scheduler.support.SchedulerInstanceIdentity;
import scheduler.support.ScheduleJobRedisSupport;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * Scheduler service implementation.
 */
@Service
public class ScheduleJobServiceImpl implements ScheduleJobService {
    private final ScheduleJobMapper scheduleJobMapper;
    private final ScheduleJobRedisSupport scheduleJobRedisSupport;
    private final ObjectMapper objectMapper;
    private final ExecutorSelectionSupport executorSelectionSupport;
    private final WorkerRemoteClient workerRemoteClient;
    private final Executor jobDispatchExecutor;
    private final ScheduleJobPreheatService scheduleJobPreheatService;
    private final ScheduleJobProperties properties;
    private final String instanceId;
    private final Set<Integer> ownedShards = ConcurrentHashMap.newKeySet();

    public ScheduleJobServiceImpl(ScheduleJobMapper scheduleJobMapper,
                                  ScheduleJobRedisSupport scheduleJobRedisSupport,
                                  ObjectMapper objectMapper,
                                  ExecutorSelectionSupport executorSelectionSupport,
                                  WorkerRemoteClient workerRemoteClient,
                                  @Qualifier("jobDispatchExecutor") Executor jobDispatchExecutor,
                                  ScheduleJobPreheatService scheduleJobPreheatService,
                                  ScheduleJobProperties properties,
                                  SchedulerInstanceIdentity schedulerInstanceIdentity) {
        this.scheduleJobMapper = scheduleJobMapper;
        this.scheduleJobRedisSupport = scheduleJobRedisSupport;
        this.objectMapper = objectMapper;
        this.executorSelectionSupport = executorSelectionSupport;
        this.workerRemoteClient = workerRemoteClient;
        this.jobDispatchExecutor = jobDispatchExecutor;
        this.scheduleJobPreheatService = scheduleJobPreheatService;
        this.properties = properties;
        this.instanceId = schedulerInstanceIdentity.getInstanceId();
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public R createJob(CreateJobRequest request, Long creatorId) {
        validateCreateRequest(request);
        ScheduleJobDO scheduleJobDO = new ScheduleJobDO();
        scheduleJobDO.setJobName(request.getJobName().trim());
        scheduleJobDO.setRouteKey(request.getRouteKey().trim());
        scheduleJobDO.setShardKey(generateShardKey());
        scheduleJobDO.setPayloadJson(request.getPayloadJson());
        scheduleJobDO.setExecuteTime(request.getExecuteTime());
        scheduleJobDO.setPriority(defaultValue(request.getPriority(), 5));
        scheduleJobDO.setRetryTimes(0);
        scheduleJobDO.setMaxRetryTimes(defaultValue(request.getMaxRetryTimes(), 3));
        scheduleJobDO.setCreatorId(creatorId);
        boolean hotCached = shouldPreheat(request.getExecuteTime());
        scheduleJobDO.setHotCached(hotCached ? 1 : 0);
        scheduleJobDO.setStatus(JobStatus.WAITING.name());
        scheduleJobMapper.insert(scheduleJobDO);
        // MyBatis populates scheduleJobDO.id after insert via useGeneratedKeys.
        if (hotCached) {
            cacheJob(toSnapshot(scheduleJobDO), scheduleJobDO.getExecuteTime());
        }
        return R.ok("create success", scheduleJobDO.getId());
    }

    @Override
    public ScheduleJobSnapshot getJob(Long id) {
        ScheduleJobDO scheduleJobDO = scheduleJobMapper.selectById(id);
        if (scheduleJobDO == null) {
            scheduleJobDO = scheduleJobMapper.selectArchiveById(id);
        }
        return scheduleJobDO == null ? null : resolveRuntimeSnapshot(scheduleJobDO);
    }

    @Override
    public List<ScheduleJobSnapshot> listJobs(String status) {
        List<ScheduleJobDO> jobs;
        if (JobStatus.ARCHIVED.name().equals(status)) {
            jobs = scheduleJobMapper.selectArchiveJobs(100);
        } else {
            jobs = scheduleJobMapper.selectJobs(status, 100);
        }
        List<ScheduleJobSnapshot> snapshots = new ArrayList<>(jobs.size());
        for (ScheduleJobDO job : jobs) {
            snapshots.add(toSnapshot(job));
        }
        return snapshots;
    }

    @Override
    public ScheduleDashboardView dashboard() {
        ScheduleDashboardView view = new ScheduleDashboardView();
        long activeJobs = defaultLong(scheduleJobMapper.countAll());
        long archivedJobs = defaultLong(scheduleJobMapper.countArchiveAll());
        view.setTotalJobs(activeJobs + archivedJobs);
        view.setWaitingJobs(defaultLong(scheduleJobMapper.countByStatus(JobStatus.WAITING.name())));
        view.setRunningJobs(0L);
        view.setPausedJobs(defaultLong(scheduleJobMapper.countByStatus(JobStatus.PAUSED.name())));
        view.setHotJobs(defaultLong(scheduleJobMapper.countHotCached()));
        view.setDispatchedJobs(defaultLong(scheduleJobMapper.countByStatus(JobStatus.Completed.name())));
        view.setFailedJobs(defaultLong(scheduleJobMapper.countByStatus(JobStatus.FAILED.name())));
        view.setArchivedJobs(archivedJobs);
        long readySize = 0L;
        long inflightSize = 0L;
        for (int shard = 0; shard < properties.getShardCount(); shard++) {
            readySize += scheduleJobRedisSupport.readySize(shard);
            inflightSize += scheduleJobRedisSupport.inflightSize(shard);
        }
        view.setClaimedJobs(inflightSize);
        view.setRedisQueueSize(readySize);
        view.setRedisInflightSize(inflightSize);
        return view;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public R pauseJob(Long id) {
        ScheduleJobDO job = requireJob(id);
        if (JobStatus.PAUSED.name().equals(job.getStatus())) {
            return R.ok("pause success", id);
        }
        if (!JobStatus.WAITING.name().equals(job.getStatus())) {
            throw new IllegalArgumentException("only WAITING jobs can be paused");
        }
        int updated = scheduleJobMapper.markPaused(id, "job paused manually");
        if (updated == 0) {
            throw new IllegalArgumentException("failed to pause job because status changed");
        }
        ScheduleJobSnapshot runtimeSnapshot = resolveRuntimeSnapshot(job);
        clearRedisState(job);
        safeReleaseExecutor(resolveExecutorHost(runtimeSnapshot, null));
        return R.ok("pause success", id);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public R resumeJob(Long id) {
        ScheduleJobDO job = requireJob(id);
        if (JobStatus.WAITING.name().equals(job.getStatus())) {
            return R.ok("resume success", id);
        }
        if (!JobStatus.PAUSED.name().equals(job.getStatus())) {
            throw new IllegalArgumentException("only PAUSED jobs can be resumed");
        }
        boolean hotCached = shouldPreheat(job.getExecuteTime());
        int updated = scheduleJobMapper.markWaiting(id, "job resumed manually", hotCached ? 1 : 0);
        if (updated == 0) {
            throw new IllegalArgumentException("failed to resume job because status changed");
        }
        job.setStatus(JobStatus.WAITING.name());
        job.setHotCached(hotCached ? 1 : 0);
        job.setClaimOwner(null);
        job.setClaimTime(null);
        job.setClaimDeadline(null);
        job.setDispatchTarget(null);
        job.setDispatchNote("job resumed manually");
        if (hotCached) {
            cacheJob(toSnapshot(job), job.getExecuteTime());
        }
        return R.ok("resume success", id);
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public R completeJob(Long id, JobExecutionCallbackRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("callback request is required");
        }
        CallbackContext context = resolveCallbackContext(id, request);
        if (context == null) {
            return R.ok("complete ignored", id);
        }
        String callbackMessage = resolveCallbackMessage(request);
        String targetStatus = Boolean.TRUE.equals(request.getSuccess())
                ? JobStatus.Completed.name()
                : JobStatus.FAILED.name();
        int updated = scheduleJobMapper.markCompletedOrFailed(id, targetStatus, callbackMessage);
        if (updated == 0) {
            return R.ok("complete ignored", id);
        }
        clearRedisMembership(id, context.shard());
        safeReleaseExecutor(resolveExecutorHost(context.snapshot(), request));
        return R.ok("complete success", id);
    }

    /**
     * Refresh shard leases for the current scheduler instance.
     * Every scheduler derives ownership from the same sorted active-member list.
     */
    @Override
    public int refreshShardLeases() {
        // Register the current scheduler instance and refresh its liveness in Redis.
        // If instance-id is not configured, a random local instance id is used.
        scheduleJobRedisSupport.registerSchedulerMember(instanceId, properties.getMemberTtlMs());
        // All schedulers must derive shard ownership from the same sorted member list.
        List<String> activeMembers = new ArrayList<>(scheduleJobRedisSupport.listActiveSchedulerMembers());
        if (!activeMembers.contains(instanceId)) {
            activeMembers.add(instanceId);
            activeMembers.sort(String::compareTo);
        }
        // Compute the shards this instance should own in the current membership view.
        Set<Integer> desiredShards = computeDesiredShards(activeMembers);
        // Keep only leases that were actually renewed or acquired in this round.
        Set<Integer> nextOwnedShards = new LinkedHashSet<>();
        // Try to acquire or renew the shards assigned to this instance.
        for (Integer shard : desiredShards) {
            if (scheduleJobRedisSupport.acquireOrRenewShardLease(shard, instanceId, properties.getLeaseTtlMs())) {
                nextOwnedShards.add(shard);
            }
        }
        for (Integer shard : List.copyOf(ownedShards)) {
            if (!desiredShards.contains(shard)) {
                scheduleJobRedisSupport.releaseShardLease(shard, instanceId);
            }
        }
        ownedShards.clear();
        ownedShards.addAll(nextOwnedShards);
        return ownedShards.size();
    }

    /**
     * Load near-future jobs into Redis.
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public int preheatHotJobs() {
        return scheduleJobPreheatService.preheatHotJobs();
    }
    @Override
    public int dispatchDueJobs() {
        if (ownedShards.isEmpty()) {
            refreshShardLeases();
        }
        List<ScheduleJobSnapshot> claimedSnapshots = new ArrayList<>();
        long nowMillis = System.currentTimeMillis();
        long deadlineMillis = nowMillis + properties.getClaimTimeoutMs();
        for (Integer shard : ownedShards.stream().sorted(Comparator.naturalOrder()).toList()) {
            List<ScheduleJobSnapshot> shardSnapshots = scheduleJobRedisSupport.claimDueJobSnapshots(
                    shard,
                    nowMillis,
                    properties.getClaimBatchSize(),
                    deadlineMillis
            );
            if (shardSnapshots.isEmpty()) {
                continue;
            }
            claimedSnapshots.addAll(shardSnapshots);
        }
        if (claimedSnapshots.isEmpty()) {
            return 0;
        }
        if (properties.isBenchmarkNoWorkerMode()) {
            return benchmarkDispatchWithoutWorker(claimedSnapshots);
        }
        List<String> reservations = executorSelectionSupport.reserveBatch(
                claimedSnapshots.size(),
                properties.getExecutorHeartbeatTimeoutMs()
        );
        int submitted = 0;
        for (int index = 0; index < claimedSnapshots.size(); index++) {
            ScheduleJobSnapshot snapshot = claimedSnapshots.get(index);
            if (index >= reservations.size()) {
                requeueClaimedJob(snapshot, "no available executor host");
                continue;
            }
            String executorHost = reservations.get(index);
            try {
                ScheduleJobSnapshot claimedSnapshot = snapshot;
                jobDispatchExecutor.execute(() -> dispatchSingleJob(claimedSnapshot, executorHost));
                submitted++;
            } catch (RejectedExecutionException e) {
                safeReleaseExecutor(executorHost);
                requeueClaimedJob(snapshot, "dispatch executor is saturated");
            }
        }
        return submitted;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public int recoverTimedOutJobs() {
        if (properties.isBenchmarkNoWorkerMode()) {
            return 0;
        }
        if (ownedShards.isEmpty()) {
            return 0;
        }
        int recovered = 0;
        long nowMillis = System.currentTimeMillis();
        for (Integer shard : ownedShards.stream().sorted().toList()) {
            List<String> jobIds = scheduleJobRedisSupport.recoverExpiredInflightJobs(
                    shard,
                    nowMillis,
                    properties.getScanBatchSize()
            );
            for (String jobIdText : jobIds) {
                if (!StringUtils.hasText(jobIdText)) {
                    continue;
                }
                Long jobId = Long.valueOf(jobIdText);
                ScheduleJobDO job = scheduleJobMapper.selectById(jobId);
                if (job == null) {
                    clearRedisMembership(jobId, shard);
                    continue;
                }
                if (!JobStatus.WAITING.name().equals(job.getStatus())) {
                    evictRedisState(job);
                    continue;
                }
                ScheduleJobSnapshot runtimeSnapshot = scheduleJobRedisSupport.loadSnapshot(jobId, shard);
                if (runtimeSnapshot == null || !StringUtils.hasText(runtimeSnapshot.getDispatchTarget())) {
                    scheduleJobRedisSupport.cacheDetail(toSnapshot(job), shard);
                    scheduleJobMapper.markHotCached(jobId, 1);
                    recovered++;
                    continue;
                }
                LocalDateTime nextExecuteTime = LocalDateTime.now();
                int updated = scheduleJobMapper.markRetryWaitingFromInflight(
                        jobId,
                        defaultValue(job.getRetryTimes(), 0),
                        nextExecuteTime,
                        "claim timeout recovered",
                        1
                );
                if (updated == 0) {
                    ScheduleJobDO latest = scheduleJobMapper.selectById(jobId);
                    if (latest == null) {
                        clearRedisMembership(jobId, shard);
                    } else if (JobStatus.WAITING.name().equals(latest.getStatus()) && shouldPreheat(latest.getExecuteTime())) {
                        cacheJob(toSnapshot(latest), latest.getExecuteTime());
                        scheduleJobMapper.markHotCached(jobId, 1);
                    } else if (!JobStatus.WAITING.name().equals(latest.getStatus())) {
                        evictRedisState(latest);
                    }
                    continue;
                }
                safeReleaseExecutor(resolveExecutorHost(runtimeSnapshot, null));
                job.setStatus(JobStatus.WAITING.name());
                job.setExecuteTime(nextExecuteTime);
                job.setHotCached(1);
                job.setClaimOwner(null);
                job.setClaimTime(null);
                job.setClaimDeadline(null);
                job.setDispatchTarget(null);
                job.setDispatchNote("claim timeout recovered");
                scheduleJobRedisSupport.cacheDetail(toSnapshot(job), shard);
                recovered++;
            }
        }
        return recovered;
    }

    private int benchmarkDispatchWithoutWorker(List<ScheduleJobSnapshot> claimedSnapshots) {
        int dispatched = 0;
        String benchmarkExecutorHost = resolveBenchmarkExecutorHost();
        for (ScheduleJobSnapshot snapshot : claimedSnapshots) {
            if (markRedisDispatchTarget(snapshot, benchmarkExecutorHost)) {
                dispatched++;
            }
        }
        return dispatched;
    }

    private void dispatchSingleJob(ScheduleJobSnapshot snapshot, String selectedExecutorHost) {
        try {
            if (!markRedisDispatchTarget(snapshot, selectedExecutorHost)) {
                safeReleaseExecutor(selectedExecutorHost);
                return;
            }
            // The worker ACK only confirms acceptance; actual execution completes asynchronously.
            R workerResponse = workerRemoteClient.execute(
                    toWorkerExecuteRequest(snapshot, selectedExecutorHost),
                    selectedExecutorHost
            );
            if (workerResponse == null || workerResponse.getCode() == null || workerResponse.getCode() != 200) {
                requeueClaimedJob(snapshot, "worker rejected: "
                        + (workerResponse == null ? "null response" : workerResponse.getMessage()));
                safeReleaseExecutor(selectedExecutorHost);
                return;
            }
            WorkerExecuteResponse executeResponse = objectMapper.convertValue(workerResponse.getData(), WorkerExecuteResponse.class);
            if (executeResponse == null || !Boolean.TRUE.equals(executeResponse.getAccepted())) {
                requeueClaimedJob(snapshot, "worker did not accept the job");
                safeReleaseExecutor(selectedExecutorHost);
            }
        } catch (Exception e) {
            requeueClaimedJob(snapshot, "dispatch exception: " + e.getMessage());
            if (StringUtils.hasText(selectedExecutorHost)) {
                safeReleaseExecutor(selectedExecutorHost);
            }
        }
    }

    @Transactional(rollbackFor = Exception.class)
    protected void requeueClaimedJob(ScheduleJobSnapshot snapshot, String reason) {
        ScheduleJobDO job = requireJob(snapshot.getId());
        if (!JobStatus.WAITING.name().equals(job.getStatus())) {
            return;
        }
        int shard = resolveShard(job.getShardKey());
        clearRedisMembership(job.getId(), shard);

        int nextRetryTimes = defaultValue(job.getRetryTimes(), 0) + 1;
        if (nextRetryTimes > defaultValue(job.getMaxRetryTimes(), 3)) {
            scheduleJobMapper.markFailed(job.getId(), reason);
            job.setStatus(JobStatus.FAILED.name());
            job.setHotCached(0);
            job.setClaimOwner(null);
            job.setClaimTime(null);
            job.setClaimDeadline(null);
            job.setDispatchTarget(null);
            job.setDispatchNote(reason);
            return;
        }
        LocalDateTime nextExecuteTime = LocalDateTime.now().plusSeconds(properties.getRetryDelaySeconds());
        boolean hotCached = shouldPreheat(nextExecuteTime);
        int updated = scheduleJobMapper.markRetryWaitingFromInflight(
                job.getId(),
                nextRetryTimes,
                nextExecuteTime,
                reason,
                hotCached ? 1 : 0
        );
        if (updated == 0) {
            return;
        }
        job.setStatus(JobStatus.WAITING.name());
        job.setRetryTimes(nextRetryTimes);
        job.setExecuteTime(nextExecuteTime);
        job.setHotCached(hotCached ? 1 : 0);
        job.setClaimOwner(null);
        job.setClaimTime(null);
        job.setClaimDeadline(null);
        job.setDispatchTarget(null);
        job.setDispatchNote(reason);
        if (hotCached) {
            cacheJob(toSnapshot(job), nextExecuteTime);
        }
    }

    private void cacheJob(ScheduleJobSnapshot snapshot, LocalDateTime executeTime) {
        int shard = resolveShard(snapshot.getShardKey());
        scheduleJobRedisSupport.cacheJob(snapshot, executeTime, shard);
    }

    private void evictRedisState(ScheduleJobDO job) {
        clearRedisState(job);
        scheduleJobMapper.markHotCached(job.getId(), 0);
    }

    private void clearRedisState(ScheduleJobDO job) {
        clearRedisMembership(job.getId(), resolveShard(job.getShardKey()));
    }

    private void clearRedisMembership(Long jobId, int shard) {
        scheduleJobRedisSupport.removeFromReady(jobId, shard);
        scheduleJobRedisSupport.removeFromInflight(jobId, shard);
        scheduleJobRedisSupport.removeDetail(jobId, shard);
    }

    private Set<Integer> computeDesiredShards(List<String> activeMembers) {
        Set<Integer> desiredShards = new LinkedHashSet<>();
        if (activeMembers.isEmpty()) {
            return desiredShards;
        }
        int memberCount = activeMembers.size();
        // Assign shards by round-robin over the sorted active scheduler list.
        for (int shard = 0; shard < properties.getShardCount(); shard++) {
            if (instanceId.equals(activeMembers.get(shard % memberCount))) {
                desiredShards.add(shard);
            }
        }
        return desiredShards;
    }

    private ScheduleJobDO requireJob(Long id) {
        ScheduleJobDO job = scheduleJobMapper.selectById(id);
        if (job == null) {
            throw new IllegalArgumentException("job does not exist");
        }
        return job;
    }

    private CallbackContext resolveCallbackContext(Long id, JobExecutionCallbackRequest request) {
        if (request != null && StringUtils.hasText(request.getShardKey())) {
            String shardKey = request.getShardKey().trim();
            int shard = resolveShard(shardKey);
            ScheduleJobSnapshot snapshot = scheduleJobRedisSupport.loadSnapshot(id, shard);
            if (snapshot != null) {
                if (!StringUtils.hasText(snapshot.getShardKey())) {
                    snapshot.setShardKey(shardKey);
                }
                return new CallbackContext(shard, snapshot);
            }
        }
        ScheduleJobDO job = scheduleJobMapper.selectById(id);
        if (job == null) {
            return null;
        }
        return new CallbackContext(resolveShard(job.getShardKey()), toSnapshot(job));
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

    private ScheduleJobSnapshot resolveRuntimeSnapshot(ScheduleJobDO job) {
        if (job == null) {
            return null;
        }
        if (JobStatus.ARCHIVED.name().equals(job.getStatus()) || !StringUtils.hasText(job.getShardKey())) {
            return toSnapshot(job);
        }
        ScheduleJobSnapshot cached = scheduleJobRedisSupport.loadSnapshot(job.getId(), resolveShard(job.getShardKey()));
        return cached != null ? cached : toSnapshot(job);
    }

    private boolean markRedisDispatchTarget(ScheduleJobSnapshot snapshot, String executorHost) {
        if (snapshot == null || snapshot.getId() == null || !StringUtils.hasText(snapshot.getShardKey())
                || !StringUtils.hasText(executorHost)) {
            return false;
        }
        String normalizedHost = executorHost.trim();
        snapshot.setDispatchTarget(normalizedHost);
        snapshot.setDispatchNote("job dispatched to host " + normalizedHost);
        return scheduleJobRedisSupport.updateInflightSnapshot(snapshot, resolveShard(snapshot.getShardKey()));
    }

    private WorkerExecuteRequest toWorkerExecuteRequest(ScheduleJobSnapshot snapshot, String executorHost) {
        WorkerExecuteRequest request = new WorkerExecuteRequest();
        request.setJobId(snapshot.getId());
        request.setJobName(snapshot.getJobName());
        request.setRouteKey(snapshot.getRouteKey());
        request.setShardKey(snapshot.getShardKey());
        request.setPayloadJson(snapshot.getPayloadJson());
        request.setPriority(snapshot.getPriority());
        request.setExecutorHost(executorHost);
        return request;
    }

    private String resolveExecutorHost(ScheduleJobSnapshot snapshot, JobExecutionCallbackRequest request) {
        if (request != null && StringUtils.hasText(request.getExecutorHost())) {
            return request.getExecutorHost().trim();
        }
        return snapshot == null ? null : snapshot.getDispatchTarget();
    }

    private String resolveBenchmarkExecutorHost() {
        if (StringUtils.hasText(properties.getBenchmarkExecutorHost())) {
            return properties.getBenchmarkExecutorHost().trim();
        }
        return "benchmark-local";
    }

    private String resolveCallbackMessage(JobExecutionCallbackRequest request) {
        if (StringUtils.hasText(request.getMessage())) {
            return request.getMessage().trim();
        }
        return Boolean.TRUE.equals(request.getSuccess()) ? "worker callback success" : "worker callback failed";
    }

    private void releaseExecutor(String executorHost) {
        executorSelectionSupport.release(executorHost);
    }

    private void safeReleaseExecutor(String executorHost) {
        try {
            releaseExecutor(executorHost);
        } catch (Exception ignored) {
            // Ignore release failures on the dispatch path and let heartbeat correct the load later.
        }
    }

    private boolean shouldPreheat(LocalDateTime executeTime) {
        return executeTime.isBefore(LocalDateTime.now().plusMinutes(properties.getPreheatMinutes()));
    }

    private int resolveShard(String shardKey) {
        return Math.floorMod(shardKey.hashCode(), properties.getShardCount());
    }

    private void validateCreateRequest(CreateJobRequest request) {
        if (!StringUtils.hasText(request.getJobName())) {
            throw new IllegalArgumentException("jobName is required");
        }
        if (!StringUtils.hasText(request.getRouteKey())) {
            throw new IllegalArgumentException("routeKey is required");
        }
        if (request.getExecuteTime() == null) {
            throw new IllegalArgumentException("executeTime is required");
        }
        if (request.getExecuteTime().isBefore(LocalDateTime.now().minusSeconds(5))) {
            throw new IllegalArgumentException("executeTime cannot be in the past");
        }
    }

    private String generateShardKey() {
        return "job:" + UUID.randomUUID();
    }

    private int defaultValue(Integer value, int defaultValue) {
        return value == null ? defaultValue : value;
    }

    private long defaultLong(Long value) {
        return value == null ? 0L : value;
    }

    private record CallbackContext(int shard, ScheduleJobSnapshot snapshot) {
    }
}



