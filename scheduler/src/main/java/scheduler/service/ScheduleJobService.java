package scheduler.service;


import common.CreateJobRequest;
import common.JobExecutionCallbackRequest;
import common.R;
import scheduler.model.ScheduleDashboardView;
import scheduler.model.ScheduleJobSnapshot;

import java.util.List;

/**
 * Service contract for scheduled jobs.
 */
public interface ScheduleJobService {
    R createJob(CreateJobRequest request, Long creatorId);

    ScheduleJobSnapshot getJob(Long id);

    List<ScheduleJobSnapshot> listJobs(String status);

    ScheduleDashboardView dashboard();

    R pauseJob(Long id);

    R resumeJob(Long id);

    R completeJob(Long id, JobExecutionCallbackRequest request);

    int refreshShardLeases();

    int preheatHotJobs();

    int dispatchDueJobs();

    int recoverTimedOutJobs();
}
