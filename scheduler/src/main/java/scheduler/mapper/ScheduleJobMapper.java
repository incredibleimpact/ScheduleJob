package scheduler.mapper;


import scheduler.domain.ScheduleJobDO;
import org.apache.ibatis.annotations.Param;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Mapper for scheduled jobs.
 */
public interface ScheduleJobMapper {
    int insert(ScheduleJobDO scheduleJobDO);

    ScheduleJobDO selectById(@Param("id") Long id);

    List<ScheduleJobDO> selectJobs(@Param("status") String status, @Param("limit") Integer limit);

    List<ScheduleJobDO> selectPreheatJobs(@Param("fromTime") LocalDateTime fromTime,
                                          @Param("toTime") LocalDateTime toTime,
                                          @Param("limit") Integer limit);

    List<ScheduleJobDO> selectHotCachedWaitingJobs(@Param("toTime") LocalDateTime toTime,
                                                   @Param("limit") Integer limit);

    List<ScheduleJobDO> selectArchiveCandidates(@Param("beforeTime") LocalDateTime beforeTime);

    ScheduleJobDO selectArchiveById(@Param("id") Long id);

    List<ScheduleJobDO> selectArchiveJobs(@Param("limit") Integer limit);

    List<ScheduleJobDO> selectArchiveFileCandidates();

    int markHotCached(@Param("id") Long id, @Param("hotCached") Integer hotCached);

    int markWaiting(@Param("id") Long id,
                    @Param("dispatchNote") String dispatchNote,
                    @Param("hotCached") Integer hotCached);

    int markRetryWaitingFromInflight(@Param("id") Long id,
                                     @Param("retryTimes") Integer retryTimes,
                                     @Param("executeTime") LocalDateTime executeTime,
                                     @Param("dispatchNote") String dispatchNote,
                                     @Param("hotCached") Integer hotCached);

    int markPaused(@Param("id") Long id, @Param("dispatchNote") String dispatchNote);

    int markCompletedOrFailed(@Param("id") Long id,
                              @Param("status") String status,
                              @Param("dispatchNote") String dispatchNote);

    int updateRetryState(@Param("id") Long id,
                         @Param("retryTimes") Integer retryTimes,
                         @Param("executeTime") LocalDateTime executeTime,
                         @Param("dispatchNote") String dispatchNote,
                         @Param("hotCached") Integer hotCached);

    int markFailed(@Param("id") Long id, @Param("dispatchNote") String dispatchNote);

    int upsertArchiveJob(ScheduleJobDO scheduleJobDO);

    int deleteHotJob(@Param("id") Long id, @Param("status") String status);

    int updateArchivePath(@Param("id") Long id, @Param("archivePath") String archivePath);

    Long countAll();

    Long countByStatus(@Param("status") String status);

    Long countHotCached();

    Long countArchiveAll();
}
