package archiver.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import archiver.config.ArchiverProperties;
import scheduler.domain.ScheduleJobDO;
import scheduler.mapper.ScheduleJobMapper;
import scheduler.model.JobStatus;
import scheduler.model.ScheduleJobSnapshot;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Moves cold jobs to the archive table and exports archive files.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ScheduleJobArchiverService {
    private static final DateTimeFormatter ARCHIVE_FOLDER_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final ScheduleJobMapper scheduleJobMapper;
    private final ArchiverProperties archiverProperties;
    private final ObjectMapper objectMapper;

    public void archiveOnce() {
        int moved = moveColdJobsToArchiveTable();
        int exported = exportArchiveFiles();
        if (moved > 0 || exported > 0) {
            log.info("archive run completed: moved={} exported={}", moved, exported);
        }
    }

    @Transactional(rollbackFor = Exception.class)
    public int moveColdJobsToArchiveTable() {
        List<ScheduleJobDO> jobs = scheduleJobMapper.selectArchiveCandidates(
                LocalDateTime.now().minusMinutes(archiverProperties.getArchiveAfterMinutes())
        );
        int moved = 0;
        for (ScheduleJobDO job : jobs) {
            scheduleJobMapper.upsertArchiveJob(toArchivedJob(job));
            int deleted = scheduleJobMapper.deleteHotJob(job.getId(), job.getStatus());
            if (deleted == 1) {
                moved++;
            }
        }
        return moved;
    }

    public int exportArchiveFiles() {
        List<ScheduleJobDO> jobs = scheduleJobMapper.selectArchiveFileCandidates();
        if (jobs.isEmpty()) {
            return 0;
        }
        Path filePath = resolveDailyArchiveFilePath(LocalDate.now());
        String archivePath = filePath.toString();
        try {
            Map<Long, String> entries = loadExistingArchiveEntries(filePath);
            for (ScheduleJobDO job : jobs) {
                entries.put(job.getId(), serializeSnapshotLine(toSnapshot(job)));
            }
            writeDailyArchiveFile(filePath, entries.values());
        } catch (Exception e) {
            log.warn("failed to export archive file {}", archivePath, e);
            return 0;
        }
        int exported = 0;
        for (ScheduleJobDO job : jobs) {
            try {
                exported += scheduleJobMapper.updateArchivePath(job.getId(), archivePath);
            } catch (Exception e) {
                log.warn("failed to export archive file for job {}", job.getId(), e);
            }
        }
        return exported;
    }

    private ScheduleJobDO toArchivedJob(ScheduleJobDO job) {
        ScheduleJobDO archivedJob = new ScheduleJobDO();
        archivedJob.setId(job.getId());
        archivedJob.setJobName(job.getJobName());
        archivedJob.setRouteKey(job.getRouteKey());
        archivedJob.setShardKey(job.getShardKey());
        archivedJob.setPayloadJson(job.getPayloadJson());
        archivedJob.setExecuteTime(job.getExecuteTime());
        archivedJob.setPriority(job.getPriority());
        archivedJob.setRetryTimes(job.getRetryTimes());
        archivedJob.setMaxRetryTimes(job.getMaxRetryTimes());
        archivedJob.setCreatorId(job.getCreatorId());
        archivedJob.setHotCached(0);
        archivedJob.setStatus(JobStatus.ARCHIVED.name());
        archivedJob.setClaimOwner(null);
        archivedJob.setClaimTime(null);
        archivedJob.setClaimDeadline(null);
        archivedJob.setDispatchTarget(job.getDispatchTarget());
        archivedJob.setDispatchNote(job.getDispatchNote());
        archivedJob.setArchivePath(job.getArchivePath());
        archivedJob.setCreateTime(job.getCreateTime());
        archivedJob.setUpdateTime(job.getUpdateTime());
        return archivedJob;
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

    private Path resolveDailyArchiveFilePath(LocalDate date) {
        String folderName = date.format(ARCHIVE_FOLDER_FORMATTER);
        Path targetDir = Path.of(archiverProperties.getArchiveDir(), folderName);
        return targetDir.resolve("jobs.ndjson");
    }

    private Map<Long, String> loadExistingArchiveEntries(Path filePath) throws Exception {
        Map<Long, String> entries = new LinkedHashMap<>();
        if (!Files.exists(filePath)) {
            return entries;
        }
        for (String line : Files.readAllLines(filePath, StandardCharsets.UTF_8)) {
            if (line == null || line.isBlank()) {
                continue;
            }
            ScheduleJobSnapshot snapshot = objectMapper.readValue(line, ScheduleJobSnapshot.class);
            if (snapshot.getId() == null) {
                throw new IllegalStateException("archive file contains a snapshot without id: " + filePath);
            }
            entries.put(snapshot.getId(), line);
        }
        return entries;
    }

    private void writeDailyArchiveFile(Path filePath, Iterable<String> lines) throws Exception {
        Path targetDir = filePath.getParent();
        Files.createDirectories(targetDir);
        Path tempPath = filePath.resolveSibling(filePath.getFileName() + ".tmp");
        try (BufferedWriter writer = Files.newBufferedWriter(tempPath, StandardCharsets.UTF_8)) {
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
        }
        Files.move(tempPath, filePath, StandardCopyOption.REPLACE_EXISTING);
    }

    private String serializeSnapshotLine(ScheduleJobSnapshot snapshot) throws Exception {
        return objectMapper.writeValueAsString(snapshot);
    }
}
