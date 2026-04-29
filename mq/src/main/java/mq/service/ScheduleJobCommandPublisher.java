package mq.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import common.CreateJobRequest;
import common.JobCommandType;
import common.JobExecutionCallbackRequest;
import common.R;
import lombok.RequiredArgsConstructor;
import mq.config.MqProperties;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Publishes scheduler write commands to Redis Stream.
 */
@Service
@RequiredArgsConstructor
public class ScheduleJobCommandPublisher {
    private final StringRedisTemplate stringRedisTemplate;
    private final ObjectMapper objectMapper;
    private final MqProperties mqProperties;

    public R publishCreateJob(CreateJobRequest request, Long creatorId) {
        return accepted(JobCommandType.CREATE_JOB, publish(JobCommandType.CREATE_JOB, null, creatorId, request));
    }

    public R publishPauseJob(Long jobId) {
        return accepted(JobCommandType.PAUSE_JOB, publish(JobCommandType.PAUSE_JOB, jobId, null, null));
    }

    public R publishResumeJob(Long jobId) {
        return accepted(JobCommandType.RESUME_JOB, publish(JobCommandType.RESUME_JOB, jobId, null, null));
    }

    public R publishCompleteJob(Long jobId, JobExecutionCallbackRequest request) {
        return accepted(JobCommandType.COMPLETE_JOB, publish(JobCommandType.COMPLETE_JOB, jobId, null, request));
    }

    private RecordId publish(JobCommandType commandType, Long jobId, Long creatorId, Object body) {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("commandType", commandType.name());
        if (jobId != null) {
            values.put("jobId", String.valueOf(jobId));
        }
        if (creatorId != null) {
            values.put("creatorId", String.valueOf(creatorId));
        }
        if (body != null) {
            values.put("bodyJson", toJson(body));
        }
        values.put("publishedAt", String.valueOf(Instant.now().toEpochMilli()));
        MapRecord<String, String, String> record = StreamRecords.mapBacked(values).withStreamKey(mqProperties.getStreamKey());
        RecordId recordId = stringRedisTemplate.opsForStream().add(record);
        if (recordId == null) {
            throw new IllegalStateException("failed to append command to Redis Stream");
        }
        return recordId;
    }

    private R accepted(JobCommandType commandType, RecordId recordId) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("commandType", commandType.name());
        data.put("streamRecordId", recordId.getValue());
        data.put("streamKey", mqProperties.getStreamKey());
        return R.ok("command accepted", data);
    }

    private String toJson(Object body) {
        try {
            return objectMapper.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize command body", e);
        }
    }
}
