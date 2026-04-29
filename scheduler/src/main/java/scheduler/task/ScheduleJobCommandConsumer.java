package scheduler.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import common.CreateJobRequest;
import common.JobCommandMessage;
import common.JobCommandType;
import common.JobExecutionCallbackRequest;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import scheduler.config.ScheduleJobProperties;
import scheduler.service.ScheduleJobService;
import scheduler.support.SchedulerInstanceIdentity;
import scheduler.support.ScheduleJobRedisSupport;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consumes scheduler commands from Redis Stream with an internal dedicated thread pool.
 */
@Slf4j
@Component
public class ScheduleJobCommandConsumer {
    private final StringRedisTemplate stringRedisTemplate;
    private final ObjectMapper objectMapper;
    private final ScheduleJobService scheduleJobService;
    private final ScheduleJobRedisSupport scheduleJobRedisSupport;
    private final ScheduleJobProperties properties;
    private final SchedulerInstanceIdentity schedulerInstanceIdentity;

    private ExecutorService commandConsumerExecutor;

    public ScheduleJobCommandConsumer(StringRedisTemplate stringRedisTemplate,
                                      ObjectMapper objectMapper,
                                      ScheduleJobService scheduleJobService,
                                      ScheduleJobRedisSupport scheduleJobRedisSupport,
                                      ScheduleJobProperties properties,
                                      SchedulerInstanceIdentity schedulerInstanceIdentity) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.objectMapper = objectMapper;
        this.scheduleJobService = scheduleJobService;
        this.scheduleJobRedisSupport = scheduleJobRedisSupport;
        this.properties = properties;
        this.schedulerInstanceIdentity = schedulerInstanceIdentity;
    }

    @PostConstruct
    public void start() {
        if (commandConsumerExecutor != null && !commandConsumerExecutor.isShutdown()) {
            return;
        }
        int threadCount = Math.max(properties.getCommandConsumerThreadCount(), 1);
        scheduleJobRedisSupport.ensureCommandConsumerGroup(
                properties.getCommandStreamKey(),
                properties.getCommandConsumerGroup()
        );
        commandConsumerExecutor = new ThreadPoolExecutor(
                threadCount,
                threadCount,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                new CommandConsumerThreadFactory()
        );
        for (int index = 0; index < threadCount; index++) {
            String consumerName = schedulerInstanceIdentity.getInstanceId() + "-command-" + index;
            commandConsumerExecutor.execute(() -> consumeLoop(consumerName));
        }
        log.info("started {} scheduler command consumers for instance {}", threadCount,
                schedulerInstanceIdentity.getInstanceId());
    }

    @PreDestroy
    public void stop() {
        if (commandConsumerExecutor != null) {
            commandConsumerExecutor.shutdownNow();
            try {
                if (!commandConsumerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("scheduler command consumer executor did not terminate within timeout");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            commandConsumerExecutor = null;
        }
    }

    private void consumeLoop(String consumerName) {
        recoverPendingList(consumerName);
        while (!Thread.currentThread().isInterrupted()) {
            try {
                List<MapRecord<String, Object, Object>> records = stringRedisTemplate.opsForStream().read(
                        Consumer.from(properties.getCommandConsumerGroup(), consumerName),
                        StreamReadOptions.empty()
                                .count(Math.max(properties.getCommandReadBatchSize(), 1))
                                .block(Duration.ofMillis(Math.max(properties.getCommandReadBlockTimeoutMs(), 100L))),
                        StreamOffset.create(properties.getCommandStreamKey(), ReadOffset.lastConsumed())
                );
                if (records == null || records.isEmpty()) {
                    continue;
                }
                for (MapRecord<String, Object, Object> record : records) {
                    handleRecord(record);
                }
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }
                if (containsConnectionFactoryStopping(e)) {
                    return;
                }
                if (containsNoGroup(e)) {
                    recreateCommandConsumerGroup();
                }
                log.error("failed to consume scheduler command, consumer={}", consumerName, e);
                recoverPendingList(consumerName);
            }
        }
    }

    private void handleRecord(MapRecord<String, Object, Object> record) {
        String recordId = record.getId().getValue();
        JobCommandType commandType = null;
        try {
            JobCommandMessage command = toCommandMessage(record.getValue());
            commandType = command.getCommandType();
            dispatchCommand(command);
            acknowledge(record);
        } catch (IllegalArgumentException | IOException e) {
            log.warn("dropping invalid scheduler command, recordId={}, commandType={}, message={}",
                    recordId, commandType, e.getMessage());
            acknowledge(record);
        }
    }

    private void recoverPendingList(String consumerName) {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                List<MapRecord<String, Object, Object>> records = stringRedisTemplate.opsForStream().read(
                        Consumer.from(properties.getCommandConsumerGroup(), consumerName),
                        StreamReadOptions.empty().count(Math.max(properties.getCommandReadBatchSize(), 1)),
                        StreamOffset.create(properties.getCommandStreamKey(), ReadOffset.from("0"))
                );
                if (records == null || records.isEmpty()) {
                    return;
                }
                for (MapRecord<String, Object, Object> record : records) {
                    handleRecord(record);
                }
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }
                if (containsConnectionFactoryStopping(e)) {
                    return;
                }
                if (containsNoGroup(e)) {
                    recreateCommandConsumerGroup();
                }
                log.error("failed to recover pending scheduler commands, consumer={}", consumerName, e);
                sleepQuietly(100L);
            }
        }
    }

    private void dispatchCommand(JobCommandMessage command) throws IOException {
        if (command.getCommandType() == null) {
            throw new IllegalArgumentException("commandType is required");
        }
        switch (command.getCommandType()) {
            case CREATE_JOB -> scheduleJobService.createJob(parseBody(command, CreateJobRequest.class), command.getCreatorId());
            case PAUSE_JOB -> scheduleJobService.pauseJob(requireJobId(command));
            case RESUME_JOB -> scheduleJobService.resumeJob(requireJobId(command));
            case COMPLETE_JOB -> scheduleJobService.completeJob(
                    requireJobId(command),
                    parseBody(command, JobExecutionCallbackRequest.class)
            );
            default -> throw new IllegalArgumentException("unsupported commandType: " + command.getCommandType());
        }
    }

    private Long requireJobId(JobCommandMessage command) {
        if (command.getJobId() == null) {
            throw new IllegalArgumentException("jobId is required");
        }
        return command.getJobId();
    }

    private <T> T parseBody(JobCommandMessage command, Class<T> targetType) throws IOException {
        if (!StringUtils.hasText(command.getBodyJson())) {
            throw new IllegalArgumentException("bodyJson is required");
        }
        return objectMapper.readValue(command.getBodyJson(), targetType);
    }

    private JobCommandMessage toCommandMessage(Map<Object, Object> values) {
        JobCommandMessage message = new JobCommandMessage();
        message.setCommandType(parseCommandType(values.get("commandType")));
        message.setJobId(parseLong(values.get("jobId")));
        message.setCreatorId(parseLong(values.get("creatorId")));
        message.setBodyJson(parseText(values.get("bodyJson")));
        return message;
    }

    private JobCommandType parseCommandType(Object value) {
        String text = parseText(value);
        if (!StringUtils.hasText(text)) {
            return null;
        }
        return JobCommandType.valueOf(text.trim());
    }

    private Long parseLong(Object value) {
        String text = parseText(value);
        if (!StringUtils.hasText(text)) {
            return null;
        }
        return Long.valueOf(text.trim());
    }

    private String parseText(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private void acknowledge(MapRecord<String, Object, Object> record) {
        stringRedisTemplate.opsForStream().acknowledge(
                properties.getCommandStreamKey(),
                properties.getCommandConsumerGroup(),
                record.getId()
        );
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void recreateCommandConsumerGroup() {
        scheduleJobRedisSupport.ensureCommandConsumerGroup(
                properties.getCommandStreamKey(),
                properties.getCommandConsumerGroup()
        );
    }

    private boolean containsNoGroup(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && message.contains("NOGROUP")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private boolean containsConnectionFactoryStopping(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && message.contains("LettuceConnectionFactory is STOPPING")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static final class CommandConsumerThreadFactory implements ThreadFactory {
        private final AtomicInteger threadIndex = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable);
            thread.setName("schedulejob-command-" + threadIndex.getAndIncrement());
            thread.setDaemon(false);
            return thread;
        }
    }
}
