package scheduler.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

/**
 * Thread pool configuration for job dispatch.
 */
@Configuration
public class DispatchThreadPoolConfig {
    @Bean("jobDispatchExecutor")
    public Executor jobDispatchExecutor(ScheduleJobProperties properties) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(properties.getDispatchCorePoolSize());
        executor.setMaxPoolSize(properties.getDispatchMaxPoolSize());
        executor.setQueueCapacity(properties.getDispatchQueueCapacity());
        executor.setThreadNamePrefix("schedulejob-dispatch-");
        executor.setKeepAliveSeconds(60);
        executor.initialize();
        return executor;
    }
}
