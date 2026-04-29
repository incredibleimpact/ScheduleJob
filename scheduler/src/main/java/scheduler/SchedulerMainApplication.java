package scheduler;


import scheduler.config.ScheduleJobProperties;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Scheduler service entry point.
 */
@MapperScan("scheduler.mapper")
@EnableScheduling
@SpringBootApplication
@EnableConfigurationProperties(ScheduleJobProperties.class)
public class SchedulerMainApplication {
    public static void main(String[] args) {
        SpringApplication.run(SchedulerMainApplication.class, args);
    }
}
