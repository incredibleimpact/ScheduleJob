package preheater;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;
import preheater.config.PreheaterProperties;
import scheduler.config.ScheduleJobProperties;
import scheduler.preheat.ScheduleJobPreheatService;
import scheduler.support.ScheduleJobRedisSupport;

/**
 * Preheater service entry point.
 */
@MapperScan("scheduler.mapper")
@EnableScheduling
@SpringBootApplication(scanBasePackageClasses = {
        PreheaterMainApplication.class,
        ScheduleJobPreheatService.class,
        ScheduleJobRedisSupport.class,
        ScheduleJobProperties.class
})
@EnableConfigurationProperties({ScheduleJobProperties.class, PreheaterProperties.class})
public class PreheaterMainApplication {
    public static void main(String[] args) {
        SpringApplication.run(PreheaterMainApplication.class, args);
    }
}
