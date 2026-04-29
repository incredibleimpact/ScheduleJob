package archiver;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;
import archiver.config.ArchiverProperties;
import archiver.service.ScheduleJobArchiverService;
import scheduler.config.ScheduleJobProperties;
import scheduler.support.ScheduleJobRedisSupport;

/**
 * Archive service entry point.
 */
@MapperScan("scheduler.mapper")
@EnableScheduling
@SpringBootApplication(scanBasePackageClasses = {
        ArchiverMainApplication.class,
        ScheduleJobArchiverService.class,
        ScheduleJobRedisSupport.class,
        ScheduleJobProperties.class
})
@EnableConfigurationProperties({ScheduleJobProperties.class, ArchiverProperties.class})
public class ArchiverMainApplication {
    public static void main(String[] args) {
        SpringApplication.run(ArchiverMainApplication.class, args);
    }
}
