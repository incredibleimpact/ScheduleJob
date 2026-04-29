package mq;

import mq.config.MqProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * MQ ingress application entry point.
 */
@SpringBootApplication
@EnableConfigurationProperties(MqProperties.class)
public class MqMainApplication {
    public static void main(String[] args) {
        SpringApplication.run(MqMainApplication.class, args);
    }
}
