package org.github.dbjo.kafka.listener.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaListenerApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaListenerApplication.class, args);
    }
}
