package com.selftech.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaInfrastructureApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaInfrastructureApplication.class, args);
    }
}
