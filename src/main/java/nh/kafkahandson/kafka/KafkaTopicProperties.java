package nh.kafkahandson.kafka;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("app.kafka")
record KafkaTopicProperties(@NotBlank String quoteTopic) {
}
