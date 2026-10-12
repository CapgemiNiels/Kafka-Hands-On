package nh.kafkahandson.kafka;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

// TODO 1: Bind configuration properties with prefix "app.kafka"
// TODO 2: Ensure validation is active (@Validated)
// TODO 3: Define record component 'quoteTopic' with validation (@NotBlank String quoteTopic)
@Validated
@ConfigurationProperties("app.kafka")
record KafkaTopicProperties(@NotBlank String quoteTopic) {
}

