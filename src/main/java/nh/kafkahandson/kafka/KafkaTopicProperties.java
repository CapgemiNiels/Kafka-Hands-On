package nh.kafkahandson.kafka;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("app.kafka")
record KafkaTopicProperties(@NotBlank String quoteTopic) {
}
