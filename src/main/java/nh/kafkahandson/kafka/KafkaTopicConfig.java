package nh.kafkahandson.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import java.time.Duration;

@Configuration
class KafkaTopicConfig {

    // TODO: Define the primary quotes topic bean using TopicBuilder:
    //   - Topic name from props.quoteTopic()
    //   - Partitions: 3
    //   - Replicas: 3
    //   - Min in-sync replicas: 3 (TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)
    //   - Retention: 7 days in ms (TopicConfig.RETENTION_MS_CONFIG, Duration.ofDays(7).toMillis())
    @Bean
    NewTopic quoteTopic(KafkaTopicProperties props) {
        throw new UnsupportedOperationException("TODO: implement quoteTopic bean");
    }

    // TODO: Define the dead-letter topic (DLT) bean using TopicBuilder:
    //   - Topic name: props.quoteTopic() + KafkaConsumerConfig.DEAD_LETTER_SUFFIX
    //   - Partitions: 3
    //   - Replicas: 3
    //   - Min in-sync replicas: 2 (TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)
    //   - Retention: 30 days in ms (TopicConfig.RETENTION_MS_CONFIG, Duration.ofDays(30).toMillis())
    @Bean
    NewTopic quoteDeadLetterTopic(KafkaTopicProperties props) {
        throw new UnsupportedOperationException("TODO: implement quoteDeadLetterTopic bean");
    }
}