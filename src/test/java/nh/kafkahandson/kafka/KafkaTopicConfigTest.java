package nh.kafkahandson.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Test;

class KafkaTopicConfigTest {

    private final NewTopic topic = new KafkaTopicConfig().quoteTopic(new KafkaTopicProperties("quotes-topic"));

    @Test
    void usesConfiguredTopicName() {
        assertThat(topic.name()).isEqualTo("quotes-topic");
    }

    @Test
    void hasThreePartitions() {
        assertThat(topic.numPartitions()).isEqualTo(3);
    }

    @Test
    void hasReplicationFactorThree() {
        assertThat(topic.replicationFactor()).isEqualTo((short) 3);
    }

    @Test
    void requiresAllThreeReplicasInSync() {
        assertThat(topic.configs()).containsEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3");
    }

    @Test
    void retainsMessagesForSevenDays() {
        assertThat(topic.configs())
                .containsEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(Duration.ofDays(7).toMillis()));
    }

    private final NewTopic deadLetterTopic =
            new KafkaTopicConfig().quoteDeadLetterTopic(new KafkaTopicProperties("quotes-topic"));

    @Test
    void deadLetterTopicNameIsQuoteTopicWithDltSuffix() {
        assertThat(deadLetterTopic.name()).isEqualTo("quotes-topic.DLT");
    }

    @Test
    void deadLetterTopicHasAtLeastAsManyPartitionsAsQuoteTopic() {
        assertThat(deadLetterTopic.numPartitions()).isGreaterThanOrEqualTo(topic.numPartitions());
    }

    @Test
    void deadLetterTopicRequiresTwoInSyncReplicas() {
        assertThat(deadLetterTopic.configs()).containsEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2");
    }

    @Test
    void deadLetterTopicRetainsMessagesForThirtyDays() {
        assertThat(deadLetterTopic.configs())
                .containsEntry(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(Duration.ofDays(30).toMillis()));
    }
}