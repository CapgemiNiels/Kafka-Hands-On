package nh.kafkahandson.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class KafkaTopicPropertiesTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner().withUserConfiguration(PropertiesConfig.class);

    @EnableConfigurationProperties(KafkaTopicProperties.class)
    static class PropertiesConfig {
    }

    @Test
    void bindsQuoteTopic() {
        runner.withPropertyValues("app.kafka.quote-topic=quotes-topic")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean(KafkaTopicProperties.class).quoteTopic()).isEqualTo("quotes-topic");
                });
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    void failsToStartWhenQuoteTopicIsBlank(String blank) {
        runner.withPropertyValues("app.kafka.quote-topic=" + blank)
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void failsToStartWhenQuoteTopicIsMissing() {
        runner.run(context -> assertThat(context).hasFailed());
    }
}
