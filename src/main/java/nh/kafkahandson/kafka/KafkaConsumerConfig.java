package nh.kafkahandson.kafka;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import tools.jackson.core.JacksonException;

@Configuration
class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    static final String DEAD_LETTER_SUFFIX = ".DLT";
    static final int MAX_RETRIES = 3;

    /**
     * Spring Boot applies this bean to the listener container factory automatically.
     * Failing records are retried with backoff, then parked on the dead letter topic so the
     * partition keeps moving. Unparseable messages can never succeed, so they skip the retries.
     */
    @Bean
    DefaultErrorHandler quoteErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + DEAD_LETTER_SUFFIX, -1));

        var backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxAttempts(MAX_RETRIES);

        var handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(JacksonException.class, IllegalArgumentException.class);
        handler.setRetryListeners((record, ex, attempt) ->
                log.warn("Delivery attempt {} failed for {}-{}@{}", attempt,
                        record.topic(), record.partition(), record.offset(), ex));
        return handler;
    }
}
