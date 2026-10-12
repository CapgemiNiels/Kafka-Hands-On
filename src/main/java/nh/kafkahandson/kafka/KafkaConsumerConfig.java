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
        // TODO: Configure the error handler:
        //  1. Create a DeadLetterPublishingRecoverer sending failed records to <record.topic() + DEAD_LETTER_SUFFIX>
        //     with partition -1 (let Kafka choose the partition).
        //  2. Create an ExponentialBackOff with initial interval 1000ms, multiplier 2.0, maxAttempts = MAX_RETRIES.
        //  3. Create DefaultErrorHandler with the recoverer and backoff.
        //  4. Add non-retryable exceptions: JacksonException.class and IllegalArgumentException.class.
        //  5. Add a retry listener to log warning on failed delivery attempts.
        //  6. Return the configured handler.
        throw new UnsupportedOperationException("TODO: implement quoteErrorHandler");
    }
}
