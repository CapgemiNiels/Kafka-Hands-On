package nh.kafkahandson.kafka;

import nh.kafkahandson.market.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.CompletableFuture;

@Component
public class KafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    KafkaProducer(KafkaTemplate<String, String> kafkaTemplate,
                         ObjectMapper objectMapper,
                         KafkaTopicProperties props) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = props.quoteTopic();
    }

    public CompletableFuture<SendResult<String, String>> send(Quote quote) {
        String value = objectMapper.writeValueAsString(quote);
        return kafkaTemplate.send(topic, quote.symbol(), value)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish quote for {}", quote.symbol(), ex);
                    } else {
                        var md = result.getRecordMetadata();
                        log.info("Produced quote with key:{} to topic:{} on partition:{} with offset:{}", quote.symbol(), md.topic(), md.partition(), md.offset());
                    }
                });
    }
}
