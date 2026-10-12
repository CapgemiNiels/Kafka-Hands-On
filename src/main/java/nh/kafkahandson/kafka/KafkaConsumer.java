package nh.kafkahandson.kafka;

import nh.kafkahandson.market.Quote;
import nh.kafkahandson.service.QuoteStorageService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    private final ObjectMapper objectMapper;
    private final QuoteStorageService storageService;

    KafkaConsumer(ObjectMapper objectMapper, QuoteStorageService storageService) {
        this.objectMapper = objectMapper;
        this.storageService = storageService;
    }

    // TODO: Annotate with @KafkaListener:
    //  - id: "quote-db-writer"
    //  - idIsGroup: false (so group id is inherited from application.yaml: spring.kafka.consumer.group-id)
    //  - topics: "${app.kafka.quote-topic}"
    //
    // TODO: Implement onMessage handler:
    //  1. Log received record (key, topic, partition, offset).
    //  2. Deserialize record.value() to Quote using objectMapper.readValue(record.value(), Quote.class).
    //  3. Store quote using storageService.store(quote).
    @KafkaListener(id = "quote-db-writer", idIsGroup = false, topics = "${app.kafka.quote-topic}")
    void onMessage(ConsumerRecord<String, String> record) {
        throw new UnsupportedOperationException("TODO: implement onMessage");
    }
}
