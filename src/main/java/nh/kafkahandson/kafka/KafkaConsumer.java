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

    // Group id and offset behaviour come from spring.kafka.consumer.* in application.yaml.
    // Any exception thrown here is handled by the error handler in KafkaConsumerConfig.
    @KafkaListener(id = "quote-db-writer", idIsGroup = false, topics = "${app.kafka.quote-topic}")
    void onMessage(ConsumerRecord<String, String> record) {
        log.info("Consumed quote with key:{} from topic:{} partition:{} offset:{} and sent to postgres successfully",
                record.key(), record.topic(), record.partition(), record.offset());
        Quote quote = objectMapper.readValue(record.value(), Quote.class);
        storageService.store(quote);
    }
}
