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
        // TODO: Produce quote to Kafka:
        //  1. Validate quote is not null (e.g. Objects.requireNonNull or let serialization fail appropriately).
        //  2. Serialize the Quote to a JSON String using objectMapper.writeValueAsString(quote).
        //  3. Send to Kafka via kafkaTemplate.send(topic, quote.symbol(), value).
        //     - topic: the configured quote topic name
        //     - key: quote.symbol() (ensures partition affinity by stock symbol)
        //     - value: serialized JSON string
        //  4. Attach whenComplete callback on the returned CompletableFuture:
        //     - On failure (ex != null): log error with symbol and exception.
        //     - On success: log info with key, metadata.topic(), metadata.partition(), metadata.offset().
        //  5. Return the CompletableFuture.
        throw new UnsupportedOperationException("TODO: implement send");
    }
}
