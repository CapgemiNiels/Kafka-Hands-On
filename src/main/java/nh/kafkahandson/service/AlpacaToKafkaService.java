package nh.kafkahandson.service;

import nh.kafkahandson.alpaca.LatestQuotesRunner;
import nh.kafkahandson.kafka.KafkaProducer;
import nh.kafkahandson.market.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
public class AlpacaToKafkaService {

    private static final Logger log = LoggerFactory.getLogger(AlpacaToKafkaService.class);

    private final LatestQuotesRunner latestQuotesRunner;
    private final KafkaProducer kafkaProducer;

    public AlpacaToKafkaService(LatestQuotesRunner latestQuotesRunner, KafkaProducer kafkaProducer) {
        this.latestQuotesRunner = latestQuotesRunner;
        this.kafkaProducer = kafkaProducer;
    }

    public void fetchAndPublish() {
        List<Quote> quotes = latestQuotesRunner.run();

        var futures = quotes.stream()
                .map(kafkaProducer::send)
                .toList();

        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .exceptionally(ex -> null)   // failures are already logged per record
                .join();

        long ok = futures.stream().filter(f -> !f.isCompletedExceptionally()).count();
        log.info("Published {}/{} quotes", ok, quotes.size());
    }
}
