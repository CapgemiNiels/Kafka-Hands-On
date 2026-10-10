package nh.kafkahandson.service;

import java.time.ZoneOffset;
import nh.kafkahandson.market.Quote;
import nh.kafkahandson.persistence.QuoteRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class QuoteStorageService {

    private static final Logger log = LoggerFactory.getLogger(QuoteStorageService.class);

    private final QuoteRepository repository;

    public QuoteStorageService(QuoteRepository repository) {
        this.repository = repository;
    }

    /**
     * @return true when the quote was stored, false when it was already present
     */
    @Transactional
    public boolean store(Quote quote) {
        int inserted = repository.insertIfAbsent(
                quote.symbol(),
                quote.bidPrice(), quote.bidSize(), quote.bidExchange(),
                quote.askPrice(), quote.askSize(), quote.askExchange(),
                quote.timestamp().atOffset(ZoneOffset.UTC));
        if (inserted == 0) {
            log.info("Skipped duplicate quote for {} at {}", quote.symbol(), quote.timestamp());
        }
        return inserted > 0;
    }
}
