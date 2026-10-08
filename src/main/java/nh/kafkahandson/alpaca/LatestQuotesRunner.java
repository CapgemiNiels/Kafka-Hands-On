package nh.kafkahandson.alpaca;

import java.util.List;
import nh.kafkahandson.market.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
@Profile("!test")
class LatestQuotesRunner {

    private static final Logger log = LoggerFactory.getLogger(LatestQuotesRunner.class);

    private final AlpacaMarketDataClient client;
    private final AlpacaProperties properties;

    LatestQuotesRunner(AlpacaMarketDataClient client, AlpacaProperties properties) {
        this.client = client;
        this.properties = properties;
    }

    @EventListener(ApplicationReadyEvent.class)
    void run() {
        List<Quote> quotes = client.getLatestQuotes(properties.symbols());
        quotes.forEach(quote -> log.info("{} bid={} ask={} at {}",
                quote.symbol(), quote.bidPrice(), quote.askPrice(), quote.timestamp()));
    }
}
