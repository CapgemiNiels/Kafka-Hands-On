package nh.kafkahandson.alpaca;

import java.util.List;
import java.util.Map;
import nh.kafkahandson.market.Quote;

record LatestQuotesResponse(Map<String, QuoteDto> quotes) {

    List<Quote> toQuotes() {
        if (quotes == null) {
            return List.of();
        }
        return quotes.entrySet().stream()
                .map(entry -> entry.getValue().toQuote(entry.getKey()))
                .toList();
    }
}
