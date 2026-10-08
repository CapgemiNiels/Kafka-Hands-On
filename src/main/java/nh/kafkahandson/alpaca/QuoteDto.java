package nh.kafkahandson.alpaca;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.time.Instant;
import nh.kafkahandson.market.Quote;

record QuoteDto(
        @JsonProperty("bp") BigDecimal bidPrice,
        @JsonProperty("bs") long bidSize,
        @JsonProperty("bx") String bidExchange,
        @JsonProperty("ap") BigDecimal askPrice,
        @JsonProperty("as") long askSize,
        @JsonProperty("ax") String askExchange,
        @JsonProperty("t") Instant timestamp) {

    Quote toQuote(String symbol) {
        return new Quote(symbol, bidPrice, bidSize, bidExchange, askPrice, askSize, askExchange, timestamp);
    }
}
