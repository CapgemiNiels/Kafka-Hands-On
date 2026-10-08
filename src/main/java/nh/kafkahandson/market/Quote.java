package nh.kafkahandson.market;

import java.math.BigDecimal;
import java.time.Instant;

public record Quote(
        String symbol,
        BigDecimal bidPrice,
        long bidSize,
        String bidExchange,
        BigDecimal askPrice,
        long askSize,
        String askExchange,
        Instant timestamp) {
}
