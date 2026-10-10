package nh.kafkahandson.persistence;

import java.math.BigDecimal;
import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("quote")
public record QuoteEntity(
        @Id Long id,
        String symbol,
        BigDecimal bidPrice,
        long bidSize,
        String bidExchange,
        BigDecimal askPrice,
        long askSize,
        String askExchange,
        Instant quotedAt) {
}
