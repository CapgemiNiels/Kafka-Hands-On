package nh.kafkahandson.persistence;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;

public interface QuoteRepository extends Repository<QuoteEntity, Long> {

    /**
     * Inserts the quote unless one with the same symbol and quote time exists.
     *
     * @return 1 when a row was inserted, 0 when it was a duplicate
     */
    @Modifying
    @Query("""
            INSERT INTO quote (symbol, bid_price, bid_size, bid_exchange, ask_price, ask_size, ask_exchange, quoted_at)
            VALUES (:symbol, :bidPrice, :bidSize, :bidExchange, :askPrice, :askSize, :askExchange, :quotedAt)
            ON CONFLICT (symbol, quoted_at) DO NOTHING
            """)
    int insertIfAbsent(@Param("symbol") String symbol,
                       @Param("bidPrice") BigDecimal bidPrice,
                       @Param("bidSize") long bidSize,
                       @Param("bidExchange") String bidExchange,
                       @Param("askPrice") BigDecimal askPrice,
                       @Param("askSize") long askSize,
                       @Param("askExchange") String askExchange,
                       @Param("quotedAt") OffsetDateTime quotedAt);

    long count();
}
