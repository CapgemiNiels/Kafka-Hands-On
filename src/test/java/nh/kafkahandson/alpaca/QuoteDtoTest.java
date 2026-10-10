package nh.kafkahandson.alpaca;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import nh.kafkahandson.market.Quote;
import org.junit.jupiter.api.Test;

class QuoteDtoTest {

    @Test
    void toQuoteUsesGivenSymbolAndCopiesAllFields() {
        var timestamp = Instant.parse("2026-10-08T18:05:24Z");
        var dto = new QuoteDto(new BigDecimal("1.5"), 10, "B", new BigDecimal("1.6"), 20, "A", timestamp);

        assertThat(dto.toQuote("AAPL")).isEqualTo(
                new Quote("AAPL", new BigDecimal("1.5"), 10, "B", new BigDecimal("1.6"), 20, "A", timestamp));
    }
}
