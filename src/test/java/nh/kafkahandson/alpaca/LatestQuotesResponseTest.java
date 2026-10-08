package nh.kafkahandson.alpaca;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import nh.kafkahandson.market.Quote;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;
import tools.jackson.databind.json.JsonMapper;

@ActiveProfiles("test")
class LatestQuotesResponseTest {

    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    @Test
    void mapsSingleQuote() throws IOException {
        List<Quote> quotes = read("latest-quote-single.json").toQuotes();

        assertThat(quotes).hasSize(1);
        Quote quote = quotes.getFirst();
        assertThat(quote.symbol()).isEqualTo("AAPL");
        assertThat(quote.bidPrice()).isEqualByComparingTo("338.78");
        assertThat(quote.bidSize()).isEqualTo(40);
        assertThat(quote.askPrice()).isEqualByComparingTo("339.27");
        assertThat(quote.askSize()).isEqualTo(80);
        assertThat(quote.bidExchange()).isEqualTo("V");
        assertThat(quote.askExchange()).isEqualTo("V");
        assertThat(quote.timestamp()).isEqualTo(Instant.parse("2026-10-08T18:05:24.219928048Z"));
    }

    @Test
    void mapsFiveQuotesInAnyOrder() throws IOException {
        List<Quote> quotes = read("latest-quotes-five.json").toQuotes();

        assertThat(quotes).extracting(Quote::symbol)
                .containsExactlyInAnyOrder("AAPL", "MSFT", "GOOGL", "AMZN", "NVDA");
        assertThat(quotes).filteredOn(q -> q.symbol().equals("NVDA")).singleElement()
                .satisfies(q -> {
                    assertThat(q.askPrice()).isEqualByComparingTo(new BigDecimal("230.49"));
                    assertThat(q.askSize()).isEqualTo(400);
                });
        assertThat(quotes).filteredOn(q -> q.symbol().equals("AMZN")).singleElement()
                .satisfies(q -> assertThat(q.bidSize()).isEqualTo(500));
    }

    @Test
    void parsesTimestampsWithVaryingFractionDigits() throws IOException {
        List<Quote> quotes = read("latest-quotes-five.json").toQuotes();

        assertThat(quotes).filteredOn(q -> q.symbol().equals("MSFT")).singleElement()
                .satisfies(q -> assertThat(q.timestamp())
                        .isEqualTo(Instant.parse("2026-10-08T18:05:26.32196487Z")));
    }

    @Test
    void mapsEmptyResponseToNoQuotes() throws IOException {
        assertThat(read("latest-quotes-empty.json").toQuotes()).isEmpty();
    }

    @Test
    void mapsMissingQuotesFieldToNoQuotes() {
        assertThat(new LatestQuotesResponse(null).toQuotes()).isEmpty();
    }

    private LatestQuotesResponse read(String fixture) throws IOException {
        try (InputStream in = getClass().getResourceAsStream("/alpaca/" + fixture)) {
            return jsonMapper.readValue(in, LatestQuotesResponse.class);
        }
    }
}
