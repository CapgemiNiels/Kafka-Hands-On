package nh.kafkahandson.alpaca;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nh.kafkahandson.market.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class AlpacaMarketDataClient {

    private static final Logger log = LoggerFactory.getLogger(AlpacaMarketDataClient.class);
    private static final String LATEST_QUOTES_PATH = "/v2/stocks/quotes/latest";

    private final RestClient restClient;
    private final AlpacaProperties properties;

    AlpacaMarketDataClient(RestClient alpacaRestClient, AlpacaProperties properties) {
        this.restClient = alpacaRestClient;
        this.properties = properties;
    }

    public List<Quote> getLatestQuotes(List<String> symbols) {
        if (symbols == null || symbols.isEmpty()) {
            throw new IllegalArgumentException("At least one symbol is required");
        }

        LatestQuotesResponse response = fetchLatestQuotes(symbols);
        List<Quote> quotes = response == null ? List.of() : response.toQuotes();

        warnAboutMissingSymbols(symbols, quotes);
        return quotes;
    }

    private LatestQuotesResponse fetchLatestQuotes(List<String> symbols) {
        try {
            return restClient.get()
                    .uri(uri -> uri.path(LATEST_QUOTES_PATH)
                            .queryParam("symbols", String.join(",", symbols))
                            .queryParam("feed", properties.feed())
                            .build())
                    .retrieve()
                    .onStatus(status -> status.isError(), (request, httpResponse) -> {
                        throw new AlpacaClientException(
                                "Alpaca request failed with HTTP status " + httpResponse.getStatusCode().value());
                    })
                    .body(LatestQuotesResponse.class);
        } catch (AlpacaClientException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new AlpacaClientException("Could not fetch or read latest quotes from Alpaca", e);
        }
    }

    private void warnAboutMissingSymbols(List<String> requested, List<Quote> quotes) {
        Set<String> missing = new HashSet<>(requested);
        quotes.forEach(quote -> missing.remove(quote.symbol()));
        if (!missing.isEmpty()) {
            log.warn("Alpaca returned no quote for symbols {}", missing);
        }
    }
}
