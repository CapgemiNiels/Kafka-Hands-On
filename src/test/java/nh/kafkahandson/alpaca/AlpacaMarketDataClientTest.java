package nh.kafkahandson.alpaca;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.startsWith;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.queryParam;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import nh.kafkahandson.market.Quote;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

@ExtendWith(OutputCaptureExtension.class)
class AlpacaMarketDataClientTest {

    private static final String BASE_URL = "http://localhost";
    private static final String QUOTES_URL = BASE_URL + "/v2/stocks/quotes/latest";

    private MockRestServiceServer server;
    private AlpacaMarketDataClient client;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder();
        server = MockRestServiceServer.bindTo(builder).build();

        AlpacaProperties properties =
                new AlpacaProperties(BASE_URL, "test-key", "test-secret", "iex", List.of("AAPL", "MSFT"));
        RestClient restClient = new AlpacaClientConfig().alpacaRestClient(builder, properties);

        client = new AlpacaMarketDataClient(restClient, properties);
    }

    @AfterEach
    void verifyAllExpectedCallsWereMade() {
        server.verify();
    }

    // Test 2: request shape
    @Test
    void requestsLatestQuotesForGivenSymbolsOnIexFeed() throws IOException {
        server.expect(requestTo(startsWith(QUOTES_URL)))
                .andExpect(method(HttpMethod.GET))
                .andExpect(queryParam("symbols", "AAPL,MSFT"))
                .andExpect(queryParam("feed", "iex"))
                .andRespond(withSuccess(fixture("latest-quotes-empty.json"), MediaType.APPLICATION_JSON));

        client.getLatestQuotes(List.of("AAPL", "MSFT"));
    }

    // Test 3: auth headers
    @Test
    void sendsAlpacaAuthenticationHeaders() throws IOException {
        server.expect(requestTo(startsWith(QUOTES_URL)))
                .andExpect(header("APCA-API-KEY-ID", "test-key"))
                .andExpect(header("APCA-API-SECRET-KEY", "test-secret"))
                .andRespond(withSuccess(fixture("latest-quotes-empty.json"), MediaType.APPLICATION_JSON));

        client.getLatestQuotes(List.of("AAPL"));
    }

    // Test 5: end to end with fixtures
    @Test
    void returnsQuotesMappedFromResponse() throws IOException {
        server.expect(requestTo(startsWith(QUOTES_URL)))
                .andRespond(withSuccess(fixture("latest-quotes-five.json"), MediaType.APPLICATION_JSON));

        List<Quote> quotes = client.getLatestQuotes(List.of("AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"));

        assertThat(quotes).extracting(Quote::symbol)
                .containsExactlyInAnyOrder("AAPL", "MSFT", "GOOGL", "AMZN", "NVDA");
        assertThat(quotes).filteredOn(q -> q.symbol().equals("NVDA")).singleElement()
                .satisfies(q -> {
                    assertThat(q.askPrice()).isEqualByComparingTo("230.49");
                    assertThat(q.askSize()).isEqualTo(400);
                });
    }

    @Test
    void returnsNoQuotesWhenResponseBodyIsEmpty() {
        server.expect(requestTo(startsWith(QUOTES_URL)))
                .andRespond(withSuccess("", MediaType.APPLICATION_JSON));

        assertThat(client.getLatestQuotes(List.of("AAPL"))).isEmpty();
    }

    // Test 6: symbol missing from response
    @Test
    void omitsSymbolsMissingFromResponseAndLogsWarning(CapturedOutput output) throws IOException {
        server.expect(requestTo(startsWith(QUOTES_URL)))
                .andRespond(withSuccess(fixture("latest-quotes-unknown-symbol.json"), MediaType.APPLICATION_JSON));

        List<Quote> quotes = client.getLatestQuotes(List.of("AAPL", "ZZZZZZ"));

        assertThat(quotes).extracting(Quote::symbol).containsExactly("AAPL");
        assertThat(output).contains("ZZZZZZ");
    }

    // Test 7: empty input, no HTTP call (the server has no expectations)
    @Test
    void rejectsEmptySymbolListWithoutCallingAlpaca() {
        assertThatThrownBy(() -> client.getLatestQuotes(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullSymbolListWithoutCallingAlpaca() {
        assertThatThrownBy(() -> client.getLatestQuotes(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // Test 8: HTTP errors
    @ParameterizedTest
    @ValueSource(ints = {401, 403, 429, 500, 503})
    void mapsHttpErrorsToAlpacaClientException(int status) {
        server.expect(requestTo(startsWith(QUOTES_URL)))
                .andRespond(withStatus(HttpStatusCode.valueOf(status)));

        assertThatThrownBy(() -> client.getLatestQuotes(List.of("AAPL")))
                .isInstanceOf(AlpacaClientException.class)
                .hasMessageContaining(String.valueOf(status))
                .hasMessageNotContaining("test-secret");
    }

    // Test 9: malformed JSON
    @Test
    void wrapsMalformedJsonInAlpacaClientException() {
        server.expect(requestTo(startsWith(QUOTES_URL)))
                .andRespond(withSuccess("not json", MediaType.APPLICATION_JSON));

        assertThatThrownBy(() -> client.getLatestQuotes(List.of("AAPL")))
                .isInstanceOf(AlpacaClientException.class)
                .hasCauseInstanceOf(RuntimeException.class);
    }

    private static String fixture(String name) throws IOException {
        try (InputStream in = AlpacaMarketDataClientTest.class.getResourceAsStream("/alpaca/" + name)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
