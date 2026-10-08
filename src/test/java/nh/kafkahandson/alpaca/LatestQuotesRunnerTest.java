package nh.kafkahandson.alpaca;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import nh.kafkahandson.market.Quote;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

@ExtendWith({MockitoExtension.class, OutputCaptureExtension.class})
class LatestQuotesRunnerTest {

    @Mock
    AlpacaMarketDataClient client;

    // Test 10: smoke runner
    @Test
    void logsOneLinePerQuote(CapturedOutput output) {
        AlpacaProperties properties = new AlpacaProperties("http://x", "k", "s", "iex", List.of("AAPL"));
        when(client.getLatestQuotes(List.of("AAPL"))).thenReturn(List.of(
                new Quote("AAPL", new BigDecimal("338.78"), 40, "V",
                        new BigDecimal("339.27"), 80, "V", Instant.parse("2026-10-08T18:05:24Z"))));

        new LatestQuotesRunner(client, properties).run();

        assertThat(output).contains("AAPL").contains("338.78").contains("339.27");
    }
}
