package nh.kafkahandson.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import nh.kafkahandson.alpaca.AlpacaClientException;
import nh.kafkahandson.alpaca.LatestQuotesRunner;
import nh.kafkahandson.kafka.KafkaProducer;
import nh.kafkahandson.market.Quote;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.support.SendResult;

@ExtendWith({MockitoExtension.class, OutputCaptureExtension.class})
class AlpacaToKafkaServiceTest {

    @Mock
    LatestQuotesRunner runner;

    @Mock
    KafkaProducer producer;

    private AlpacaToKafkaService service() {
        return new AlpacaToKafkaService(runner, producer);
    }

    @Test
    void publishesEveryFetchedQuote() {
        Quote aapl = quote("AAPL");
        Quote msft = quote("MSFT");
        when(runner.run()).thenReturn(List.of(aapl, msft));
        when(producer.send(aapl)).thenReturn(ok());
        when(producer.send(msft)).thenReturn(ok());

        service().fetchAndPublish();

        verify(producer).send(aapl);
        verify(producer).send(msft);
    }

    @Test
    void fetchesQuotesBeforePublishing() {
        Quote aapl = quote("AAPL");
        when(runner.run()).thenReturn(List.of(aapl));
        when(producer.send(aapl)).thenReturn(ok());

        service().fetchAndPublish();

        InOrder order = Mockito.inOrder(runner, producer);
        order.verify(runner).run();
        order.verify(producer).send(aapl);
    }

    @Test
    void logsAllQuotesPublishedWhenEverySendSucceeds(CapturedOutput output) {
        when(runner.run()).thenReturn(List.of(quote("AAPL"), quote("MSFT")));
        when(producer.send(Mockito.any())).thenReturn(ok());

        service().fetchAndPublish();

        assertThat(output).contains("Published 2/2 quotes");
    }

    @Test
    void doesNotThrowWhenOneSendFails() {
        Quote aapl = quote("AAPL");
        Quote msft = quote("MSFT");
        when(runner.run()).thenReturn(List.of(aapl, msft));
        when(producer.send(aapl)).thenReturn(failed());
        when(producer.send(msft)).thenReturn(ok());

        service().fetchAndPublish();

        verify(producer).send(msft);
    }

    @Test
    void logsPartialCountWhenOneSendFails(CapturedOutput output) {
        when(runner.run()).thenReturn(List.of(quote("AAPL"), quote("MSFT")));
        when(producer.send(Mockito.any())).thenReturn(failed(), ok());

        service().fetchAndPublish();

        assertThat(output).contains("Published 1/2 quotes");
    }

    @Test
    void logsZeroPublishedWhenEverySendFails(CapturedOutput output) {
        when(runner.run()).thenReturn(List.of(quote("AAPL"), quote("MSFT")));
        when(producer.send(Mockito.any())).thenReturn(failed());

        service().fetchAndPublish();

        assertThat(output).contains("Published 0/2 quotes");
    }

    @Test
    void sendsNothingWhenNoQuotesAreFetched() {
        when(runner.run()).thenReturn(List.of());

        service().fetchAndPublish();

        verify(producer, never()).send(Mockito.any());
    }

    @Test
    void logsZeroOfZeroWhenNoQuotesAreFetched(CapturedOutput output) {
        when(runner.run()).thenReturn(List.of());

        service().fetchAndPublish();

        assertThat(output).contains("Published 0/0 quotes");
    }

    @Test
    void propagatesExceptionWhenFetchingQuotesFails() {
        when(runner.run()).thenThrow(new AlpacaClientException("alpaca down"));

        assertThatThrownBy(() -> service().fetchAndPublish())
                .isInstanceOf(AlpacaClientException.class)
                .hasMessage("alpaca down");
    }

    @Test
    void doesNotPublishWhenFetchingQuotesFails() {
        when(runner.run()).thenThrow(new AlpacaClientException("alpaca down"));

        assertThatThrownBy(() -> service().fetchAndPublish()).isInstanceOf(AlpacaClientException.class);

        verifyNoInteractions(producer);
    }

    @Test
    void propagatesExceptionWhenProducerThrowsSynchronously() {
        when(runner.run()).thenReturn(List.of(quote("AAPL")));
        when(producer.send(Mockito.any())).thenThrow(new IllegalStateException("serialization failed"));

        assertThatThrownBy(() -> service().fetchAndPublish())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("serialization failed");
    }

    private static Quote quote(String symbol) {
        return new Quote(symbol, new BigDecimal("1.00"), 10, "V",
                new BigDecimal("1.10"), 20, "V", Instant.parse("2026-10-08T18:05:24Z"));
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<SendResult<String, String>> ok() {
        return CompletableFuture.completedFuture(Mockito.mock(SendResult.class));
    }

    private static CompletableFuture<SendResult<String, String>> failed() {
        return CompletableFuture.failedFuture(new IllegalStateException("send failed"));
    }
}
