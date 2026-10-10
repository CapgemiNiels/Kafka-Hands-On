package nh.kafkahandson.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import nh.kafkahandson.market.Quote;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

@ExtendWith({MockitoExtension.class, OutputCaptureExtension.class})
class KafkaProducerTest {

    private static final String TOPIC = "quotes-topic";

    @Mock
    KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper = JsonMapper.builder().build();
    private KafkaProducer producer;

    @BeforeEach
    void setUp() {
        producer = new KafkaProducer(kafkaTemplate, objectMapper, new KafkaTopicProperties(TOPIC));
    }

    @Test
    void sendsToConfiguredTopic() {
        var sent = successfulSend();
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(sent);

        producer.send(quote("AAPL"));

        verify(kafkaTemplate).send(eq(TOPIC), anyString(), anyString());
    }

    @Test
    void usesSymbolAsMessageKey() {
        var sent = successfulSend();
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(sent);

        producer.send(quote("AAPL"));

        verify(kafkaTemplate).send(eq(TOPIC), eq("AAPL"), anyString());
    }

    @Test
    void sendsQuoteAsJsonThatCanBeReadBack() {
        var sent = successfulSend();
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(sent);
        ArgumentCaptor<String> value = ArgumentCaptor.forClass(String.class);

        producer.send(quote("AAPL"));

        verify(kafkaTemplate).send(eq(TOPIC), eq("AAPL"), value.capture());
        assertThat(objectMapper.readValue(value.getValue(), Quote.class)).isEqualTo(quote("AAPL"));
    }

    @Test
    void returnsFutureThatCompletesWithBrokerResult() throws Exception {
        SendResult<String, String> result = successfulSendResult();
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(result));

        assertThat(producer.send(quote("AAPL")).get()).isSameAs(result);
    }

    @Test
    void logsPublishedTopicPartitionAndOffsetOnSuccess(CapturedOutput output) {
        var sent = successfulSend();
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(sent);

        producer.send(quote("AAPL"));

        assertThat(output).contains("Produced quote with key:AAPL to topic:" + TOPIC + " on partition:2 with offset:42");
    }

    @Test
    void returnedFutureFailsWhenBrokerSendFails() {
        var failure = new IllegalStateException("broker down");
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(failure));

        var future = producer.send(quote("AAPL"));

        assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasCause(failure);
    }

    @Test
    void logsErrorWithSymbolWhenBrokerSendFails(CapturedOutput output) {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        producer.send(quote("AAPL"));

        assertThat(output).contains("Failed to publish quote for AAPL").contains("broker down");
    }

    @Test
    void doesNotSendWhenSerializationFails() {
        ObjectMapper failingMapper = mock(ObjectMapper.class);
        when(failingMapper.writeValueAsString(any())).thenThrow(new IllegalArgumentException("cannot serialize"));
        var failingProducer = new KafkaProducer(kafkaTemplate, failingMapper, new KafkaTopicProperties(TOPIC));

        assertThatThrownBy(() -> failingProducer.send(quote("AAPL")))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    void propagatesExceptionWhenTemplateThrowsSynchronously() {
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenThrow(new IllegalStateException("template closed"));

        assertThatThrownBy(() -> producer.send(quote("AAPL")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("template closed");
    }

    @Test
    void rejectsNullQuote() {
        assertThatThrownBy(() -> producer.send(null)).isInstanceOf(NullPointerException.class);
        verifyNoInteractions(kafkaTemplate);
    }

    private static Quote quote(String symbol) {
        return new Quote(symbol, new BigDecimal("338.78"), 40, "V",
                new BigDecimal("339.27"), 80, "V", Instant.parse("2026-10-08T18:05:24.219928048Z"));
    }

    private static CompletableFuture<SendResult<String, String>> successfulSend() {
        return CompletableFuture.completedFuture(successfulSendResult());
    }

    @SuppressWarnings("unchecked")
    private static SendResult<String, String> successfulSendResult() {
        RecordMetadata metadata = mock(RecordMetadata.class);
        when(metadata.topic()).thenReturn(TOPIC);
        when(metadata.partition()).thenReturn(2);
        when(metadata.offset()).thenReturn(42L);
        SendResult<String, String> result = mock(SendResult.class);
        when(result.getRecordMetadata()).thenReturn(metadata);
        return result;
    }
}
