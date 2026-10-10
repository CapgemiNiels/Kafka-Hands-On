package nh.kafkahandson.kafka;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.math.BigDecimal;
import java.time.Instant;
import nh.kafkahandson.market.Quote;
import nh.kafkahandson.service.QuoteStorageService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.annotation.KafkaListener;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

@ExtendWith({MockitoExtension.class, OutputCaptureExtension.class})
class KafkaConsumerTest {

    private static final String TOPIC = "quote-data";

    @Mock
    QuoteStorageService storageService;

    private final ObjectMapper objectMapper = JsonMapper.builder().build();

    private KafkaConsumer consumer() {
        return new KafkaConsumer(objectMapper, storageService);
    }

    @Test
    void storesQuoteParsedFromMessageValue() {
        consumer().onMessage(record("AAPL", json(quote("AAPL"))));

        ArgumentCaptor<Quote> stored = ArgumentCaptor.forClass(Quote.class);
        verify(storageService).store(stored.capture());
        org.assertj.core.api.Assertions.assertThat(stored.getValue()).isEqualTo(quote("AAPL"));
    }

    @Test
    void logsTopicPartitionAndOffsetOfReceivedRecord(CapturedOutput output) {
        consumer().onMessage(record("AAPL", json(quote("AAPL"))));

        org.assertj.core.api.Assertions.assertThat(output)
                .contains("Consumed quote with key:AAPL from topic:quote-data partition:1 offset:7 and sent to postgres successfully");
    }

    @Test
    void doesNotThrowWhenQuoteWasAlreadyStored() {
        org.mockito.Mockito.when(storageService.store(any())).thenReturn(false);

        assertThatCode(() -> consumer().onMessage(record("AAPL", json(quote("AAPL")))))
                .doesNotThrowAnyException();
    }

    @Test
    void throwsJacksonExceptionWhenValueIsNotJson() {
        assertThatThrownBy(() -> consumer().onMessage(record("AAPL", "not json")))
                .isInstanceOf(JacksonException.class);
    }

    @Test
    void doesNotStoreAnythingWhenValueIsNotJson() {
        assertThatThrownBy(() -> consumer().onMessage(record("AAPL", "not json")));

        verifyNoInteractions(storageService);
    }

    @Test
    void throwsWhenValueIsNull() {
        assertThatThrownBy(() -> consumer().onMessage(record("AAPL", null)))
                .isInstanceOf(RuntimeException.class);
        verifyNoInteractions(storageService);
    }

    @Test
    void propagatesStorageFailureSoTheErrorHandlerCanRetry() {
        doThrow(new IllegalStateException("db down")).when(storageService).store(any());

        assertThatThrownBy(() -> consumer().onMessage(record("AAPL", json(quote("AAPL")))))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("db down");
    }

    @Test
    void listensToConfiguredQuoteTopic() throws NoSuchMethodException {
        KafkaListener listener = KafkaConsumer.class
                .getDeclaredMethod("onMessage", ConsumerRecord.class)
                .getAnnotation(KafkaListener.class);

        org.assertj.core.api.Assertions.assertThat(listener.topics()).containsExactly("${app.kafka.quote-topic}");
    }

    @Test
    void usesGroupIdFromConfigurationInsteadOfListenerId() throws NoSuchMethodException {
        KafkaListener listener = KafkaConsumer.class
                .getDeclaredMethod("onMessage", ConsumerRecord.class)
                .getAnnotation(KafkaListener.class);

        org.assertj.core.api.Assertions.assertThat(listener.idIsGroup()).isFalse();
    }

    private ConsumerRecord<String, String> record(String key, String value) {
        return new ConsumerRecord<>(TOPIC, 1, 7L, key, value);
    }

    private String json(Quote quote) {
        return objectMapper.writeValueAsString(quote);
    }

    private static Quote quote(String symbol) {
        return new Quote(symbol, new BigDecimal("338.78"), 40, "V",
                new BigDecimal("339.27"), 80, "V", Instant.parse("2026-10-08T18:05:24.219928048Z"));
    }
}
