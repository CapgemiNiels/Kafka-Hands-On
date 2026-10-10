package nh.kafkahandson.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListenerContainer;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

class KafkaConsumerConfigTest {

    private KafkaTemplate<String, String> template;
    private DefaultErrorHandler handler;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        template = mock(KafkaTemplate.class);
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        handler = new KafkaConsumerConfig().quoteErrorHandler(template);
    }

    @Test
    void sendsUnparseableMessageStraightToDeadLetterTopicWithoutRetry() {
        boolean recovered = handler.handleOne(listenerFailure(unparseable()), record(), mockConsumer(), mockContainer());

        assertThat(recovered).isTrue();
        assertThat(sentRecord().topic()).isEqualTo("quote-data.DLT");
    }

    @Test
    void sendsIllegalArgumentFailureStraightToDeadLetterTopicWithoutRetry() {
        boolean recovered = handler.handleOne(
                listenerFailure(new IllegalArgumentException("bad quote")), record(), mockConsumer(), mockContainer());

        assertThat(recovered).isTrue();
    }

    @Test
    void keepsOriginalKeyAndValueOnDeadLetterRecord() {
        handler.handleOne(listenerFailure(unparseable()), record(), mockConsumer(), mockContainer());

        assertThat(sentRecord().key()).isEqualTo("AAPL");
        assertThat(sentRecord().value()).isEqualTo("not json");
    }

    @Test
    void letsKafkaChooseDeadLetterPartition() {
        handler.handleOne(listenerFailure(unparseable()), record(), mockConsumer(), mockContainer());

        assertThat(sentRecord().partition()).isNull();
    }

    @Test
    void doesNotRecoverRetryableFailureOnFirstAttempt() {
        boolean recovered = handler.handleOne(
                listenerFailure(new IllegalStateException("db down")), record(), mockConsumer(), mockContainer());

        assertThat(recovered).isFalse();
    }

    @Test
    void doesNotSendRetryableFailureToDeadLetterTopicOnFirstAttempt() {
        handler.handleOne(listenerFailure(new IllegalStateException("db down")), record(), mockConsumer(), mockContainer());

        org.mockito.Mockito.verify(template, org.mockito.Mockito.never()).send(any(ProducerRecord.class));
    }

    private ProducerRecord<String, String> sentRecord() {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(captor.capture());
        return captor.getValue();
    }

    private static ConsumerRecord<String, String> record() {
        return new ConsumerRecord<>("quote-data", 1, 7L, "AAPL", "not json");
    }

    private static ListenerExecutionFailedException listenerFailure(Exception cause) {
        return new ListenerExecutionFailedException("listener failed", cause);
    }

    private static JacksonException unparseable() {
        try {
            JsonMapper.builder().build().readTree("not json");
        } catch (JacksonException e) {
            return e;
        }
        throw new AssertionError("expected parse failure");
    }

    @SuppressWarnings("unchecked")
    private static Consumer<?, ?> mockConsumer() {
        return mock(Consumer.class);
    }

    private static MessageListenerContainer mockContainer() {
        return mock(MessageListenerContainer.class);
    }
}
