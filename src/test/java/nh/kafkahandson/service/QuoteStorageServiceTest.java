package nh.kafkahandson.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import nh.kafkahandson.market.Quote;
import nh.kafkahandson.persistence.QuoteRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

@ExtendWith({MockitoExtension.class, OutputCaptureExtension.class})
class QuoteStorageServiceTest {

    private static final Instant TIMESTAMP = Instant.parse("2026-10-08T18:05:24.219928048Z");

    @Mock
    QuoteRepository repository;

    @Test
    void passesEveryQuoteFieldToRepository() {
        when(repository.insertIfAbsent(any(), any(), org.mockito.ArgumentMatchers.anyLong(), any(), any(),
                org.mockito.ArgumentMatchers.anyLong(), any(), any())).thenReturn(1);

        new QuoteStorageService(repository).store(quote());

        verify(repository).insertIfAbsent("AAPL", new BigDecimal("338.78"), 40L, "B",
                new BigDecimal("339.27"), 80L, "A", OffsetDateTime.of(2026, 10, 8, 18, 5, 24, 219928048, ZoneOffset.UTC));
    }

    @Test
    void returnsTrueWhenQuoteWasInserted() {
        stubInsertResult(1);

        assertThat(new QuoteStorageService(repository).store(quote())).isTrue();
    }

    @Test
    void returnsFalseWhenQuoteWasADuplicate() {
        stubInsertResult(0);

        assertThat(new QuoteStorageService(repository).store(quote())).isFalse();
    }

    @Test
    void logsSkippedDuplicate(CapturedOutput output) {
        stubInsertResult(0);

        new QuoteStorageService(repository).store(quote());

        assertThat(output).contains("Skipped duplicate quote for AAPL");
    }

    @Test
    void doesNotLogSkippedDuplicateWhenInserted(CapturedOutput output) {
        stubInsertResult(1);

        new QuoteStorageService(repository).store(quote());

        assertThat(output).doesNotContain("Skipped duplicate");
    }

    @Test
    void propagatesRepositoryFailure() {
        when(repository.insertIfAbsent(any(), any(), org.mockito.ArgumentMatchers.anyLong(), any(), any(),
                org.mockito.ArgumentMatchers.anyLong(), any(), any()))
                .thenThrow(new IllegalStateException("db down"));

        assertThatThrownBy(() -> new QuoteStorageService(repository).store(quote()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("db down");
    }

    @Test
    void rejectsQuoteWithoutTimestamp() {
        var noTimestamp = new Quote("AAPL", BigDecimal.ONE, 1, "B", BigDecimal.TEN, 1, "A", null);

        assertThatThrownBy(() -> new QuoteStorageService(repository).store(noTimestamp))
                .isInstanceOf(NullPointerException.class);
    }

    private void stubInsertResult(int result) {
        when(repository.insertIfAbsent(any(), any(), org.mockito.ArgumentMatchers.anyLong(), any(), any(),
                org.mockito.ArgumentMatchers.anyLong(), any(), any())).thenReturn(result);
    }

    private static Quote quote() {
        return new Quote("AAPL", new BigDecimal("338.78"), 40, "B",
                new BigDecimal("339.27"), 80, "A", TIMESTAMP);
    }
}
