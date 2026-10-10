package nh.kafkahandson.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jdbc.test.autoconfigure.DataJdbcTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

// Real PostgreSQL via Testcontainers, so the migration and ON CONFLICT run exactly as in production.
// Skipped automatically when Docker is not available.
@DataJdbcTest
@Testcontainers(disabledWithoutDocker = true)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
class QuoteRepositoryTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:latest");

    private static final OffsetDateTime TIME = OffsetDateTime.of(2026, 10, 8, 18, 5, 24, 0, ZoneOffset.UTC);

    @Autowired
    QuoteRepository repository;

    @Test
    void insertsNewQuote() {
        int inserted = insert("AAPL", TIME);

        assertThat(inserted).isEqualTo(1);
        assertThat(repository.count()).isEqualTo(1);
    }

    @Test
    void ignoresSecondInsertOfSameSymbolAndTime() {
        insert("AAPL", TIME);

        assertThat(insert("AAPL", TIME)).isZero();
        assertThat(repository.count()).isEqualTo(1);
    }

    @Test
    void storesSameSymbolAtDifferentTimes() {
        insert("AAPL", TIME);
        insert("AAPL", TIME.plusSeconds(1));

        assertThat(repository.count()).isEqualTo(2);
    }

    @Test
    void storesDifferentSymbolsAtSameTime() {
        insert("AAPL", TIME);
        insert("MSFT", TIME);

        assertThat(repository.count()).isEqualTo(2);
    }

    @Test
    void rejectsQuoteWithoutSymbol() {
        assertThatThrownBy(() -> insert(null, TIME)).isInstanceOf(DataIntegrityViolationException.class);
    }

    private int insert(String symbol, OffsetDateTime quotedAt) {
        return repository.insertIfAbsent(symbol, new BigDecimal("338.78"), 40, "V",
                new BigDecimal("339.27"), 80, "V", quotedAt);
    }
}
