# Kafka Hands-On

A small Spring Boot application that shows Apache Kafka end to end:

**Alpaca market data (REST)** -> **Kafka producer** -> **Kafka topic (3-broker cluster)** -> **Kafka consumer** -> **PostgreSQL**

The Kafka part is the point of the project and is explained in depth in [kafka-config/kafkaconfig.md](kafka-config/kafkaconfig.md).
This README only covers the supporting scaffolding.

## Stack

Java 25, Spring Boot 4.1.1, Maven, Spring for Apache Kafka, Spring Data JDBC, Flyway, PostgreSQL.

## Project layout

| Package | Purpose |
|---|---|
| `alpaca` | REST client for the Alpaca market data API (not Kafka related). |
| `market` | `Quote`, the domain record shared by all layers. |
| `service` | `AlpacaToKafkaService` (fetch and publish) and `QuoteStorageService` (store). |
| `scheduler` | `QuoteScheduler` triggers a fetch every 5 minutes (`app.quotes.poll-interval`). |
| `kafka` | Producer, consumer, topic definitions and error handling. See the Kafka guide. |
| `persistence` | Spring Data JDBC repository for the `quote` table. |

Flyway migrations are in `src/main/resources/db/migration`.

### The Alpaca part
* `AlpacaClientConfig` builds a `RestClient` with the base URL and the API key headers.
* `AlpacaMarketDataClient` calls `GET /v2/stocks/quotes/latest` for a list of symbols and maps the response to `Quote` objects. HTTP errors and malformed responses become an `AlpacaClientException`.
* `LatestQuotesRunner` fetches the quotes for the configured symbols and logs them.
* `AlpacaProperties` binds and validates the `alpaca.*` settings.

## Running it

Prerequisites: JDK 25 and Docker.

1. Start the infrastructure (3 Kafka brokers, Kafka UI and PostgreSQL):
   ```
   docker compose up -d
   ```
   | Service | Address |
   |---|---|
   | Kafka brokers | `localhost:9092`, `localhost:9094`, `localhost:9096` |
   | Kafka UI | <http://localhost:8080> |
   | PostgreSQL | `localhost:5432`, database, user and password `quotes` (local development only) |

2. Provide your Alpaca credentials (a free account is enough) as environment variables:
   ```
   ALPACA_API_KEY=<your key>
   ALPACA_API_SECRET=<your secret>
   ```
3. Start the application:
   ```
   ./mvnw spring-boot:run
   ```
   The topics are created on startup. After 10 seconds the first poll runs, quotes appear in Kafka UI and in the `quote` table.

Check the stored data:
```
docker exec -it postgres psql -U quotes -d quotes -c "select symbol, bid_price, ask_price, quoted_at from quote order by id desc limit 10;"
```

## Configuration

Everything is in `src/main/resources/application.yaml`: Kafka producer and consumer settings (commented), datasource, topic name, poll interval, Alpaca symbols and feed.

## Tests

```
./mvnw test
```
Unit tests use Mockito and the Spring test slices. `QuoteRepositoryTest` runs against a real PostgreSQL with Testcontainers and is skipped automatically when Docker is not running. The `test` profile uses an in-memory H2 database so the context test needs no infrastructure.
