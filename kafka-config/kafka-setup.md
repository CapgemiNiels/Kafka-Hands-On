# Kafka Live-Coding Presentation Guide: From Scaffolding to Green Tests

This guide outlines the step-by-step path to live-code the Kafka implementation in this project, explaining the concepts, why each setting is chosen, and how each step turns red tests green.

---

## Architecture Overview

```text
  Alpaca API -> AlpacaToKafkaService
                      |
                      v
                KafkaProducer (serializes Quote to JSON String)
                      |
                      v  [quote-data topic (3 partitions, RF 3, min.isr 3)]
                Kafka Brokers
                      |
                      v
                KafkaConsumer (@KafkaListener, 3 concurrency threads)
                      |
                      +---> On Success: QuoteStorageService -> PostgreSQL (at-least-once, idempotent insert)
                      |
                      +---> On Failure: KafkaConsumerConfig (DefaultErrorHandler)
                                  |
                                  +--> Non-retryable (bad JSON): Straight to quote-data.DLT
                                  +--> Retryable (DB down): 3 retries (1s, 2s, 4s backoff) -> quote-data.DLT
```

---

## Step 0: Understanding `KafkaTopicProperties`

### What is `KafkaTopicProperties` and what does it do for you?
In Spring Boot, topics should **never be hardcoded** across classes. If the topic name changes in `application.yaml` (e.g., in dev, test, or prod), hardcoded strings break.

`KafkaTopicProperties`:
```java
@Validated
@ConfigurationProperties("app.kafka")
record KafkaTopicProperties(@NotBlank String quoteTopic) {
}
```
1. **Typesafe configuration binding**: Automatically maps `app.kafka.quote-topic` in `application.yaml` to the `quoteTopic()` method using Spring Boot's `@ConfigurationPropertiesScan`.
2. **Fail-fast validation**: With `@Validated` and `@NotBlank`, the application **refuses to start** if the property is missing or blank, preventing silent misconfigurations at runtime.
3. **Dependency Injection**: Can be injected directly into `KafkaTopicConfig` and `KafkaProducer`, acting as a single source of truth for the topic name.
4. **Tests pass right away**: Run `mvn test -Dtest=KafkaTopicPropertiesTest` (4 tests green).

---

## Step 1: `KafkaTopicConfig.java` (Topic Provisioning)

### Background & Concepts
- Spring Boot provides an `AdminClient` behind the scenes. Any `NewTopic` bean registered in the application context will be automatically declared on the broker on startup.
- **Partitions**: Set to 3 to allow parallel consumption across up to 3 consumer threads or instances.
- **Replicas**: Set to 3 (across the 3-broker cluster).
- **`min.insync.replicas`**:
  - Main topic: `3` (with producer `acks=all`, guarantees all 3 brokers persist every quote).
  - DLT: `2` (maintains high availability even if 1 broker is temporarily offline so poisoned records can still be parked).
- **`retention.ms`**:
  - Main topic: `7 days` (sufficient buffer for replay).
  - DLT: `30 days` (operators have a month to investigate and replay failed messages).

### Implementation
Open `../src/main/java/nh/kafkahandson/kafka/KafkaTopicConfig.java` and implement the two beans:

```java
@Bean
NewTopic quoteTopic(KafkaTopicProperties props) {
    return TopicBuilder.name(props.quoteTopic())
            .partitions(3)
            .replicas(3)
            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3")
            .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(Duration.ofDays(7).toMillis()))
            .build();
}

@Bean
NewTopic quoteDeadLetterTopic(KafkaTopicProperties props) {
    return TopicBuilder.name(props.quoteTopic() + KafkaConsumerConfig.DEAD_LETTER_SUFFIX)
            .partitions(3)
            .replicas(3)
            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
            .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(Duration.ofDays(30).toMillis()))
            .build();
}
```

### Verification
Run the topic config unit tests:
```bash
./mvnw.cmd test -Dtest=KafkaTopicConfigTest
```
Expected: **9 tests pass**.

---

## Step 2: `KafkaProducer.java` (Publishing Quotes)

### Background & Concepts
- **Partition Key**: We use `quote.symbol()` (e.g., `"AAPL"`, `"MSFT"`). In Kafka, messages with the same key always hash to the same partition, preserving strict chronological ordering per stock ticker.
- **Serialization**: We serialize domain `Quote` objects to JSON strings using `ObjectMapper`.
- **Asynchronous Send**: `kafkaTemplate.send()` returns a `CompletableFuture<SendResult<String, String>>`. It doesn't block the caller thread; rather, we register callbacks via `.whenComplete((result, ex) -> ...)` to log success (with topic, partition, offset) or failure.

### Implementation
Open `../src/main/java/nh/kafkahandson/kafka/KafkaProducer.java` and implement `send`:

```java
public CompletableFuture<SendResult<String, String>> send(Quote quote) {
    String value = objectMapper.writeValueAsString(quote);
    return kafkaTemplate.send(topic, quote.symbol(), value)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish quote for {}", quote.symbol(), ex);
                } else {
                    var md = result.getRecordMetadata();
                    log.info("Produced quote with key:{} to topic:{} on partition:{} with offset:{}",
                            quote.symbol(), md.topic(), md.partition(), md.offset());
                }
            });
}
```

### Verification
Run the producer unit tests:
```bash
./mvnw.cmd test -Dtest=KafkaProducerTest
```
Expected: **10 tests pass**.

---

## Step 3: `KafkaConsumer.java` (Consuming Quotes)

### Background & Concepts
- **`@KafkaListener`**:
  - `topics = "${app.kafka.quote-topic}"`: Resolves topic dynamically from properties.
  - `id = "quote-db-writer"`, `idIsGroup = false`: Lets `spring.kafka.consumer.group-id` in `application.yaml` control the consumer group name.
- **Deserialization**: We parse the JSON record value back into a `Quote` object using `ObjectMapper.readValue()`.
- **Storage**: Calls `QuoteStorageService.store(quote)` which performs an idempotent insert into PostgreSQL. If parsing or DB storage throws, the exception bubbles up to the Kafka listener container error handler.

### Implementation
Open `../src/main/java/nh/kafkahandson/kafka/KafkaConsumer.java` and implement `onMessage`:

```java
@KafkaListener(id = "quote-db-writer", idIsGroup = false, topics = "${app.kafka.quote-topic}")
void onMessage(ConsumerRecord<String, String> record) {
    log.info("Consumed quote with key:{} from topic:{} partition:{} offset:{} and sent to postgres successfully",
            record.key(), record.topic(), record.partition(), record.offset());
    Quote quote = objectMapper.readValue(record.value(), Quote.class);
    storageService.store(quote);
}
```

### Verification
Run the consumer unit tests:
```bash
./mvnw.cmd test -Dtest=KafkaConsumerTest
```
Expected: **9 tests pass**.

---

## Step 4: `KafkaConsumerConfig.java` (Error Handling & DLT)

### Background & Concepts
- Without an error handler, a poison pill (malformed JSON) will cause the consumer to fail, re-poll the same offset infinitely, and halt processing for that partition.
- **`DeadLetterPublishingRecoverer`**: Forwards failed records to `<topic>.DLT`. Passing `-1` for the partition allows Kafka's default partitioner to balance records across the DLT partitions.
- **`DefaultErrorHandler`**:
  - **Exponential Backoff**: `1000ms`, multiplier `2.0`, max `3` attempts (1s, 2s, 4s). Used for transient issues (e.g., temporary database hiccups).
  - **Non-retryable exceptions**: `JacksonException` and `IllegalArgumentException`. A corrupted JSON string will never succeed on retry; sending it straight to the DLT prevents wasting retry time and burning compute.
  - **Retry listener**: Logs warnings on each failed attempt for observability.

### Implementation
Open `../src/main/java/nh/kafkahandson/kafka/KafkaConsumerConfig.java` and implement `quoteErrorHandler`:

```java
@Bean
DefaultErrorHandler quoteErrorHandler(KafkaTemplate<String, String> kafkaTemplate) {
    var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
            (record, ex) -> new TopicPartition(record.topic() + DEAD_LETTER_SUFFIX, -1));

    var backOff = new ExponentialBackOff(1_000L, 2.0);
    backOff.setMaxAttempts(MAX_RETRIES);

    var handler = new DefaultErrorHandler(recoverer, backOff);
    handler.addNotRetryableExceptions(JacksonException.class, IllegalArgumentException.class);
    handler.setRetryListeners((record, ex, attempt) ->
            log.warn("Delivery attempt {} failed for {}-{}@{}", attempt,
                    record.topic(), record.partition(), record.offset(), ex));
    return handler;
}
```

### Verification
Run the consumer config unit tests:
```bash
./mvnw.cmd test -Dtest=KafkaConsumerConfigTest
```
Expected: **6 tests pass**.

---

## Step 5: `application.yaml` (Kafka Configuration)

### Background & Concepts
- **`bootstrap-servers`**: Connection points for the Kafka cluster.
- **Producer Configuration**:
  - `acks: all`: Strongest durability — producer waits until all in-sync replicas confirm receipt.
  - `compression-type: gzip`: Compresses batches over the network.
  - `enable.idempotence: true`: Prevents duplicate records from network retry issues.
  - `linger.ms: 20`: Allows small batching delay to improve throughput.
- **Consumer Configuration**:
  - `group-id: quote-db-writer`: Multiple consumers in this group share the partitions.
  - `auto-offset-reset: earliest`: Brand new consumer groups read from the beginning of retained data.
  - `enable-auto-commit: false`: Manual offset management via Spring's listener container.
  - `ack-mode: record`: Offset is committed immediately after each successfully processed message.
  - `concurrency: 3`: Spins up 3 concurrent consumer threads (1 per partition for maximum parallel throughput).

### Implementation
Uncomment the `spring.kafka` section in `../src/main/resources/application.yaml`:

```yaml
  kafka:
    bootstrap-servers: localhost:9092,localhost:9094,localhost:9096
    producer:
      acks: all                      # wait for all in-sync replicas (you have min.insync=2)
      compression-type: gzip
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      properties:
        enable.idempotence: true     # default in modern clients, explicit is nice
        delivery.timeout.ms: 120000
        linger.ms: 20
        request.timeout.ms: 30000
        max.in.flight.requests.per.connection: 5
    consumer:
      group-id: quote-db-writer      # consumers sharing a group id split the partitions between them
      auto-offset-reset: earliest    # a brand new group starts at the beginning of the topic
      enable-auto-commit: false      # offsets are committed by the listener container after processing
      max-poll-records: 100          # upper bound of records returned by one poll()
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      properties:
        max.poll.interval.ms: 300000 # max time between polls before the consumer is kicked out of the group
        session.timeout.ms: 45000    # no heartbeat for this long = consumer considered dead
        heartbeat.interval.ms: 15000 # keep at most 1/3 of session.timeout.ms
    listener:
      ack-mode: record               # commit the offset after each successfully processed record
      concurrency: 3                 # one consumer thread per partition
```

---

## Step 6: Final Verification (All Tests Green)

Run the full test suite:
```bash
./mvnw.cmd test
```
All unit tests and the Spring Boot application context integration test will be **100% green**.
