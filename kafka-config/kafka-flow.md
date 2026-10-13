# Kafka Producer and Consumer Flow

This document follows one quote from the application to Kafka and then from Kafka
to PostgreSQL. It is written to support a live presentation alongside the code.

## At a glance

```text
Alpaca API
    |
    v
Quote
    |
    v
KafkaProducer -> quote-data topic -> Kafka brokers
                                      |
                                      v
                         KafkaConsumer -> PostgreSQL
                                      |
                         failure: retry -> quote-data.DLT
```

## Producer flow

The application fetches a `List<Quote>` from Alpaca. `AlpacaToKafkaService`
starts one asynchronous Kafka send per quote:

```java
var futures = quotes.stream()
        .map(kafkaProducer::send)
        .toList();
```

`kafkaProducer::send` is shorthand for:

```java
.map(quote -> kafkaProducer.send(quote))
```

Each call to `send` follows this sequence:

| Step | Object or component | What happens |
| --- | --- | --- |
| 1 | `Quote` | The application passes a domain object, such as a quote for `AAPL`, to `KafkaProducer.send(quote)`. |
| 2 | `quote.symbol()` | The quote symbol becomes the Kafka record key. Records with the same key are sent to the same partition. |
| 3 | `ObjectMapper` | `objectMapper.writeValueAsString(quote)` converts the `Quote` object to a JSON `String`. |
| 4 | `KafkaTemplate<String, String>` | `kafkaTemplate.send(topic, key, value)` creates and submits a `ProducerRecord<String, String>` for the `quote-data` topic. |
| 5 | `StringSerializer` | The Kafka client serializes the key and JSON value from strings into bytes. |
| 6 | Producer buffer | The Kafka client buffers records; its background sender batches them. `linger.ms=20` permits a brief wait to form more efficient batches. |
| 7 | Kafka broker | The producer sends the batch to the leader for the selected partition. |
| 8 | Replicas and acknowledgement | `acks=all` waits for all in-sync replicas. The topic requires at least two in-sync replicas, so Kafka rejects the write if that durability requirement cannot be met. |
| 9 | `RecordMetadata` | On success, Kafka returns the assigned topic, partition, and offset. |
| 10 | `SendResult<String, String>` | Spring wraps the original record and its metadata in a `SendResult`. |
| 11 | `CompletableFuture<SendResult<String, String>>` | The future completes successfully with the result, or exceptionally with the failure. The `whenComplete` callback logs either outcome. |

```text
Quote
  -> JSON String
  -> ProducerRecord<String, String>
  -> serialized bytes
  -> producer buffer and batch
  -> partition leader and replicas
  -> RecordMetadata
  -> SendResult<String, String>
  -> CompletableFuture completion
```

`send` returns immediately with a future; it does not block for the broker
acknowledgement. The service gathers all futures, then waits for their
completion:

```java
CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
        .exceptionally(ex -> null)
        .join();
```

The individual `whenComplete` callbacks log failed records. The
`exceptionally` stage allows the batch to finish even if one or more individual
sends failed, after which the service counts the successfully completed futures.

### Producer reliability settings

| Setting | Effect |
| --- | --- |
| `acks=all` | The broker acknowledges a record only after all in-sync replicas acknowledge it. |
| `min.insync.replicas=3` on `quote-data` | The topic needs three in-sync replicas to accept an `acks=all` write. |
| `enable.idempotence=true` | The producer avoids duplicate records caused by retries from the same producer session. |
| `max.in.flight.requests.per.connection=5` | Allows throughput while retaining ordering guarantees with idempotence enabled. |
| `delivery.timeout.ms=120000` | Fails the send if Kafka cannot deliver it within two minutes. |

## Consumer flow

The `@KafkaListener` method consumes from `quote-data`:

```java
@KafkaListener(id = "quote-db-writer", idIsGroup = false,
        topics = "${app.kafka.quote-topic}")
void onMessage(ConsumerRecord<String, String> record) {
    Quote quote = objectMapper.readValue(record.value(), Quote.class);
    storageService.store(quote);
}
```

Each received record follows this sequence:

| Step | Object or component | What happens |
| --- | --- | --- |
| 1 | Consumer group `quote-db-writer` | Kafka tracks offsets for this group. Consumers in the same group divide the topic partitions between themselves. |
| 2 | Kafka listener container | Spring starts up to three consumer threads (`concurrency=3`), matching the topic's three partitions. |
| 3 | `poll()` | The underlying Kafka consumer polls brokers for records. A poll can return up to 100 records (`max-poll-records=100`). |
| 4 | `ConsumerRecord<String, String>` | Spring delivers one record to `onMessage`. It contains the key, JSON string value, topic, partition, and offset. |
| 5 | `StringDeserializer` | The Kafka client converts the key and value bytes received from Kafka back into strings. |
| 6 | `ObjectMapper` | `objectMapper.readValue(record.value(), Quote.class)` turns the JSON value into a `Quote` object. |
| 7 | `QuoteStorageService` | `storageService.store(quote)` writes the quote to PostgreSQL in a transaction. The repository ignores an already-stored quote, which helps make repeat delivery safe. |
| 8 | Listener acknowledgement | If the listener returns normally, `ack-mode=record` commits that record's offset. Kafka then knows this consumer group has processed it. |

```text
Kafka broker
  -> poll()
  -> ConsumerRecord<String, String>
  -> StringDeserializer
  -> JSON String
  -> Quote
  -> PostgreSQL transaction
  -> offset commit for quote-db-writer
```

For a new consumer group with no saved offset, `auto-offset-reset=earliest`
starts processing from the earliest retained record. Once offsets have been
committed, restarts resume after the last committed offset.

### Consumer failure flow

If JSON parsing or database storage throws an exception, the listener does not
successfully return, so its offset is not committed. `KafkaConsumerConfig`
routes the failure through `DefaultErrorHandler`:

```text
listener exception
  -> no successful offset commit
  -> retry with exponential backoff (1 s, 2 s, 4 s)
  -> success: commit and continue
  -> still failing: publish original record to quote-data.DLT
  -> recover and continue with the next record
```

| Failure type | Handling |
| --- | --- |
| `JacksonException` or `IllegalArgumentException` | Not retried because malformed input will not become valid; it is sent directly to `quote-data.DLT`. |
| Other exceptions, such as a database failure | Retried up to three times with exponential backoff. |
| Still failing after retries | `DeadLetterPublishingRecoverer` publishes the record to `quote-data.DLT`, allowing the original partition to continue. |

The dead-letter topic has the same three partitions as the main topic and a
30-day retention period, giving operators time to inspect and replay failed
records.

## Key presentation points

1. A Kafka producer sends asynchronously: a `CompletableFuture` represents the
   broker acknowledgement that has not happened yet.
2. A Kafka record has a key, value, topic, partition, and offset. The key
   determines partition placement; the offset is that record's position inside
   the partition.
3. Kafka retains records independently of consumers. Each consumer group tracks
   its own offsets and can read the same topic independently.
4. Processing is at-least-once: if storage succeeds but the application stops
   before the offset commit, Kafka can deliver the record again. The database's
   duplicate-safe insert makes that repeat safe.
5. A dead-letter topic prevents one bad record from blocking all later records
   in its partition.
