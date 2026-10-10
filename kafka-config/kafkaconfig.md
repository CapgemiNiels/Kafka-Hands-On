# Kafka configuration explained

This guide walks through every Kafka setting used in this project: the cluster, the topic, the producer and the consumer.
All client settings live in `src/main/resources/application.yaml` under `spring.kafka`; the topic is declared in `KafkaTopicConfig`.

## 1. The big picture

```
Alpaca REST --> QuoteScheduler --> AlpacaToKafkaService --> KafkaProducer
                                                               |
                                                     topic: quote-data (3 partitions)
                                                               |
                                        KafkaConsumer --> QuoteStorageService --> PostgreSQL
                                              |
                                    (after 3 failed retries)
                                              v
                                    topic: quote-data.DLT
```

Open the Kafka UI at <http://localhost:8080> to see the brokers, topics, partitions, messages and consumer groups described below.

## 2. Cluster concepts

| Concept | What it is | In this project |
|---|---|---|
| **Broker** | A Kafka server that stores partitions and serves producers and consumers. | 3 brokers: `kafka1`, `kafka2`, `kafka3` |
| **Topic** | A named stream of messages, like a table or a queue that is never emptied by reading. | `quote-data` |
| **Partition** | A topic is split into partitions. Each is an ordered, append-only log. Partitions are how Kafka scales and parallelises. | 3 |
| **Replica** | A copy of a partition on another broker. | 3 (one per broker) |
| **Leader / follower** | One replica per partition is the leader. Producers and consumers talk to it; followers copy it. | chosen by Kafka |
| **ISR (in-sync replicas)** | The replicas that are fully caught up with the leader. | up to 3 |
| **Offset** | The position of a message within one partition. Offsets are per partition, not per topic. | shown in the log lines |

### Number of brokers
More brokers give more capacity and more fault tolerance. With 3 brokers and replication factor 3, every broker holds a copy of every partition, so no data is lost when a broker fails (whether writes continue depends on `min.insync.replicas`, see below).
The brokers also act as KRaft controllers (`KAFKA_PROCESS_ROLES: broker,controller`), which means no separate ZooKeeper is needed. A controller quorum needs a majority, so 3 controllers tolerate 1 failure.

### Number of partitions
```java
TopicBuilder.name("quote-data").partitions(3).replicas(3)
```
* Ordering is guaranteed **only within a partition**.
* The producer uses the stock symbol as the message **key**. Kafka hashes the key to choose the partition, so all `AAPL` quotes always land on the same partition and stay in order, while different symbols spread over the 3 partitions.
* The partition count is also the **maximum parallelism of one consumer group**: 3 partitions means at most 3 active consumers in a group. More consumers than partitions sit idle.
* Partitions can be added later but never removed, and adding them changes which partition a key maps to. Choose with some headroom.

### Replication factor
`replicas(3)` stores each partition on 3 brokers. It cannot be higher than the number of brokers.

### min.insync.replicas
```java
.config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3")
```
The minimum number of replicas (the leader included) that must have a message before a write with `acks=all` is accepted. This project uses `3` with replication factor 3, so **every** broker must be in sync:

| Brokers up | In-sync replicas | Writes with `acks=all` |
|---|---|---|
| 3 | 3 | accepted |
| 2 | 2 | **rejected** (`NotEnoughReplicasException`) |
| 1 | 1 | **rejected** |

This is deliberately the strictest setting, chosen to make the behaviour easy to demonstrate: stop one broker and the producer stops publishing quotes. Nothing is lost, the writes are refused rather than stored on fewer copies. Reads of existing data keep working.
It is the trade-off between consistency and availability:

| `min.insync.replicas` (RF=3) | Tolerates | Trade-off |
|---|---|---|
| 1 | 2 brokers down | Writes keep flowing, but an acknowledged message can exist on a single broker. |
| 2 | 1 broker down | The common production choice: still available with one broker down, every acknowledged message is on at least 2 brokers. |
| 3 (this project) | 0 brokers down | Maximum durability, the topic stops accepting writes as soon as one broker is down. |

The topic-level setting is what counts. `KAFKA_MIN_INSYNC_REPLICAS: 3` in `docker-compose.yaml` is only the default for topics that do not define their own, and is also set to 3 on every broker so the two agree.
Changing it in code does not alter a topic that already exists. To apply a new value to the local cluster, recreate it with `docker compose down -v` and start again (or change it in the Kafka UI).
### Retention
```java
.config(TopicConfig.RETENTION_MS_CONFIG, "604800000") // 7 days
```
Kafka does not delete a message when it is read. It keeps messages for the retention period (here 7 days), so consumers can re-read, and a new consumer group can start from the beginning.

## 3. Producer settings

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092,localhost:9094,localhost:9096
    producer:
      acks: all
      compression-type: gzip
      key-serializer: ...StringSerializer
      value-serializer: ...StringSerializer
      properties:
        enable.idempotence: true
        delivery.timeout.ms: 120000
        linger.ms: 20
        request.timeout.ms: 30000
        max.in.flight.requests.per.connection: 5
```

| Setting | Meaning | Why this value |
|---|---|---|
| `bootstrap-servers` | Initial brokers used to discover the rest of the cluster. | List several so the client can still connect when one is down. |
| **`acks`** | How many brokers must confirm a write before it counts as successful. | see below |
| `key-serializer` / `value-serializer` | Turn Java objects into bytes. | Key is the symbol, value is the quote as a JSON string. |
| `compression-type` | Compresses batches on the wire and on disk. | `gzip` saves bandwidth for JSON text; `lz4` or `zstd` are faster alternatives. |
| `enable.idempotence` | Retries cannot create duplicates or reorder messages within a partition. | On. It requires `acks=all`. |
| `linger.ms` | How long the producer waits to fill a batch before sending. | 20 ms trades a tiny delay for fewer, bigger requests. `0` sends immediately. |
| `request.timeout.ms` | How long to wait for one broker response. | 30 s. |
| `delivery.timeout.ms` | Total time allowed for a send including retries. After this the future fails. | 2 min. Must be at least `linger.ms + request.timeout.ms`. |
| `max.in.flight.requests.per.connection` | Unacknowledged requests per broker connection. | 5 is the highest value that still keeps ordering with idempotence. |

### acks

| Value | Behaviour | Durability | Latency |
|---|---|---|---|
| `0` | Fire and forget. Does not wait for any response. | Messages can be lost silently. | lowest |
| `1` | The partition leader confirms after writing locally. | Lost if the leader dies before followers copy it. | low |
| `all` (or `-1`) | The leader waits until all **in-sync** replicas have the message, and at least `min.insync.replicas`. | Survives broker failures. | highest |

`acks=all` and `min.insync.replicas=3` work together: `acks=all` says "wait for the in-sync replicas", `min.insync.replicas` says "and there must be at least 3 of them, otherwise refuse the write".
`acks=all` on its own, with the ISR shrunk to just the leader, is no safer than `acks=1`.

### Sending is asynchronous
`KafkaProducer.send` returns a `CompletableFuture`. The result (partition and offset, or an error) arrives later. `AlpacaToKafkaService` sends every quote first and then waits for all the futures, so a batch is sent in parallel instead of one record at a time.

## 4. Consumer settings

```yaml
spring:
  kafka:
    consumer:
      group-id: quote-db-writer
      auto-offset-reset: earliest
      enable-auto-commit: false
      max-poll-records: 100
      key-deserializer: ...StringDeserializer
      value-deserializer: ...StringDeserializer
      properties:
        max.poll.interval.ms: 300000
        session.timeout.ms: 45000
        heartbeat.interval.ms: 15000
    listener:
      ack-mode: record
      concurrency: 3
```

| Setting | Meaning | Why this value |
|---|---|---|
| **`group-id`** | Consumers with the same group id form a group and share the partitions of a topic. Each partition goes to exactly one consumer in the group. | `quote-db-writer`. A second application with a different group id would receive every message again, independently. |
| **`auto-offset-reset`** | What to do when the group has no committed offset yet (or it has expired). `earliest` starts at the oldest message, `latest` at the newest. | `earliest`, so no data is missed on the first start. |
| **`enable-auto-commit`** | Whether the client commits offsets in the background on a timer. | `false`. Background commits can mark a message as done before it is stored (loss) or after (duplicates). The listener container commits explicitly instead. |
| `ack-mode` | When the container commits the offset. `record` is after each message, `batch` (default) after each poll. | `record`. A crash replays at most the one message in flight. |
| `max-poll-records` | Maximum records returned by one `poll()`. | 100. Keeps the time spent per poll predictable. |
| `max.poll.interval.ms` | Maximum time between two polls. Longer and the consumer is removed from the group, which triggers a rebalance. | 5 min. Processing 100 records must stay well below this. |
| `session.timeout.ms` | A consumer without heartbeats for this long is considered dead. | 45 s. |
| `heartbeat.interval.ms` | How often heartbeats are sent. | 15 s, at most a third of the session timeout. |
| `concurrency` | Number of consumer threads in this application. | 3, one per partition. More would be idle. |
| deserializers | Turn bytes back into objects. | Strings, then Jackson parses the JSON. |

### Offsets and delivery guarantees
An **offset** is the consumer group's bookmark per partition. The committed offset is stored by Kafka in the internal topic `__consumer_offsets`.

* We store to the database **first** and commit the offset **after**. If the app crashes in between, the message is delivered again. This is **at-least-once** delivery.
* Duplicates must therefore be harmless. The table has a unique key on `(symbol, quoted_at)` and the insert uses `ON CONFLICT DO NOTHING`, which makes the consumer **idempotent**.
* Committing before processing would give *at-most-once*: no duplicates, but messages can be lost.
* True exactly-once is only available inside Kafka (transactions). When the destination is a database, at-least-once plus an idempotent write is the usual answer.

### Consumer groups and rebalancing
Start a second instance of the application and Kafka redistributes the 3 partitions over the consumers in the group. This is a **rebalance**. It also happens when a consumer crashes (missed heartbeats) or stops polling for longer than `max.poll.interval.ms`.
In the Kafka UI, open *Consumers* to see the group, which member owns which partition and the **lag** (how far behind the latest offset the group is).

### Error handling and the dead letter topic
Defined in `KafkaConsumerConfig`:

1. A message that fails is retried with exponential backoff (1 s, 2 s, 4 s), 3 retries.
2. A message that can never succeed (invalid JSON, bad data) is **not** retried.
3. After the retries are exhausted the message is published to `quote-data.DLT` (the dead letter topic) and the consumer carries on with the next message.

Without this, one bad message (a *poison pill*) would block its partition forever, since the consumer would keep failing on the same offset.
Inspect `quote-data.DLT` in the Kafka UI. The `kafka_dlt-exception-*` headers explain why each message was parked.

## 5. Try it yourself

1. `docker compose up -d`, then start the application with your Alpaca credentials (see the README).
2. In the Kafka UI, open the `quote-data` topic: see 3 partitions, 3 replicas and the messages with their keys.
3. Open *Consumers* and look at the lag of `quote-db-writer`.
4. Stop one broker (`docker stop kafka3`). The cluster stays up and leaders move to the remaining brokers, but the in-sync replicas drop to 2, below `min.insync.replicas=3`. Sends now fail with `NotEnoughReplicasException` and the log shows `Failed to publish quote`. The consumer keeps reading what is already in the topic.
5. Start it again (`docker start kafka3`), watch the ISR recover in the Kafka UI, and see publishing resume at the next poll.
7. Publish a non-JSON message to `quote-data` from the Kafka UI and watch it end up in `quote-data.DLT`.

## 6. Property reference

Everything below goes under `spring.kafka` in `application.yaml`. Spring Boot has dedicated keys for the common settings (for example `producer.acks`). Any other Kafka client property goes under `producer.properties`, `consumer.properties` or `properties` (all clients) using its original dotted name.
? = used in this project, ? = not used here but common in production.

### 6.1 Producer

| | Property | Default | What it does |
|---|---|---|---|
| ? | `acks` | `all` | Brokers that must confirm a write. See section 3. |
| ? | `compression-type` | `none` | Compression of batches: `gzip`, `snappy`, `lz4`, `zstd`. |
| ? | `enable.idempotence` | `true` | Retries never create duplicates or reorder messages within a partition. |
| ? | `linger.ms` | `5` | Wait time to fill a batch before sending. |
| ? | `delivery.timeout.ms` | `120000` | Total time for a send including retries. |
| ? | `request.timeout.ms` | `30000` | Time to wait for one broker response. |
| ? | `max.in.flight.requests.per.connection` | `5` | Unacknowledged requests per connection. |
| ? | `retries` | `2147483647` | Number of retries on a transient error. In practice you leave this alone and bound the total time with `delivery.timeout.ms`. |
| ? | `retry.backoff.ms` | `100` | Pause between retries. |
| ? | `batch-size` (`batch.size`) | `16384` | Maximum bytes per batch per partition. A larger batch together with `linger.ms` raises throughput. |
| ? | `buffer-memory` (`buffer.memory`) | `33554432` | Memory for records waiting to be sent. When full, `send()` blocks. |
| ? | `max.block.ms` | `60000` | How long `send()` may block when the buffer is full or metadata is missing, before throwing. |
| ? | `max.request.size` | `1048576` | Largest request (roughly the largest message). Must not exceed the broker's `message.max.bytes`. |
| ? | `client-id` | generated | A name for this producer in broker logs and metrics. Set it to make monitoring readable. |
| ? | `transaction-id-prefix` | none | Turns on transactions, so writes to several partitions or topics commit atomically. Used for exactly-once pipelines inside Kafka. |
| ? | `partitioner.class` | built in | Custom logic for choosing the partition. The default hashes the key. |
| ? | `interceptor.classes` | none | Hook to inspect or modify records before they are sent (tracing, auditing). |
| ? | `template.default-topic` | none | Topic used when `KafkaTemplate.send` is called without one. |

### 6.2 Consumer

| | Property | Default | What it does |
|---|---|---|---|
| ? | `group-id` | none | The consumer group. See section 4. |
| ? | `auto-offset-reset` | `latest` | Where to start without a committed offset: `earliest`, `latest` or `none` (fail). |
| ? | `enable-auto-commit` | `true` (Boot sets `false`) | Background offset commits. Off, so the container commits. |
| ? | `max-poll-records` | `500` | Maximum records per `poll()`. |
| ? | `max.poll.interval.ms` | `300000` | Maximum time between polls before the consumer leaves the group. |
| ? | `session.timeout.ms` | `45000` | No heartbeat for this long and the consumer is considered dead. |
| ? | `heartbeat.interval.ms` | `3000` | Heartbeat frequency, at most a third of the session timeout. |
| ? | `listener.ack-mode` | `batch` | When offsets are committed: `record`, `batch`, `manual`, `manual_immediate`, `time`, `count`. |
| ? | `listener.concurrency` | `1` | Consumer threads per listener, at most the number of partitions. |
| ? | `fetch-min-size` (`fetch.min.bytes`) | `1` | The broker waits until this much data is available before answering a fetch. Higher means fewer, larger fetches, at the cost of latency. |
| ? | `fetch-max-wait` (`fetch.max.wait.ms`) | `500` | How long the broker waits for `fetch.min.bytes` before answering anyway. |
| ? | `fetch.max.bytes` | `52428800` | Upper limit of data returned for one fetch across all partitions. |
| ? | `max.partition.fetch.bytes` | `1048576` | Upper limit per partition per fetch. Must fit the largest message. |
| ? | `isolation-level` (`isolation.level`) | `read_uncommitted` | `read_committed` hides messages from aborted or open transactions. Needed when producers use transactions. |
| ? | `partition.assignment.strategy` | range, cooperative sticky | `CooperativeStickyAssignor` rebalances incrementally, so consumers keep reading during a rebalance. |
| ? | `group.instance.id` | none | Static membership. A restarting instance keeps its partitions without triggering a rebalance if it returns within `session.timeout.ms`. Good for rolling deployments. |
| ? | `client-id` | generated | A name for this consumer in logs and metrics. |
| ? | `auto.commit.interval.ms` | `5000` | Commit interval, only relevant when `enable-auto-commit` is `true`. |
| ? | `allow.auto.create.topics` | `true` | Whether subscribing to a missing topic creates it. Switch off in production, topics should be created deliberately. |
| ? | `listener.type` | `single` | `batch` hands the whole poll result to the listener as a list, for bulk database inserts. |
| ? | `listener.poll-timeout` | `5s` | How long the container blocks in each `poll()`. Affects how quickly it notices a shutdown. |
| ? | `listener.missing-topics-fatal` | `false` | Fail the start when a listened topic does not exist. |
| ? | `listener.idle-between-polls` | `0` | Pause between polls, to throttle consumption. |
| ? | `listener.auto-startup` | `true` | Set `false` to start and stop listeners yourself, for example via `KafkaListenerEndpointRegistry`. |

### 6.3 Deserialization safety (consumer)
A plain `StringDeserializer` never fails, but `JsonDeserializer` or Avro deserializers throw on bad bytes, and such a failure happens **before** your listener runs. In a poll loop that means the same record fails forever. Wrap the real deserializer:
```yaml
spring.kafka.consumer.value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
spring.kafka.consumer.properties.spring.deserializer.value.delegate.class: <your deserializer>
```
The failure then arrives in the error handler, which can send the record to the dead letter topic as this project does.

### 6.4 Connection and security (all clients)
Production clusters are almost never `PLAINTEXT`. Typical settings under `spring.kafka.properties` (or `spring.kafka.security` / `spring.kafka.ssl`):

| Property | What it does |
|---|---|
| `security.protocol` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` or `SASL_SSL`. |
| `sasl.mechanism` | Login method: `PLAIN`, `SCRAM-SHA-512`, `OAUTHBEARER`. |
| `sasl.jaas.config` | Username and password (or token) for SASL. Load from a secret, never commit it. |
| `ssl.truststore.location` / `.password` | Certificates used to trust the brokers (`spring.kafka.ssl.trust-store-*`). |
| `ssl.keystore.location` / `.password` | Client certificate for mutual TLS (`spring.kafka.ssl.key-store-*`). |
| `client.dns.lookup` | `use_all_dns_ips` tries every address behind a DNS name. Useful for load balanced bootstrap servers. |
| `reconnect.backoff.ms` / `reconnect.backoff.max.ms` | Pause between reconnect attempts to a broker (default 50 ms, grows to 1 s). |
| `metadata.max.age.ms` | How often cluster metadata is refreshed even when nothing changed (default 5 min). |

### 6.5 Rules of thumb
* Durability: `acks=all`, `enable.idempotence=true`, replication factor 3 and `min.insync.replicas` 2 (this demo uses 3 to show the producer stopping).
* Throughput: raise `batch.size` and `linger.ms`, add compression, add partitions.
* Latency: `linger.ms=0`, `fetch.min.bytes=1`.
* Stable consumers: `max.poll.records` low enough that one batch is processed well within `max.poll.interval.ms`; `group.instance.id` and `CooperativeStickyAssignor` to avoid disruptive rebalances.
* Never rely on auto-commit when losing or repeating a message matters.