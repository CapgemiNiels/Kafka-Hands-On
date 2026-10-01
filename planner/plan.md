# Kafka live-coding demo plan

## 1. Demo goal

Build a **simple but realistic Kafka flow** for Java developers with little Kafka knowledge:

1. Poll a public HTTP API every minute for changing stock-price data.
2. Produce that data into Kafka with a **stable message key** per stock symbol.
3. Show the topic, partitions, leaders, replicas, and messages in the cluster.
4. Build a consumer that reads the events.
5. Persist every consumed event into Postgres.

This keeps the story linear:

**API -> Producer -> Kafka topic/partitions -> Consumer -> Postgres**

The live-coding portion should focus on the **producer and consumer code**, while infrastructure and non-essential scaffolding are already prepared.

---

## 2. Recommended demo scenario

Use **stock symbols as message keys** so partitioning is easy to explain:

- key = `symbol` (for example `AAPL`, `MSFT`, `NVDA`)
- value = stock price event payload

Why this is a good fit:

- easy business story
- values change over time
- stable key demonstrates partition affinity
- you can explain that all events for the same symbol tend to go to the same partition

### Recommended public API

Use a public HTTP API that returns changing market data without local auth in the demo flow.

Best practical setup for the presentation:

1. **Prebuild an HTTP client abstraction** in the app.
2. Put the base URL and any optional API token in configuration properties.
3. Before the session, verify the chosen API is stable and returns live-ish values.
4. Have a fallback plan: a small mock endpoint or static JSON file served locally if the public API fails.

Because public finance APIs can be rate-limited or unreliable, the safest presentation setup is:

- **Primary path:** public API
- **Backup path:** local mock API in Docker Compose or a stubbed JSON endpoint in the Spring app

Even if you prefer the public API, prepare the fallback in advance.

---

## 3. Architecture for the demo

Use **one Spring Boot app** with separate components:

- `StockPriceClient` for fetching API data
- `StockPricePoller` for scheduled polling
- `KafkaProducerConfig` + producer service
- `KafkaConsumerConfig` + consumer listener
- `StockPriceEventRepository` / persistence layer for Postgres

### Runtime flow

1. Scheduler triggers every minute.
2. HTTP client fetches current stock prices for a few symbols.
3. Producer publishes one Kafka record per symbol.
4. Consumer receives each record.
5. Consumer writes each event to Postgres.

---

## 4. What must already be running before the presentation

Start everything with Docker Compose before the session:

1. **Kafka cluster with 3 brokers**
2. **Kafbat UI**
3. **Postgres**
4. Optional but recommended: **mock API** as fallback

### Why 3 brokers

A 3-node cluster makes it easy to explain:

- broker 1 / broker 2 / broker 3
- partition leaders
- replicas on followers
- failover conceptually, even if not demoed live

### Recommended topic setup

Create the demo topic in advance:

- topic name: `stock-prices`
- partitions: `3`
- replication factor: `3`
- min in-sync replicas: `2`

This gives a clean story:

- one partition leader can live on each broker
- every broker can host replicas
- Kafbat UI can show topic metadata and partition layout

---

## 5. Can Kafbat UI show leader/follower and partition placement?

**Yes, partially.**

Kafbat UI is good for showing:

- topics
- partition count
- partition leaders
- replica assignments
- in-sync replicas
- browsing records
- consumer groups and lag

This is enough to explain:

- which partition is led by which broker
- that replicas are spread across brokers
- that messages with the same key land in the same partition

What it is **less suited** for:

- visually showing where the raw log files are stored on disk
- deep broker internals

For storage explanation, combine Kafbat UI with a quick terminal/Docker volume explanation:

1. show topic partitions in Kafbat UI
2. explain each broker stores its own partition logs locally
3. optionally show the broker container filesystem or mounted volume path

So the answer is:

- **partition leadership and replica placement:** yes, Kafbat UI can help
- **actual on-disk files:** better shown from Docker/container/volume mapping

---

## 6. Infrastructure to prepare in advance

## 6.1 Docker Compose services

Prepare a `docker-compose.yml` with at least:

1. `broker-1`
2. `broker-2`
3. `broker-3`
4. `kafbat-ui`
5. `postgres`
6. optional `mock-stock-api`

### Kafka cluster requirements

Your Compose setup should already define:

- KRaft mode or ZooKeeper-based mode
- internal broker communication
- advertised listeners for local host access
- broker IDs / node IDs
- replication defaults
- persistent volumes

For this demo, **KRaft mode** is the most modern choice and easier to explain because it avoids introducing ZooKeeper.

### Volumes

Use named volumes so broker storage survives container restarts during rehearsal:

- `broker1-data`
- `broker2-data`
- `broker3-data`
- `postgres-data`

### Networking

Use a dedicated Docker network, for example:

- `kafka-demo-net`

---

## 6.2 Kafka topic bootstrap

Create the topic before the presentation starts.

Recommended bootstrap script:

1. wait for the cluster to become healthy
2. create `stock-prices`
3. verify topic metadata

Store this as a script you can rerun quickly if needed.

---

## 6.3 Postgres bootstrap

Prepare:

- database: `kafka_demo`
- user/password for local demo
- init SQL script for schema creation

### Suggested table

```sql
create table stock_price_events (
    id bigserial primary key,
    symbol varchar(20) not null,
    price numeric(18,4) not null,
    currency varchar(10),
    event_time timestamp with time zone not null,
    source varchar(100),
    partition_id integer,
    offset_value bigint,
    consumed_at timestamp with time zone not null default now()
);
```

Useful for the demo because you can show:

- business data (`symbol`, `price`)
- Kafka metadata (`partition_id`, `offset_value`)
- DB persistence result

---

## 6.4 Optional mock API fallback

Strongly recommended if the presentation matters.

Options:

1. small mock service in Docker Compose returning changing stock values
2. WireMock container
3. tiny Spring Boot stub service

The backup endpoint should mimic the same JSON shape as the real API so your app config can switch by property only.

---

## 7. Spring Boot scaffolding to prepare in advance

Because this is plan mode, the goal is not to fully implement everything now, but to define the scaffolding that should exist before the session.

## 7.1 Maven dependencies to add

Add at least:

1. `spring-boot-starter-web` or `spring-boot-starter-restclient` support
2. `spring-kafka`
3. `spring-boot-starter-data-jdbc` or `spring-boot-starter-data-jpa`
4. PostgreSQL JDBC driver
5. optional validation starter
6. test dependencies for Spring Kafka / integration tests if you plan rehearsals

### Recommendation

For a clean live demo, prefer:

- `spring-kafka`
- `spring-boot-starter-jdbc`
- PostgreSQL driver
- `spring-boot-starter-web`

`JdbcTemplate` is simpler to explain than JPA for this use case.

---

## 7.2 Package structure

Suggested structure:

```text
nh.kafkahandson
├─ config
│  ├─ KafkaConsumerProperties
│  ├─ KafkaProducerProperties
│  ├─ StockApiProperties
│  ├─ PostgresProperties (optional)
│  ├─ KafkaProducerConfig
│  └─ KafkaConsumerConfig
├─ api
│  ├─ StockPriceClient
│  ├─ StockPriceResponse
│  └─ StockPriceDto
├─ producer
│  ├─ StockPriceProducer
│  ├─ StockPricePoller
│  └─ StockPriceEvent
├─ consumer
│  ├─ StockPriceConsumer
│  └─ StockPriceEventListener
├─ persistence
│  ├─ StockPriceEventEntity
│  └─ StockPriceEventRepository
└─ KafkaHandsOnApplication
```

---

## 7.3 Configuration properties classes to prebuild

Precreate strongly typed configuration so you can explain Kafka options cleanly.

### Producer properties

Example fields:

- `bootstrapServers`
- `topicName`
- `acks`
- `retries`
- `batchSize`
- `lingerMs`
- `compressionType`
- `clientId`

### Consumer properties

Example fields:

- `bootstrapServers`
- `groupId`
- `autoOffsetReset`
- `enableAutoCommit`
- `maxPollRecords`
- `concurrency`
- `topicName`

### Stock API properties

Example fields:

- `baseUrl`
- `symbols`
- `pollInterval`
- `apiKey` (optional)
- `enabled`

This is excellent for live coding because you can explain the settings from `application.yml` without hardcoding them.

---

## 7.4 Recommended `application.yml` structure

Prefer `application.yml` over `.properties` because nested config is easier to present.

Example structure:

```yaml
spring:
  application:
    name: kafka-hands-on
  datasource:
    url: jdbc:postgresql://localhost:5432/kafka_demo
    username: demo
    password: demo

demo:
  stock-api:
    base-url: https://...
    symbols:
      - AAPL
      - MSFT
      - NVDA
    poll-interval: PT1M
    enabled: true
  kafka:
    producer:
      bootstrap-servers: localhost:9092,localhost:9093,localhost:9094
      topic-name: stock-prices
      client-id: stock-price-producer
      acks: all
      retries: 10
      linger-ms: 5
      batch-size: 16384
      compression-type: snappy
    consumer:
      bootstrap-servers: localhost:9092,localhost:9093,localhost:9094
      topic-name: stock-prices
      group-id: stock-price-consumers
      auto-offset-reset: earliest
      enable-auto-commit: false
      max-poll-records: 50
      concurrency: 1
```

Also prepare:

- `application-local.yml`
- optional `application-demo.yml`

So you can switch environments cleanly.

---

## 8. What to prebuild before the live coding starts

Because you want to live-code the producer and consumer logic, prepare everything around them in advance.

## 8.1 Prebuilt application pieces

Have these already implemented:

1. Maven project setup
2. Docker Compose setup
3. Postgres schema init
4. configuration properties classes
5. Kafka config class skeletons
6. DTOs for stock API response
7. HTTP client scaffolding
8. event model class
9. repository class or `JdbcTemplate` persistence scaffolding
10. scheduling enabled in the app

### Specifically leave incomplete for live coding

Leave these as the coding steps during the presentation:

1. mapping API DTO -> Kafka event
2. producer send logic
3. consumer listener method
4. DB insert from consumer

That gives enough substance without spending time on boilerplate.

---

## 8.2 Suggested “TODO markers” to leave in the code

Pre-place small TODOs in strategic locations:

- `StockPriceProducer.send(...)`
- `StockPricePoller.pollAndPublish()`
- `StockPriceEventListener.onMessage(...)`
- `StockPriceEventRepository.save(...)`

This avoids hunting through files during the presentation.

---

## 9. Live coding sequence

Your usable live-coding window is roughly **30 minutes** after intro and Q&A buffer. Keep the demo focused.

## Step 1: explain the running infrastructure

Show briefly:

1. Docker containers already running
2. Kafka cluster has 3 brokers
3. Kafbat UI is available
4. Postgres is running

Do **not** explain Docker Compose internals here.

---

## Step 2: show the topic in Kafbat UI

Before coding, open the topic view and explain:

- topic name
- number of partitions
- replication factor
- partition leaders
- replicas / ISR

Teaching point:

“Kafka topics are split into partitions, and each partition has one leader broker plus replica copies on other brokers.”

---

## Step 3: show configuration-driven producer setup

Open:

- producer properties class
- `application.yml`
- producer config class

Explain options such as:

- `bootstrap.servers`
- `acks`
- `retries`
- `linger.ms`
- `batch.size`
- `compression.type`

Then implement the producer service.

### Producer live-code target

Implement:

1. create `ProducerRecord<String, StockPriceEvent>`
2. use `symbol` as the key
3. send through `KafkaTemplate`
4. log topic / partition / offset metadata on success

Teaching point:

“The key controls partitioning, so all events for the same symbol stay together.”

---

## Step 4: show the polling flow

Open the scheduler/poller class and show:

1. scheduled method
2. HTTP client usage
3. mapping response to event
4. call to producer

Because polling support is prebuilt, keep this short.

Teaching point:

“Every minute we fetch fresh data and publish a new event.”

---

## Step 5: produce a few records and inspect them

Run the app and let it publish.

In Kafbat UI, show:

1. messages arriving
2. keys by symbol
3. partition assignment

If possible, use multiple symbols so different partitions are visibly used.

Important note:

Same key -> same partition  
Different keys -> may land in different partitions

---

## Step 6: show where the data lives

Use two views together:

1. **Kafbat UI** for partition/broker metadata
2. **terminal or Docker volume mapping** for storage explanation

Explain:

- each broker stores data for the partitions it leads or replicates
- Kafka persists records as append-only logs on disk
- replicas are stored on different brokers

You do not need to browse deep Kafka files unless you want a brief advanced aside.

---

## Step 7: live-code the consumer

Open:

- consumer properties
- consumer config
- listener class

Explain options such as:

- `group.id`
- `auto.offset.reset`
- auto-commit vs manual acknowledgement
- concurrency
- max poll records

### Consumer live-code target

Implement:

1. `@KafkaListener`
2. deserialize into the event object
3. log key / partition / offset
4. call repository save method

Teaching point:

“Consumers read partition-ordered records, and the consumer group controls scaling.”

---

## Step 8: live-code Postgres persistence

Implement the repository insert.

Recommended persisted fields:

- symbol
- price
- currency
- event time
- partition
- offset
- consumed at

Then show the resulting rows in Postgres.

Teaching point:

“Kafka is the event backbone; consumers can project those events into databases for query use cases.”

---

## 10. Detailed scaffolding checklist

## 10.1 Files to prepare

### Infrastructure

- `docker-compose.yml`
- `.env` for local container settings if needed
- `scripts/create-topics.ps1` or `.sh`
- `scripts/reset-demo.ps1` or `.sh`
- `postgres/init/001-schema.sql`

### Spring Boot

- `pom.xml`
- `src/main/resources/application.yml`
- `src/main/resources/application-local.yml`
- `src/main/java/nh/kafkahandson/config/...`
- `src/main/java/nh/kafkahandson/api/...`
- `src/main/java/nh/kafkahandson/producer/...`
- `src/main/java/nh/kafkahandson/consumer/...`
- `src/main/java/nh/kafkahandson/persistence/...`

### Optional

- `http/stock-api-samples.http`
- `docs/demo-commands.txt` for your private rehearsal notes

---

## 10.2 Spring classes to create in advance

### Configuration

- `KafkaProducerProperties`
- `KafkaConsumerProperties`
- `StockApiProperties`
- `KafkaProducerConfig`
- `KafkaConsumerConfig`

### API client

- `StockPriceClient`
- `StockPriceResponse`
- `StockQuoteDto`

### Domain

- `StockPriceEvent`

### Producer side

- `StockPricePoller`
- `StockPriceProducer`

### Consumer side

- `StockPriceEventListener`

### Persistence

- `StockPriceEventRepository`

---

## 11. Suggested coding boundaries: what to prebuild vs live-code

## Prebuild

- Docker Compose
- topic creation
- Postgres schema
- all configuration properties
- all DTOs
- all package structure
- Kafka template / factory bean scaffolding
- HTTP client wiring
- repository wiring
- scheduler wiring

## Live-code

- producer `send` implementation
- poller mapping and publish call
- consumer listener implementation
- repository insert implementation

This keeps the demo fast, understandable, and safe.

---

## 12. Suggested rehearsal checklist

Before the presentation, rehearse this exact flow:

1. `docker compose up -d`
2. verify all three brokers are healthy
3. verify `stock-prices` topic exists with 3 partitions and RF=3
4. open Kafbat UI and confirm topic metadata is visible
5. start the Spring Boot app
6. verify API returns changing data
7. verify producer writes records
8. verify records are visible in Kafbat UI
9. verify consumer stores rows in Postgres
10. rehearse one “something failed” fallback path

### Fallbacks to prepare

1. public API unavailable -> switch to mock API property
2. topic missing -> rerun topic creation script
3. consumer broke -> show existing produced messages in UI and recover
4. DB issue -> keep consumer logging path as minimum viable ending

---

## 13. Talking points you can attach to each step

### When showing the producer

- producers write records to topics
- keys influence partitioning
- config controls durability and throughput tradeoffs

### When showing the cluster

- brokers host leaders and replicas
- partitions are the unit of parallelism and ordering
- replication provides resilience

### When showing the consumer

- consumers read from partitions
- consumer groups enable scale-out
- offsets track progress

### When showing Postgres

- Kafka is not your OLTP query store
- consumers materialize data into systems optimized for serving/querying

---

## 14. Strong recommendation on scope

Do **not** try to live-code:

- full Docker setup
- advanced serializers
- Avro/Schema Registry
- security/TLS/SASL
- complex retry/DLT logic
- multi-service decomposition

Those are great follow-up topics, but they will dilute this introductory demo.

For this audience, the best demo is:

- small
- visual
- property-driven
- end-to-end
- uses a real changing data source

---

## 15. Recommended next implementation order in this repository

When moving from plan to build, implement in this order:

1. Docker Compose with 3 Kafka brokers, Kafbat UI, and Postgres
2. topic/bootstrap scripts
3. Maven dependencies
4. `application.yml` + config properties classes
5. stock API client and DTOs
6. Kafka producer config + producer service skeleton
7. scheduler/poller skeleton
8. Kafka consumer config + listener skeleton
9. Postgres schema + repository
10. final live-coding TODO markers

---

## 16. Open decisions still worth locking down before implementation

These are the only remaining decisions that matter materially:

1. exact stock API provider
2. exact Docker images for Kafka and Kafbat UI
3. whether to use JSON serializer/deserializer directly or manual string payload mapping
4. whether Postgres persistence should use `JdbcTemplate` or Spring Data JPA

### Recommended choices

1. choose a public stock API **plus local fallback**
2. use a **3-broker KRaft cluster**
3. use **JSON payloads**
4. use **JdbcTemplate**

---

## 17. Final recommendation

The safest, clearest presentation setup is:

1. **pre-run** Docker Compose with a 3-broker Kafka cluster, Kafbat UI, and Postgres
2. **prebuild** all config classes, DTOs, HTTP client, scheduler shell, and persistence shell
3. **live-code** only the producer send flow, consumer listener flow, and DB insert flow
4. use **stock symbols as keys** to demonstrate partitioning
5. use **Kafbat UI** to show partitions, leaders, replicas, messages, and consumer groups
6. use a **terminal/volume explanation** for where Kafka physically stores data

This gives you a polished 30-minute technical story with enough depth to teach Kafka fundamentals without getting lost in setup.