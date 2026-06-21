# Spring Kafka Infrastructure

![Java](https://img.shields.io/badge/Java-17-orange?logo=openjdk)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3-brightgreen?logo=springboot)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.6-231F20?logo=apachekafka)
![Avro](https://img.shields.io/badge/Avro-1.11-blue)
![GitHub last commit](https://img.shields.io/github/last-commit/erhanbarisolmez/spring-kafka-infrastructure)
![GitHub repo size](https://img.shields.io/github/repo-size/erhanbarisolmez/spring-kafka-infrastructure)

Enterprise-grade Apache Kafka infrastructure library for Spring Boot applications. Provides a centralized topic registry, automated Kafka Connect management, Dead Letter Queue (DLQ) handling, Transactional Outbox pattern, and Avro-based event serialization with Confluent Schema Registry.

## Architecture

```mermaid
graph TB
    subgraph Producers
        P1[Domain Services]
        P2[Outbox Poller]
    end

    subgraph Kafka Infrastructure
        TR[Topic Registry]
        TI[Topic Initializer]
        TV[Topic Verifier]
        EC[Event Coordinator]
        ES[Event Serialization]
    end

    subgraph Apache Kafka
        KB[Kafka Broker]
        SR[Schema Registry]
    end

    subgraph Consumer Pipeline
        CC[Central Event Consumer]
        HR[Event Handler Registry]
        EH1[Event Handler 1]
        EH2[Event Handler 2]
        EH3[Event Handler N]
    end

    subgraph DLQ Management
        DQ[DLQ Consumer]
        DR[DLQ Repository]
        DRP[DLQ Reprocessing]
        DC[DLQ Management API]
    end

    subgraph Outbox Pattern
        OE[Outbox Event Entity]
        OR[Outbox Repository]
        OP[Outbox Poller]
        OH[Outbox Health]
    end

    subgraph Kafka Connect
        KC[Kafka Connect Client]
        CR[Connector Reconciler]
        HW[Health Watcher]
        ESB[Elasticsearch Builder]
    end

    subgraph External
        ES_DB[(PostgreSQL)]
        ELS[Elasticsearch]
    end

    P1 --> EC
    EC --> ES --> KB
    ES --> SR
    TR --> TI --> KB
    TR --> TV

    KB --> CC
    CC --> HR
    HR --> EH1 & EH2 & EH3
    CC -->|Failed| DQ
    DQ --> DR --> ES_DB
    DC --> DRP --> KB

    P1 --> OE --> OR --> ES_DB
    OP --> OR
    OP --> EC
    P2 --> EC

    HW --> KC
    KC --> CR
    CR --> ESB
    KC --> ELS
```

## Technology Stack

| Technology | Purpose |
|---|---|
| Java 17 | Runtime |
| Spring Boot 3.3 | Application framework |
| Apache Kafka | Event streaming platform |
| Confluent Schema Registry | Schema management and evolution |
| Apache Avro | Event serialization format |
| Spring WebFlux | Non-blocking Kafka Connect REST client |
| PostgreSQL | Outbox and DLQ persistence |
| Resilience4j | Circuit breaker for Kafka Connect operations |
| Micrometer / Prometheus | Metrics and monitoring |
| Lombok | Boilerplate reduction |

## Key Features

### Topic Registry & Management
- **Declarative Topic Configuration** - Define topics in `application.yml` with naming conventions, partitions, replication factor, and retention policies
- **Automatic Topic Initialization** - Topics are created on application startup with verified configuration
- **Topic Name Resolution** - Consistent topic naming via `TopicNameResolver` and `TopicNamingConverter`
- **Category-Based Organization** - Topics categorized as EVENT, STREAM, ALERT, ANOMALY, METRIC

### Event Processing Pipeline
- **Central Event Consumer** - Single consumer that routes events to registered handlers via `EventHandlerRegistry`
- **Event Serialization** - Avro-based serialization with `EventSerializationService` and `EventTypeRegistry`
- **Event Converter Framework** - Pluggable converters (`AbstractEventConverter`, `EventConverterFactory`) for transforming events between formats

### Dead Letter Queue (DLQ)
- **Automatic DLQ Routing** - Failed events are automatically sent to DLQ topics
- **DLQ Persistence** - Failed events stored in PostgreSQL with error details
- **Reprocessing Service** - Retry failed events with configurable strategies
- **Management API** - REST endpoints for DLQ monitoring and manual reprocessing
- **Health Monitoring** - `DLQHealthStatus` for operational visibility

### Transactional Outbox Pattern
- **Outbox Event Entity** - JPA entity for reliable event publishing
- **Outbox Poller** - Scheduled polling of outbox table for unpublished events
- **Guaranteed Delivery** - Events are persisted in the same transaction as business data
- **Health Monitoring** - `OutboxHealthStatus` for lag and throughput tracking

### Kafka Connect Automation
- **WebClient-Based REST Client** - Non-blocking communication with Kafka Connect API
- **Connector Reconciliation** - Automatically creates/updates connectors to match desired state
- **Elasticsearch Connector Builder** - Pre-configured builder for Elasticsearch sink connectors
- **Health Watcher** - Monitors Kafka Connect cluster health with startup delay support
- **Auto-Bind by Category** - Automatically creates connectors for topics matching specified categories

## Project Structure

```
src/main/java/com/selftech/kafka/
├── KafkaInfrastructureApplication.java
├── config/                                 # Kafka configuration
│   ├── KafkaConfig.java                    # Base Kafka configuration
│   ├── KafkaConsumerConfig.java            # Consumer factory and settings
│   ├── KafkaProducerConfig.java            # Producer factory and settings
│   ├── KafkaStreamsConfig.java             # Streams configuration
│   ├── KafkaTopicInitializer.java          # Auto-creates topics on startup
│   ├── KafkaTopicRegistry.java             # Central topic registry
│   ├── KafkaTopicVerifier.java             # Validates topic configurations
│   ├── TopicCategory.java                  # Topic category enum
│   ├── TopicConfig.java                    # Topic configuration properties
│   ├── TopicDefinition.java                # Topic definition model
│   ├── TopicKey.java                       # Topic identifier
│   ├── TopicNameResolver.java              # Resolves topic names from keys
│   └── TopicNamingConverter.java           # Naming convention converter
├── connect/                                # Kafka Connect automation
│   ├── builder/
│   │   └── ElasticsearchConnectorSpecBuilder.java
│   ├── client/KafkaConnectClient.java      # WebClient REST client
│   ├── config/
│   │   ├── ElasticsearchConnectorProperties.java
│   │   ├── KafkaConnectProperties.java
│   │   └── WebClientConfig.java
│   ├── model/
│   │   ├── ConnectorDefinition.java
│   │   ├── ConnectorSpec.java
│   │   ├── ConnectorStatus.java
│   │   └── ReconciliationResult.java
│   └── reconciler/
│       ├── ConnectorReconciler.java        # Desired state reconciliation
│       └── KafkaConnectHealthWatcher.java
├── converter/                              # Event format conversion
│   ├── AbstractEventConverter.java
│   ├── EventConversionException.java
│   ├── EventConverter.java
│   ├── EventConverterFactory.java
│   └── converters/
│       ├── LockBoxEventConverter.java
│       └── SensorDataEventConverter.java
├── core/                                   # Core event processing
│   ├── config/EventHandlerConfiguration.java
│   ├── consumer/CentralEventConsumer.java
│   ├── dlq/                                # Dead Letter Queue
│   │   ├── DLQConsumer.java
│   │   ├── DLQEvent.java
│   │   ├── DLQEventRepository.java
│   │   ├── DLQEventService.java
│   │   ├── DLQHealthStatus.java
│   │   ├── DLQManagementController.java
│   │   └── DLQReprocessingService.java
│   ├── event/Event.java
│   ├── handler/
│   │   ├── EventHandler.java
│   │   └── EventHandlerRegistry.java
│   ├── outbox/                             # Transactional Outbox
│   │   ├── OutboxEvent.java
│   │   ├── OutboxEventRepository.java
│   │   ├── OutboxEventService.java
│   │   ├── OutboxHealthStatus.java
│   │   └── OutboxPoller.java
│   ├── publisher/CentralEventCoordinator.java
│   └── serialization/
│       ├── EventSerializationService.java
│       └── EventTypeRegistry.java
└── event/publisher/
    └── IEventPublisherService.java         # Publisher interface
```

## Prerequisites

- Java 17+
- Apache Kafka 3.6+
- Confluent Schema Registry
- PostgreSQL 14+
- Kafka Connect (optional, for connector automation)
- Elasticsearch (optional, for sink connectors)
- Maven 3.8+

## Usage as Library

This project is a **shared Maven library** (not a standalone application). It is published to GitHub Packages and consumed by:

- [kafka-streams-anomaly-detection](https://github.com/erhanbarisolmez/kafka-streams-anomaly-detection) - Anomaly detection engine
- [iot-device-management-platform](https://github.com/erhanbarisolmez/iot-device-management-platform) - Smart lock management platform

### Service Dependency Diagram

```
┌──────────────────────────────────┐
│  spring-kafka-infrastructure     │
│  (shared Maven library - JAR)    │
└──────────┬───────────┬───────────┘
           │           │
     dependency    dependency
           │           │
           ▼           ▼
┌─────────────────┐  ┌──────────────────────────┐
│ anomaly-detection│  │ iot-device-management    │
│ (K8s Pod)        │  │ (K8s Pod)                │
└─────────────────┘  └──────────────────────────┘
```

### Add as Dependency

In your `pom.xml`:

```xml
<repositories>
    <repository>
        <id>github-kafka-infra</id>
        <url>https://maven.pkg.github.com/erhanbarisolmez/spring-kafka-infrastructure</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.selftech</groupId>
    <artifactId>spring-kafka-infrastructure</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

Add `scanBasePackages` to your Spring Boot application:

```java
@SpringBootApplication(scanBasePackages = {"com.selftech.yourapp", "com.selftech.kafka"})
```

### Register Event Types

Each consuming service registers its own event types with the dynamic `EventTypeRegistry`:

```java
@Configuration
@RequiredArgsConstructor
public class EventTypeRegistrarConfig {
    private final EventTypeRegistry eventTypeRegistry;

    @PostConstruct
    public void registerEventTypes() {
        eventTypeRegistry.register("MyEvent", MyEvent.class);
    }
}
```

### Getting Started

1. **Clone the repository**
   ```bash
   git clone https://github.com/erhanbarisolmez/spring-kafka-infrastructure.git
   cd spring-kafka-infrastructure
   ```

2. **Build and install locally**
   ```bash
   mvn clean install
   ```

3. **Publish to GitHub Packages** (automated via GitHub Actions on push to `main`)
   ```bash
   mvn deploy
   ```

## Configuration

Topics are defined declaratively in `application.yml`:

```yaml
app:
  kafka:
    default-partitions: 3
    default-replication-factor: 1
    topics:
      my-domain-events:
        name: my-domain.event.v0
        category: EVENT
        retention-days: 30
```

Kafka Connect automation is configured under `app.kafka.connect`:

```yaml
app:
  kafka:
    connect:
      url: http://kafka-connect:8083
      elasticsearch:
        enabled: true
        auto-bind:
          categories:
            ANOMALY: elasticsearch
            ALERT: elasticsearch
```
