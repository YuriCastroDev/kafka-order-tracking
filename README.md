# 📦 Kafka Order Tracking

Real-time order tracking system built with **Java 21**, **Spring Boot 3** and **Apache Kafka**.  
Each order status change is published as an event and consumed by independent services, demonstrating core Kafka concepts such as producers, consumers, idempotency, retry policies, and dead letter queues.

---

## 🏗️ Architecture

```
[Order API]
     │
     │  publishes: OrderCreatedEvent
     ▼
[Topic: order-events]
     │
     ├──► [OrderStatusConsumer] ──► [PostgreSQL]
     │         └── on failure: retry 3x (2s backoff)
     │                   └── [Topic: order-events.DLT]
     │                              └──► [DeadLetterConsumer] ──► [failed_events table]
     │
     └──► (extensible for notification, audit, etc.)
```

---

## 🛠️ Tech Stack

| Technology | Version | Purpose |
|---|---|---|
| Java | 21 | Language |
| Spring Boot | 3.5 | Framework |
| Spring Kafka | 3.3 | Kafka integration |
| Spring Data JPA | - | Database access |
| PostgreSQL | 16 | Persistence |
| Apache Kafka | 3.9 | Event streaming |
| Docker Compose | - | Local infrastructure |
| Kafdrop | latest | Kafka UI |
| JUnit 5 + Mockito | - | Unit tests |
| EmbeddedKafka | - | Integration tests |
| Awaitility | - | Async test assertions |

---

## ▶️ Running Locally

### Prerequisites

- Docker Desktop
- Java 21 (Temurin recommended)
- Maven

### Steps

**1. Start infrastructure**
```bash
docker-compose up -d
```

This starts:
- Kafka on `localhost:9092`
- Zookeeper
- PostgreSQL on `localhost:5432`
- Kafdrop UI on `http://localhost:9000`

**2. Run the application**
```bash
./mvnw spring-boot:run
```

**3. Run tests**
```bash
./mvnw test
```

---

## 📮 API Endpoints

### Create order
```http
POST /orders
Content-Type: application/json

{
  "customerName": "John Doe",
  "productName": "Mechanical Keyboard",
  "quantity": 1,
  "price": 150.00
}
```

### Get order
```http
GET /orders/{id}
```

### Update order status
```http
PUT /orders/{id}/status?status=SHIPPED
```

Available statuses: `CREATED` → `PROCESSING` → `SHIPPED` → `DELIVERED` → `CANCELLED`

---

## 🔄 Event Flow

1. `POST /orders` saves the order to the database and publishes an `OrderCreatedEvent` to the `order-events` topic
2. `OrderStatusConsumer` consumes the event and updates the order status to `PROCESSING`
3. Before processing, an **idempotency check** verifies if the event was already handled using the `processed_events` table
4. If the consumer fails, Spring Kafka retries **3 times** with a **2-second backoff**
5. After exhausting retries, the message is sent to `order-events.DLT`
6. `DeadLetterConsumer` consumes the DLT and persists the failed event to the `failed_events` table for manual review

---

## 💡 Key Concepts Demonstrated

| Concept | Where |
|---|---|
| Kafka Producer | `OrderEventProducer` — publishes events with order ID as key |
| Kafka Consumer | `OrderStatusConsumer` — `@KafkaListener` with consumer group |
| Consumer Groups | `order-status-group`, `order-dlt-group` |
| Idempotency | `ProcessedEvent` table — skips already-processed event IDs |
| Retry with backoff | `DefaultErrorHandler` — 3 retries, 2s interval |
| Dead Letter Queue | `order-events.DLT` + `DeadLetterConsumer` |
| Event-driven status | Order status updated exclusively through events |
| Integration tests | `@EmbeddedKafka` — no broker needed to run tests |

---

## 🗂️ Project Structure

```
src/
├── main/java/com/zs/kafka_order_tracking/
│   ├── config/
│   │   └── KafkaConsumerConfig.java      # Retry, DLT and error handler setup
│   ├── consumer/
│   │   ├── OrderStatusConsumer.java      # Processes order events
│   │   └── DeadLetterConsumer.java       # Handles failed messages
│   ├── controller/
│   │   └── OrderController.java          # REST endpoints
│   ├── dto/
│   │   └── OrderRequest.java
│   ├── entity/
│   │   ├── Order.java
│   │   ├── OrderStatus.java
│   │   ├── ProcessedEvent.java           # Idempotency table
│   │   └── FailedEvent.java              # DLT persistence table
│   ├── event/
│   │   └── OrderCreatedEvent.java        # Kafka event DTO
│   ├── producer/
│   │   └── OrderEventProducer.java
│   ├── repository/
│   │   ├── OrderRepository.java
│   │   ├── ProcessedEventRepository.java
│   │   └── FailedEventRepository.java
│   └── service/
│       └── OrderService.java
└── test/java/com/zs/kafka_order_tracking/
    ├── consumer/
    │   └── OrderConsumerIntegrationTest.java
    ├── producer/
    │   ├── OrderEventProducerTest.java
    │   └── OrderProducerIntegrationTest.java
    └── service/
        └── OrderServiceTest.java
```

---

## 🧪 Tests

| Test | Type | What it covers |
|---|---|---|
| `OrderServiceTest` | Unit | createOrder, getOrder, updateStatus, error handling |
| `OrderEventProducerTest` | Unit | Correct topic, key and payload |
| `OrderProducerIntegrationTest` | Integration | Event published without errors using EmbeddedKafka |
| `OrderConsumerIntegrationTest` | Integration | Status updated to PROCESSING + duplicate event ignored |
