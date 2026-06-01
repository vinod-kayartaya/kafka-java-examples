# Kafka Connect

Kafka Connect is a framework within Apache Kafka that allows you to move data between Kafka and external systems without writing custom producer or consumer code.

# Why Kafka Connect?

Without Kafka Connect:

```text
Database → Custom Producer → Kafka
Kafka → Custom Consumer → Database
```

You must write, deploy, monitor, and maintain custom code.

With Kafka Connect:

```text
Database → Source Connector → Kafka
Kafka → Sink Connector → Database
```

No custom application is required.

# Kafka Connect Architecture

```text
                +------------------+
                | Kafka Connect    |
                | Worker           |
                +--------+---------+
                         |
        +----------------+----------------+
        |                                 |
+-------v------+                 +--------v------+
| Source       |                 | Sink          |
| Connector    |                 | Connector     |
+-------+------+                 +--------+------+
        |                                 |
        |                                 |
+-------v------+                 +--------v------+
| Database     |                 | PostgreSQL    |
| MySQL        |                 | Elasticsearch |
+--------------+                 +---------------+

                Kafka Topics
```

### Components

#### Worker

A Kafka Connect process that executes connectors.

#### Connector

Defines the integration logic.

Examples:

- JDBC Source Connector
- JDBC Sink Connector
- Elasticsearch Sink Connector
- S3 Sink Connector

#### Tasks

Actual units of work executed by a connector.

One connector may run multiple tasks in parallel.

# Types of Connectors

## 1. Source Connector

Reads data from an external system and writes it into Kafka.

Example:

```text
MySQL Customer Table
        |
        v
JDBC Source Connector
        |
        v
customer-topic
```

## 2. Sink Connector

Reads data from Kafka and writes it to an external system.

Example:

```text
payment-topic
        |
        v
JDBC Sink Connector
        |
        v
PostgreSQL
```

# Standalone vs Distributed Mode

## Standalone Mode

Single worker process.

```text
Worker
  |
  +-- Connector
```

Suitable for:

- Development
- Testing
- Learning

## Distributed Mode

Multiple workers share workload.

```text
Worker 1
Worker 2
Worker 3

      |
      v

Kafka Connect Cluster
```

Suitable for:

- Production
- Fault tolerance
- Scalability

# Example: PostgreSQL → Kafka

Suppose we have a table:

```sql
CREATE TABLE payments (
    id INT,
    customer_id VARCHAR(20),
    amount DECIMAL(10,2)
);
```

JDBC Source Connector reads rows:

```text
payments table
      |
      v
JDBC Source Connector
      |
      v
payments-topic
```

Resulting Kafka message:

```json
{
  "id": 101,
  "customer_id": "C100",
  "amount": 2500
}
```

# Example: Kafka → PostgreSQL

A payment service produces events:

```json
{
  "paymentId": "P1001",
  "amount": 5000
}
```

JDBC Sink Connector writes them into PostgreSQL.

```text
payment-topic
      |
      v
JDBC Sink Connector
      |
      v
payments table
```

# Advantages of Kafka Connect

### Reduced Development Effort

No custom producer or consumer code.

### Scalability

Connectors can run multiple tasks.

### Fault Tolerance

Distributed mode automatically recovers from failures.

### Offset Management

Kafka Connect automatically tracks progress.

### Ecosystem Support

Hundreds of connectors are available for:

- Databases
- Elasticsearch
- MongoDB
- S3
- BigQuery
- Redis
- Snowflake

# Kafka Connect vs Custom Java Code

| Feature           | Kafka Connect         | Custom Java            |
| ----------------- | --------------------- | ---------------------- |
| Coding Required   | Minimal               | High                   |
| Maintenance       | Low                   | High                   |
| Scaling           | Built-in              | Manual                 |
| Offset Management | Automatic             | Manual                 |
| Best For          | Standard integrations | Complex business logic |

# Real-World Example (Payment System)

From your training project, a typical Day 4 integration would be:

```text
Payment Service
       |
       v
payment-topic
       |
       v
Kafka Connect JDBC Sink
       |
       v
PostgreSQL Database
```

This allows every payment event flowing through Kafka to be automatically persisted to the database without writing a consumer application.

# Key Takeaways

- Kafka Connect is Kafka's data integration framework.
- Source Connectors move data **into Kafka**.
- Sink Connectors move data **out of Kafka**.
- Connectors run inside Kafka Connect workers.
- Supports standalone and distributed deployments.
- Eliminates the need for boilerplate producer/consumer code.
- Ideal for database, search engine, cloud storage, and analytics integrations.
