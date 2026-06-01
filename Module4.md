# Day 3 – Module 4: Kafka Streams

## Module Overview

Kafka Streams is a lightweight Java library for building real-time stream processing applications directly on top of Apache Kafka. Instead of writing custom consumers and producers and managing state manually, Kafka Streams provides a high-level API for transforming, filtering, aggregating, joining, and analyzing streaming data.

In our **Payment Processing & Fraud Detection System**, Kafka Streams will continuously analyze transaction events and identify suspicious activity in real time.

---

# Learning Objectives

By the end of this module, you will be able to:

- Understand stream processing fundamentals
- Differentiate between stateless and stateful processing
- Understand KStream and KTable concepts
- Implement real-time filtering and transformations
- Perform aggregations on streaming data
- Use windowing for time-based analysis
- Build a fraud detection pipeline using Kafka Streams
- Understand state stores and fault tolerance

---

# 1. Why Kafka Streams?

Traditional applications often work with stored data:

```text
Database --> Query --> Result
```

Stream processing works with continuously arriving data:

```text
Transaction Event
       ↓
Kafka Topic
       ↓
Kafka Streams
       ↓
Fraud Detection Alert
```

Instead of waiting for a report to run every hour, events are processed immediately.

---

# Real-World Example

Imagine a payment platform:

```text
Customer Pays ₹10,000
       ↓
Transaction Event Created
       ↓
Kafka Topic
       ↓
Kafka Streams Application
       ↓
Fraud Rules Executed
       ↓
Alert Generated
```

Response time becomes milliseconds instead of minutes.

---

# 2. What is Kafka Streams?

Kafka Streams is:

- A Java library
- Runs inside your application
- No separate cluster required
- Highly scalable
- Fault tolerant
- Supports exactly-once processing

Architecture:

```text
+------------------+
| Kafka Streams App|
+------------------+
       ↑
       |
   Kafka Topics
       |
+------+------+
| Kafka Cluster|
+-------------+
```

---

# 3. Stream Processing Concepts

A stream is an unbounded sequence of events.

Example:

```json
{
  "transactionId": "TX1001",
  "customerId": "C100",
  "amount": 5000,
  "timestamp": "2026-05-31T10:15:00"
}
```

Events keep arriving:

```text
Event 1
Event 2
Event 3
Event 4
...
Event N
```

Potentially forever.

---

# 4. Stateless vs Stateful Processing

This is one of the most important Kafka Streams concepts.

---

## Stateless Processing

Each event is processed independently.

Current event only matters.

### Example: High Value Detection

Input:

```json
{"amount": 500}
{"amount": 20000}
{"amount": 800}
```

Rule:

```java
amount > 10000
```

Output:

```json
{ "amount": 20000 }
```

No memory of previous events.

---

### Stateless Operations

- filter()
- map()
- flatMap()
- selectKey()
- peek()

Example:

```java
stream.filter(
    (key, tx) -> tx.getAmount() > 10000
);
```

---

## Stateful Processing

Processing depends on previous events.

Application maintains state.

Example:

```text
Customer performs:

₹5000
₹7000
₹9000
```

Total:

```text
₹21000
```

Decision depends on accumulated value.

---

### Stateful Operations

- count()
- aggregate()
- reduce()
- joins
- windowing

Kafka Streams stores state locally and replicates it to Kafka.

---

# Comparison

| Feature                | Stateless         | Stateful               |
| ---------------------- | ----------------- | ---------------------- |
| Memory Required        | No                | Yes                    |
| Previous Events Needed | No                | Yes                    |
| Complexity             | Low               | Higher                 |
| Examples               | Filter, Transform | Count, Aggregate, Join |

---

# 5. Kafka Streams Architecture

A stream processing topology defines the flow.

```text
Input Topic
     ↓
 Filter
     ↓
 Transform
     ↓
 Aggregate
     ↓
Output Topic
```

Example:

```text
transactions
      ↓
high-value-filter
      ↓
fraud-alerts
```

---

# 6. KStream

KStream represents an immutable stream of events.

Each record is independent.

```java
KStream<String, Transaction>
```

Example:

```java
transactionsStream
```

Containing:

```text
TX1
TX2
TX3
TX4
```

Every transaction remains an individual event.

---

## Characteristics

- Event by event processing
- Supports transformations
- Supports filtering
- Supports joins
- Ordered within partition

---

# KStream Example

```java
KStream<String, Transaction> transactions =
        builder.stream("transactions");
```

Filtering high-value payments:

```java
KStream<String, Transaction> highValue =
        transactions.filter(
            (key, tx) -> tx.getAmount() > 10000
        );
```

---

# 7. KTable

A KTable represents the latest state of a key.

Think of it as a continuously updated table.

Example topic:

```text
Customer A = Gold
Customer B = Silver
```

Update:

```text
Customer A = Platinum
```

KTable stores:

```text
Customer A = Platinum
Customer B = Silver
```

Only latest value is maintained.

---

## KTable Analogy

Database Table:

| Customer | Status   |
| -------- | -------- |
| A        | Platinum |
| B        | Silver   |

Kafka Streams continuously updates this table.

---

## Creating KTable

```java
KTable<String, Customer> customers =
        builder.table("customer-status");
```

---

# KStream vs KTable

| Feature        | KStream      | KTable            |
| -------------- | ------------ | ----------------- |
| Nature         | Event Stream | State Table       |
| Stores History | Yes          | Latest Value      |
| Updates        | New Events   | Upserts           |
| Use Case       | Transactions | Customer Profiles |

---

# Example

KStream:

```text
A -> 10
A -> 20
A -> 30
```

All events remain.

KTable:

```text
A = 10
A = 20
A = 30
```

Current value:

```text
A = 30
```

---

# 8. Transformations

Transformations create new streams.

---

## Map

Input:

```json
{
  "customerId": "C100",
  "amount": 5000
}
```

Output:

```java
.map((key, tx) ->
    KeyValue.pair(
        tx.getCustomerId(),
        tx.getAmount()
    )
)
```

Result:

```text
C100 → 5000
```

---

## Filter

```java
.filter(
   (key, tx) -> tx.getAmount() > 10000
)
```

Only suspicious transactions pass.

---

## Peek

Useful for debugging.

```java
.peek(
   (key, value) ->
      System.out.println(value)
)
```

---

# 9. Aggregations

Aggregations summarize data.

Example:

```text
Customer A:
5000
7000
2000
```

Total:

```text
14000
```

---

## Count

```java
stream
    .groupByKey()
    .count();
```

Output:

```text
Customer A = 3
```

---

## Sum

```java
.aggregate(
   () -> 0.0,
   (key, value, total) ->
       total + value
);
```

Output:

```text
Customer A = 14000
```

---

# 10. Windowing Concepts

Windowing is essential for fraud detection.

Without windows:

```text
Count transactions forever
```

Not useful.

Instead:

```text
Count transactions in last 5 minutes
```

---

# Why Windows?

Fraud Rule:

```text
More than 5 transactions
within 1 minute
```

Requires a time window.

---

# Tumbling Window

Fixed intervals.

```text
10:00 - 10:05
10:05 - 10:10
10:10 - 10:15
```

Events belong to one window only.

---

# Sliding Window

Continuously moving window.

```text
Last 5 Minutes
```

Provides smoother analytics.

---

# Hopping Window

Overlap exists.

```text
Window Size = 10 min
Advance = 5 min
```

```text
10:00-10:10
10:05-10:15
10:10-10:20
```

---

# Window Example

```java
.groupByKey()
.windowedBy(
     TimeWindows.ofSizeWithNoGrace(
         Duration.ofMinutes(5)
     )
)
.count();
```

Result:

```text
Customer A = 12 transactions
during last 5 minutes
```

---

# 11. Real-Time Fraud Detection Example

Requirement:

```text
Alert if customer performs
more than 3 transactions
within 60 seconds.
```

---

## Input Events

```text
10:00:01
10:00:15
10:00:25
10:00:40
```

Count:

```text
4 transactions
```

Generate alert.

---

## Processing Flow

```text
transactions
      ↓
groupBy(customer)
      ↓
1-minute window
      ↓
count
      ↓
count > 3
      ↓
fraud-alerts
```

---

# 12. State Stores

Stateful operations need storage.

Kafka Streams uses:

```text
Local State Store
```

Internally:

```text
RocksDB
```

Stores:

- Counts
- Aggregates
- Window state
- Join state

---

# Fault Tolerance

State is backed up into Kafka.

```text
State Store
      ↓
Changelog Topic
      ↓
Recovery
```

If application crashes:

```text
Restart
Restore State
Continue Processing
```

---

# 13. Kafka Streams Processing Guarantee

Kafka Streams supports:

### At-Least-Once

```text
Possible duplicates
```

### Exactly-Once

```text
No duplicates
No data loss
```

Configuration:

```java
StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
StreamsConfig.EXACTLY_ONCE_V2
```

---

# 14. Payment Fraud Detection Architecture

```text
Transaction Producer
          ↓
      transactions
          ↓
   Kafka Streams
          ↓
   Fraud Detection
          ↓
     fraud-alerts
          ↓
 Alert Consumer
```

This is the exact processing pattern used in modern payment platforms.

---

# Hands-On Lab Exercise

### Objective

Build a Kafka Streams application that identifies suspicious payment activity.

---

## Step 1

Create topic:

```bash
kafka-topics.sh \
--create \
--topic transactions
```

---

## Step 2

Create producer.

Generate transactions:

```json
{
  "customerId": "C100",
  "amount": 15000
}
```

---

## Step 3

Kafka Streams Application

```java
builder.stream("transactions")
       .filter(
           (key, tx) ->
               tx.getAmount() > 10000
       )
       .to("fraud-alerts");
```

---

## Step 4

Consume fraud alerts.

```bash
kafka-console-consumer.sh \
--topic fraud-alerts
```

Observe suspicious transactions.

---

# Module Summary

In this module you learned:

- Fundamentals of stream processing
- Kafka Streams architecture
- Stateless processing
- Stateful processing
- KStream and KTable
- Transformations and aggregations
- Windowing concepts
- State stores and fault tolerance
- Real-time fraud detection patterns
- Building Kafka Streams applications

This module prepares you for the Day 3 lab where they will implement Kafka Streams-based fraud pattern detection in the Payment Processing & Fraud Detection System.
