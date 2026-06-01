# Day 3 – Module 2: Kafka Consumer Deep Dive

## Module Overview

In this module, we move from producing events to consuming them efficiently and reliably.

A Kafka producer writes records to topics, but the real business value is realized when consumers process those records. Understanding how consumers work is essential for building scalable, fault-tolerant, and high-performance event-driven systems.

Using our **Payment Processing & Fraud Detection System**, we will learn how consumer applications process payment events, distribute workload, recover from failures, and maintain processing state.

---

# Learning Objectives

By the end of this module, you will be able to:

- Understand Kafka Consumer architecture
- Explain Consumer Groups
- Understand partition assignment strategies
- Explain Consumer Rebalancing
- Manage offsets effectively
- Design scalable consumer applications
- Build fault-tolerant event processing systems

---

# 1. Kafka Consumer Fundamentals

## What is a Consumer?

A consumer is an application that reads records from Kafka topics.

Example:

A payment service publishes:

```json
{
  "transactionId": "TXN1001",
  "customerId": "C001",
  "amount": 25000
}
```

A fraud detection consumer reads this event and determines whether it is suspicious.

---

## Consumer Workflow

```text
Producer
   |
   V
Kafka Topic
   |
   V
Consumer
```

Consumer responsibilities:

1. Connect to Kafka cluster
2. Subscribe to topics
3. Poll records continuously
4. Process messages
5. Commit offsets

---

## Basic Consumer Example

```java
Properties props = new Properties();

props.put(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
    "localhost:9092");

props.put(
    ConsumerConfig.GROUP_ID_CONFIG,
    "fraud-detector-group");

props.put(
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
    StringDeserializer.class.getName());

props.put(
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    StringDeserializer.class.getName());

KafkaConsumer<String, String> consumer =
    new KafkaConsumer<>(props);

consumer.subscribe(
    List.of("payment-transactions"));

while(true)
{
    ConsumerRecords<String,String> records =
        consumer.poll(Duration.ofMillis(100));

    for(ConsumerRecord<String,String> record : records)
    {
        System.out.println(record.value());
    }
}
```

---

# 2. Consumer Groups

## Why Consumer Groups?

Suppose:

```text
Topic:
payment-transactions

Messages:
1
2
3
4
5
6
7
8
9
10
```

One consumer may become overloaded.

Solution:

```text
Consumer Group
--------------
Consumer A
Consumer B
Consumer C
```

Kafka automatically distributes partitions among consumers.

---

## Key Rule

Within a consumer group:

```text
One partition
      →
One consumer
```

A partition cannot be actively consumed by multiple consumers within the same group.

---

## Example

Topic:

```text
payment-transactions

Partitions:
P0
P1
P2
```

Consumer Group:

```text
Consumer A
Consumer B
Consumer C
```

Assignment:

```text
P0 → Consumer A
P1 → Consumer B
P2 → Consumer C
```

---

## Scaling Example

### 3 Partitions, 2 Consumers

```text
P0 → Consumer A
P1 → Consumer B
P2 → Consumer A
```

Consumer A handles two partitions.

---

### 3 Partitions, 5 Consumers

```text
P0 → Consumer A
P1 → Consumer B
P2 → Consumer C

Consumer D → Idle
Consumer E → Idle
```

Extra consumers remain idle.

---

# 3. Consumer Group Coordination

Kafka elects one consumer as:

```text
Group Leader
```

Responsibilities:

- Receives partition metadata
- Calculates assignments
- Distributes assignments

Other consumers:

```text
Followers
```

---

## Group Coordinator

Kafka broker manages:

- Membership
- Heartbeats
- Rebalancing
- Offset tracking

```text
Broker
   |
Group Coordinator
   |
Consumer Group
```

---

# 4. Consumer Rebalancing

## What is Rebalancing?

A rebalance occurs whenever partition ownership changes.

---

## Common Triggers

### New Consumer Joins

Before:

```text
Consumer A → P0,P1,P2
```

After Consumer B joins:

```text
Consumer A → P0,P1
Consumer B → P2
```

---

### Consumer Leaves

Before:

```text
Consumer A → P0
Consumer B → P1
Consumer C → P2
```

Consumer C crashes.

After:

```text
Consumer A → P0,P2
Consumer B → P1
```

---

### Partitions Increase

Before:

```text
3 partitions
```

After:

```text
6 partitions
```

Kafka redistributes ownership.

---

## Rebalance Impact

During rebalance:

```text
Processing pauses
```

Consumers temporarily stop consuming.

Frequent rebalancing can impact throughput.

---

# 5. Heartbeats

Consumers periodically send heartbeats.

```text
Consumer
    |
Heartbeat
    |
Broker
```

Purpose:

- Verify consumer is alive
- Detect failures
- Trigger rebalancing if needed

---

## Heartbeat Failure

```text
Consumer crashes
```

No heartbeat received.

Broker:

```text
Consumer removed
```

Rebalance starts automatically.

---

# 6. Partition Assignment Strategies

Kafka supports multiple assignment strategies.

---

## Range Assignment

Partitions assigned in ranges.

Example:

```text
Partitions:
0 1 2 3 4 5

Consumers:
A B
```

Assignment:

```text
A → 0,1,2
B → 3,4,5
```

---

## Round Robin

Even distribution.

```text
A → 0,2,4
B → 1,3,5
```

More balanced.

---

## Sticky Assignment

Goal:

```text
Minimize movement
```

Useful during rebalancing.

Benefits:

- Less disruption
- Better cache locality
- Faster recovery

---

# 7. Understanding Offsets

Every message has a unique offset.

Partition:

```text
P0

Offset:
0
1
2
3
4
5
...
```

Offset acts like a bookmark.

---

## Example

```text
Offset 0 → Payment A
Offset 1 → Payment B
Offset 2 → Payment C
```

Consumer processed up to:

```text
Offset 2
```

Next read:

```text
Offset 3
```

---

# 8. Offset Management

This is one of the most important Kafka concepts.

Offsets determine:

- Recovery
- Reliability
- Duplicate handling

---

## Auto Commit

Kafka automatically commits offsets.

Configuration:

```java
enable.auto.commit=true
```

Default interval:

```java
auto.commit.interval.ms=5000
```

---

### Problem Scenario

```text
Message consumed
Offset committed
Application crashes
Business logic not completed
```

Result:

```text
Message lost
```

---

# 9. Manual Offset Commit

Recommended for critical systems.

```java
consumer.commitSync();
```

Commit only after successful processing.

---

## Workflow

```text
Read Message
      |
Process
      |
Success?
      |
     Yes
      |
Commit Offset
```

---

## Example

```java
for(ConsumerRecord<String,String> record : records)
{
    process(record);

    consumer.commitSync();
}
```

---

# 10. Commit Strategies

## Commit Per Record

```text
Highest safety
Lowest throughput
```

---

## Commit Per Batch

```text
Read 100 records
Process 100 records
Commit once
```

Benefits:

- Better performance
- Lower broker load

---

## Commit Asynchronously

```java
consumer.commitAsync();
```

Advantages:

- Faster
- Non-blocking

Disadvantages:

- More complex error handling

---

# 11. At-Least-Once Processing

Most common Kafka pattern.

Workflow:

```text
Read
Process
Commit
```

Crash before commit:

```text
Message reprocessed
```

Possible duplicates.

---

## Benefit

```text
No message loss
```

---

# 12. At-Most-Once Processing

Workflow:

```text
Commit
Read
Process
```

Crash during processing:

```text
Message lost
```

No duplicates.

---

# 13. Exactly-Once Processing

Goal:

```text
No duplicates
No loss
```

Achieved using:

- Idempotent Producers
- Kafka Transactions
- Transaction-aware Consumers

Covered in Day 4.

---

# 14. Consumer Lag

## What is Lag?

Lag =

```text
Latest Offset
       -
Current Consumer Offset
```

Example:

```text
Latest Offset = 10000

Consumer Offset = 9500

Lag = 500
```

---

## Why Lag Matters

Large lag means:

```text
Consumers are falling behind
```

Possible causes:

- Slow processing
- Too few consumers
- Too few partitions
- External service delays

---

# 15. Monitoring Consumer Lag

Useful tools:

- Kafka UI
- AKHQ
- Burrow
- Prometheus
- Grafana

Monitor:

- Consumer lag
- Throughput
- Rebalances
- Poll latency

---

# Real-World Example: Fraud Detection System

Topic:

```text
payment-transactions
```

Partitions:

```text
6
```

Consumer Group:

```text
fraud-detector-group
```

Consumers:

```text
fraud-service-1
fraud-service-2
fraud-service-3
```

Assignment:

```text
Consumer 1 → P0,P1
Consumer 2 → P2,P3
Consumer 3 → P4,P5
```

Benefits:

- Horizontal scalability
- High throughput
- Fault tolerance
- Automatic recovery

---

# Best Practices

1. Use consumer groups for scaling.

2. Prefer manual offset commits for critical workloads.

3. Keep processing fast to avoid lag.

4. Monitor consumer lag continuously.

5. Avoid excessive rebalancing.

6. Use sticky assignment in production.

7. Increase partitions before increasing consumers.

8. Handle duplicate events gracefully.

9. Ensure consumers are idempotent.

10. Use batch processing when possible.

---

# Module Summary

In this module you learned:

- Consumer architecture
- Consumer Groups
- Group coordination
- Rebalancing
- Heartbeats
- Partition assignment strategies
- Offset management
- Commit strategies
- Delivery guarantees
- Consumer lag monitoring

These concepts form the foundation for building reliable, scalable event-processing applications and prepare us for the next module: **Serialization & Schema Evolution (JSON, Avro, Protobuf, and Schema Registry)**.
