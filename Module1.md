# Day 3 – Module 1: Kafka Producer Internals

## Module Overview

In this module, participants will learn how Kafka producers work internally, how messages are sent to Kafka brokers, how partition selection occurs, and how Kafka guarantees reliable message delivery through idempotent producers.

This module forms the foundation for all Kafka applications because every event-driven system starts with producing events. Understanding producer internals helps developers build systems that are scalable, reliable, and performant.

---

# Learning Objectives

By the end of this module, participants will be able to:

- Understand Kafka Producer Architecture
- Explain the message lifecycle inside a producer
- Compare synchronous and asynchronous message publishing
- Implement key-based partitioning
- Understand batching and compression
- Configure producer acknowledgements
- Explain idempotent producers
- Build reliable event publishing applications

---

# Business Scenario

Throughout Days 3–5, we will continue building the:

**Payment Processing & Fraud Detection System**

In this module:

- Payment Service publishes transaction events
- Kafka Producer sends events to Kafka
- Fraud Detection Service consumes events later
- Events must never be lost
- Duplicate processing must be avoided

Example transaction event:

```json
{
  "transactionId": "TXN1001",
  "customerId": "CUST500",
  "amount": 25000,
  "merchant": "Amazon"
}
```

---

# 1. Kafka Producer Architecture

A producer is responsible for sending records to Kafka topics.

## High-Level Architecture

```text
Application
     |
     v
Kafka Producer API
     |
     v
Serializer
     |
     v
Partitioner
     |
     v
Producer Buffer
     |
     v
Broker
```

---

## Producer Responsibilities

The producer:

1. Creates records
2. Serializes data
3. Determines partition
4. Buffers records
5. Batches messages
6. Sends data to brokers
7. Retries failed requests
8. Tracks acknowledgements

---

# ProducerRecord

Every message is wrapped inside a ProducerRecord.

```java
ProducerRecord<String, String> record =
    new ProducerRecord<>(
        "transactions",
        "CUST500",
        "Payment Successful");
```

Parameters:

```java
(topic, key, value)
```

Example:

```java
ProducerRecord<String, String> record =
    new ProducerRecord<>(
        "transactions",
        "TXN1001",
        jsonPayload);
```

---

# 2. Kafka Producer Message Lifecycle

Let's follow one message from application to broker.

## Step 1: Application Creates Event

```java
Transaction txn =
    new Transaction(
        "TXN1001",
        25000);
```

---

## Step 2: Serialization

Kafka transmits bytes.

Objects must be converted into byte arrays.

Example:

```java
StringSerializer
JsonSerializer
AvroSerializer
```

Result:

```text
Java Object
      |
      v
Byte Array
```

---

## Step 3: Partition Selection

Kafka determines which partition receives the message.

Based on:

- Message Key
- Custom Partitioner
- Round Robin

Example:

```java
key = "CUST500"
```

Kafka computes:

```text
hash(key) % partitions
```

---

## Step 4: Buffering

Producer stores records in memory.

```text
Application
      |
      v
Producer Buffer
```

The producer does not immediately send every message.

Instead:

- Stores records
- Creates batches
- Sends efficiently

---

## Step 5: Broker Acknowledgement

Broker receives message.

Depending on producer settings:

```properties
acks=0
acks=1
acks=all
```

Broker returns acknowledgement.

---

# 3. Producer Batching

Batching is one of Kafka's biggest performance advantages.

Instead of:

```text
Send 1 message
Send 1 message
Send 1 message
```

Kafka does:

```text
Collect Messages
       |
       v
Single Network Request
```

---

## Why Batching?

Network calls are expensive.

Batching:

- Reduces latency
- Increases throughput
- Reduces broker load

---

## Important Configuration

### batch.size

Maximum batch size.

```properties
batch.size=16384
```

16 KB batch.

---

### linger.ms

How long Kafka waits before sending.

```properties
linger.ms=10
```

Meaning:

```text
Wait up to 10ms
Collect more messages
Send batch
```

---

## Real-World Example

Payment service sends:

```text
TXN1001
TXN1002
TXN1003
TXN1004
```

Instead of 4 requests:

```text
1 Batch
4 Messages
1 Network Call
```

Huge performance improvement.

---

# 4. Synchronous vs Asynchronous Send

One of the most important producer concepts.

---

# Synchronous Send

Application waits for acknowledgement.

```java
producer.send(record).get();
```

Flow:

```text
Send Message
     |
Wait
     |
Receive Ack
     |
Continue
```

---

## Advantages

- Simpler
- Immediate failure detection

---

## Disadvantages

- Slower
- Blocks application thread

---

## Example

```java
try {
    producer.send(record).get();
    System.out.println("Sent");
}
catch(Exception e) {
    e.printStackTrace();
}
```

---

# Asynchronous Send

Application does not wait.

```java
producer.send(record);
```

Flow:

```text
Send
 |
Continue Processing
 |
Broker Ack Later
```

---

## Callback-Based Async Send

```java
producer.send(
    record,
    (metadata, exception) -> {

        if(exception == null) {
            System.out.println(
                metadata.partition());
        }
    });
```

---

## Advantages

- High throughput
- Non-blocking
- Preferred in production

---

## Disadvantages

- More complex error handling

---

# Comparison

| Feature        | Sync | Async  |
| -------------- | ---- | ------ |
| Speed          | Slow | Fast   |
| Throughput     | Low  | High   |
| Blocking       | Yes  | No     |
| Production Use | Rare | Common |

---

# 5. Key-Based Partitioning

Partitioning is critical for scalability.

---

## Why Partitions Exist

Partitions allow:

- Parallel processing
- Scalability
- Ordering guarantees

Example:

```text
transactions
 ├─ P0
 ├─ P1
 └─ P2
```

---

# Without Key

Messages distributed round-robin.

```text
M1 -> P0
M2 -> P1
M3 -> P2
M4 -> P0
```

Load is balanced.

---

# With Key

```java
key="CUST500"
```

Kafka calculates:

```text
hash(key) % partitionCount
```

Example:

```text
hash(CUST500) % 3
```

Result:

```text
Partition 1
```

Every future event for CUST500 goes to the same partition.

---

# Why This Matters

Customer transaction history remains ordered.

```text
Transaction Created
Payment Authorized
Payment Settled
Refund Issued
```

All events remain in sequence.

---

# Ordering Guarantee

Kafka guarantees ordering only within a partition.

```text
Partition 1

Offset 0
Offset 1
Offset 2
Offset 3
```

Consumers receive events in the same order.

---

# 6. Producer Reliability

Kafka producers provide multiple reliability options.

---

# Acknowledgement Levels

## acks=0

```properties
acks=0
```

Producer does not wait.

```text
Send
 |
Forget
```

Fastest but risky.

---

## acks=1

```properties
acks=1
```

Leader acknowledges.

```text
Producer
   |
Leader
```

Balanced option.

---

## acks=all

```properties
acks=all
```

All in-sync replicas acknowledge.

```text
Producer
    |
Leader
    |
Replicas
```

Highest durability.

---

# Retry Mechanism

Producer retries transient failures.

```properties
retries=5
```

Example:

```text
Network Issue
      |
Retry
      |
Success
```

---

# 7. Idempotent Producers

One of Kafka's most important reliability features.

---

## The Duplicate Message Problem

Scenario:

```text
Producer Sends
      |
Broker Stores Message
      |
Ack Lost
      |
Producer Retries
```

Result:

```text
Duplicate Event
```

---

# Without Idempotence

```text
TXN1001
TXN1001
```

Duplicate records appear.

---

# Enable Idempotence

```properties
enable.idempotence=true
```

Kafka assigns:

```text
Producer ID
Sequence Number
```

Broker detects duplicates.

---

# Result

```text
TXN1001
```

Only one copy stored.

---

# Production Recommendation

Always enable:

```properties
acks=all
enable.idempotence=true
retries=Integer.MAX_VALUE
```

These settings form the basis of reliable event publishing.

---

# Producer Best Practices

## Use Keys Carefully

Good:

```java
customerId
accountId
orderId
```

Bad:

```java
randomUUID()
```

Random keys destroy ordering.

---

## Enable Compression

```properties
compression.type=snappy
```

Benefits:

- Less network traffic
- Higher throughput

---

## Tune Batching

```properties
batch.size=32768
linger.ms=5
```

Improves performance.

---

## Prefer Async Send

```java
producer.send(record, callback);
```

Provides maximum throughput.

---

## Enable Idempotence

```properties
enable.idempotence=true
```

Protects against duplicates.

---

# Hands-On Exercise

Build a Payment Event Producer.

### Requirements

Create topic:

```bash
kafka-topics.sh \
--create \
--topic transactions \
--partitions 3 \
--replication-factor 1
```

### Tasks

1. Create Kafka Producer
2. Send 100 transaction events
3. Use customerId as key
4. Observe partition assignment
5. Compare sync vs async send
6. Enable batching
7. Enable idempotence
8. Measure throughput

---

# Module Summary

In this module you learned:

- Kafka Producer Architecture
- Producer Message Lifecycle
- Serialization
- Batching and Buffering
- Synchronous vs Asynchronous Publishing
- Key-Based Partitioning
- Producer Reliability
- Acknowledgements
- Retry Mechanisms
- Idempotent Producers

These concepts provide the foundation required to build high-throughput, fault-tolerant Kafka applications and prepare you for the Day 3 labs involving transaction event production and fraud detection pipelines.
