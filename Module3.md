# Day 3 – Module 3: Serialization & Schema Evolution

This module is part of **Day 3 – Kafka Programming + Stream Processing** and focuses on how data is represented, exchanged, versioned, and evolved in Kafka-based systems.

---

# Learning Objectives

By the end of this module, you will be able to:

- Understand why serialization is required in distributed systems
- Compare JSON, Avro, and Protobuf formats
- Understand schema-driven messaging
- Configure and use Schema Registry
- Apply schema evolution techniques safely
- Design backward-compatible Kafka event contracts
- Avoid common integration failures caused by schema changes

---

# 1. Why Serialization Matters

Kafka stores and transports data as bytes.

Applications typically work with:

- Java Objects
- C# Objects
- Python Dictionaries
- JSON Documents

Before data can be sent to Kafka:

```java
Transaction tx = new Transaction(...);
```

It must be converted into bytes.

This process is called:

## Serialization

Object → Byte Array

When consuming:

Byte Array → Object

This is called:

## Deserialization

---

# Example

Producer:

```java
Transaction tx = new Transaction(
    "TXN-1001",
    "ACC-123",
    25000.0
);
```

Serialized data sent to Kafka:

```text
0101011000101001010...
```

Consumer reconstructs:

```java
Transaction tx
```

---

# Why Not Send Java Objects Directly?

Kafka is language agnostic.

A consumer may be:

- Java
- Python
- Go
- NodeJS
- .NET

Kafka cannot understand:

```java
Transaction
```

It only understands:

```text
Bytes
```

Therefore serialization becomes mandatory.

---

# Problems Without Standardized Serialization

Imagine:

Version 1 Producer

```json
{
  "transactionId": "TXN1001",
  "amount": 1000
}
```

Consumer expects:

```json
{
  "transactionId": "TXN1001",
  "amount": 1000,
  "currency": "USD"
}
```

Consumer crashes because field is missing.

This is one of the biggest challenges in event-driven systems.

---

# What is a Schema?

A schema is a contract describing the structure of data.

Example:

```json
{
  "transactionId": "string",
  "accountId": "string",
  "amount": "double"
}
```

Schema defines:

- Fields
- Data types
- Required fields
- Optional fields
- Default values

Think of schema as:

> Database table definition for Kafka messages.

---

# Serialization Formats in Kafka

Three most common formats:

1. JSON
2. Avro
3. Protobuf

---

# 2. JSON Serialization

JSON is text-based and human-readable.

Example:

```json
{
  "transactionId": "TXN1001",
  "accountId": "ACC001",
  "amount": 15000.0
}
```

---

## Advantages

Easy to read

Easy to debug

Language independent

Minimal setup

---

## Disadvantages

Large message size

No enforced schema

Weak compatibility management

Runtime errors are common

---

## JSON Producer Example

```java
ObjectMapper mapper = new ObjectMapper();

String json = mapper.writeValueAsString(transaction);

producer.send(
    new ProducerRecord<>(
        "transactions",
        json
    )
);
```

---

## JSON Consumer Example

```java
Transaction tx =
    mapper.readValue(
        record.value(),
        Transaction.class
    );
```

---

# JSON in Payment Systems

Event:

```json
{
  "transactionId": "TXN1001",
  "accountId": "ACC001",
  "amount": 12000,
  "status": "APPROVED"
}
```

Easy to inspect from CLI tools.

However schema evolution becomes difficult as systems grow.

---

# 3. Apache Avro

Avro is Kafka's most commonly used serialization format.

Designed specifically for distributed systems.

---

# Key Characteristics

Binary format

Compact

Fast

Strong schema support

Supports evolution

Works naturally with Schema Registry

---

# Avro Schema Example

```json
{
  "type": "record",
  "name": "Transaction",
  "namespace": "com.company.model",
  "fields": [
    {
      "name": "transactionId",
      "type": "string"
    },
    {
      "name": "accountId",
      "type": "string"
    },
    {
      "name": "amount",
      "type": "double"
    }
  ]
}
```

File:

```text
transaction.avsc
```

---

# Generated Java Class

Maven plugin generates:

```java
Transaction.java
```

from

```text
transaction.avsc
```

Developers use generated classes.

---

# Avro Serialization Flow

```text
Producer
    |
    v
Avro Serializer
    |
    v
Schema Registry
    |
    v
Kafka Topic
    |
    v
Consumer
    |
    v
Avro Deserializer
```

---

# Benefits of Avro

Smaller messages

Faster transmission

Automatic compatibility checks

Strong typing

Enterprise standard

---

# Example Transaction Event

Schema:

```json
{
  "name": "currency",
  "type": "string",
  "default": "USD"
}
```

Older consumers continue working.

This is schema evolution.

---

# Avro Message Size Comparison

JSON:

```json
{
  "transactionId": "TXN1001",
  "amount": 1000
}
```

Approx:

```text
50+ bytes
```

Avro:

```text
~15 bytes
```

Significantly smaller.

---

# 4. Protocol Buffers (Protobuf)

Created by Google.

Extremely efficient binary serialization format.

Widely used in:

- Microservices
- gRPC
- Cloud platforms

---

# Protobuf Schema Example

```proto
syntax = "proto3";

message Transaction {

  string transactionId = 1;

  string accountId = 2;

  double amount = 3;

}
```

---

# Generated Class

Compiler generates:

```java
Transaction.java
```

from

```proto
transaction.proto
```

---

# Advantages

Very compact

High performance

Cross-language support

Strong typing

Excellent for APIs

---

# Disadvantages

Harder to read

Schema evolution more complex than Avro

Slightly higher learning curve

---

# JSON vs Avro vs Protobuf

| Feature           | JSON   | Avro      | Protobuf   |
| ----------------- | ------ | --------- | ---------- |
| Human Readable    | Yes    | No        | No         |
| Binary Format     | No     | Yes       | Yes        |
| Schema Support    | Weak   | Strong    | Strong     |
| Message Size      | Large  | Small     | Very Small |
| Kafka Integration | Good   | Excellent | Excellent  |
| Schema Evolution  | Manual | Excellent | Good       |
| Performance       | Medium | High      | Very High  |

---

# Which Format Should We Use?

## Development/Demos

JSON

Reason:

Simple debugging

---

## Enterprise Kafka Platforms

Avro

Reason:

Schema Registry integration

Compatibility enforcement

---

## High-Performance Systems

Protobuf

Reason:

Smallest payloads

Fastest serialization

---

# 5. Schema Registry Concepts

As systems grow:

```text
Producer A
Producer B
Producer C

Consumer X
Consumer Y
Consumer Z
```

Everyone must agree on message structure.

Schema Registry becomes the central authority.

---

# What is Schema Registry?

A centralized repository storing schemas.

It manages:

- Versions
- Compatibility
- Validation
- Distribution

---

# Architecture

```text
Producer
   |
   v
Schema Registry
   |
   v
Kafka Topic
   |
   v
Consumer
```

---

# Benefits

Single source of truth

Version management

Compatibility checks

Prevents breaking changes

Improves governance

---

# Example

Register schema:

Version 1

```json
{
  "transactionId": "string",
  "amount": "double"
}
```

Later:

Version 2

```json
{
  "transactionId": "string",
  "amount": "double",
  "currency": "string"
}
```

Registry verifies compatibility.

---

# Schema Subject

A subject is a logical schema name.

Example:

```text
transactions-value
```

Versions:

```text
Version 1
Version 2
Version 3
```

All stored under same subject.

---

# Schema Registry Workflow

Producer startup:

```text
Check Schema
      |
      v
Register Schema
      |
      v
Get Schema ID
      |
      v
Publish Event
```

Consumer:

```text
Read Schema ID
      |
      v
Fetch Schema
      |
      v
Deserialize Message
```

---

# 6. Schema Evolution

Business requirements change continuously.

Example:

Initial event:

```json
{
  "transactionId": "TXN1001",
  "amount": 1000
}
```

Later:

```json
{
  "transactionId": "TXN1001",
  "amount": 1000,
  "currency": "USD"
}
```

Need to support both.

---

# What is Schema Evolution?

Ability to modify schemas without breaking existing applications.

---

# Typical Changes

Adding fields

Removing fields

Renaming fields

Changing data types

Adding defaults

---

# Safe Change Example

Version 1

```json
{
  "transactionId": "string",
  "amount": "double"
}
```

Version 2

```json
{
  "transactionId": "string",
  "amount": "double",
  "currency": "string"
}
```

Added:

```json
"default":"USD"
```

Consumers remain compatible.

---

# Dangerous Change Example

Version 1

```json
{
  "amount": "double"
}
```

Version 2

```json
{
  "amount": "string"
}
```

Type changed.

Many consumers may fail.

---

# Compatibility Strategies

Schema Registry supports multiple compatibility modes.

---

## Backward Compatibility

New consumers can read old data.

Most common mode.

```text
Consumer V2
   |
Can Read
   |
Data V1
```

---

## Forward Compatibility

Old consumers can read new data.

```text
Consumer V1
   |
Can Read
   |
Data V2
```

---

## Full Compatibility

Both directions supported.

```text
V1 <--> V2
```

Safest option.

---

# Compatibility Matrix

| Mode     | Old Consumer Reads New Data | New Consumer Reads Old Data |
| -------- | --------------------------- | --------------------------- |
| Backward | No                          | Yes                         |
| Forward  | Yes                         | No                          |
| Full     | Yes                         | Yes                         |

---

# Real Banking Example

Version 1:

```json
{
  "transactionId": "TXN1001",
  "amount": 1000
}
```

Version 2:

```json
{
  "transactionId": "TXN1001",
  "amount": 1000,
  "currency": "USD"
}
```

Added:

```json
"default":"USD"
```

Compatibility passes.

No outages occur.

---

# Best Practices

### Use Schema Registry

Never manage schemas manually.

### Prefer Avro

Best Kafka ecosystem support.

### Add Defaults

Makes evolution safer.

### Avoid Type Changes

Usually break compatibility.

### Version Everything

Treat schemas like source code.

### Test Compatibility

Before deployment.

### Maintain Event Contracts

Events are APIs.

Breaking events can break dozens of downstream systems.

---

# Summary

In this module you learned:

- Why serialization is required in Kafka
- JSON serialization concepts
- Avro serialization and schema-driven messaging
- Protobuf fundamentals
- Schema Registry architecture
- Schema subjects and versioning
- Schema evolution principles
- Backward, forward, and full compatibility strategies
- Enterprise best practices for managing Kafka event contracts

### Key Takeaway

In modern Kafka systems, **Avro + Schema Registry** is the most widely adopted combination because it provides compact messages, strong schema governance, and safe schema evolution across large distributed event-driven architectures.
