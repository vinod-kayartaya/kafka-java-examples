# Kafka Monitoring Stack User Guide

This document explains the Docker Compose setup for a monitored Apache Kafka cluster using:

- Apache Kafka
- Zookeeper
- Kafka UI
- Prometheus
- Kafka Exporter
- JMX Exporter

The setup provides complete observability into Kafka brokers, topics, partitions, replication, throughput, consumer lag, JVM metrics, and broker health.

## Folder structure

```text
lab5
├── Readme.md
├── dashboard.json
├── docker-compose.yaml
├── jmx-exporter
│   ├── jmx_prometheus_javaagent-0.20.0.jar
│   └── kafka-2_0_0.yaml
├── monitoring
│   └── jmx
│       ├── jmx_prometheus_javaagent-0.20.0.jar
│       └── kafka-jmx.yaml
└── prometheus
    └── prometheus.yaml
```

---

# Solution Overview

This environment creates:

| Component      | Purpose                              |
| -------------- | ------------------------------------ |
| Zookeeper      | Kafka cluster coordination           |
| Kafka Brokers  | Distributed messaging platform       |
| Kafka UI       | Web UI for Kafka administration      |
| Kafka Exporter | Exposes Kafka metrics for Prometheus |
| JMX Exporter   | Exposes JVM and Kafka broker metrics |
| Prometheus     | Metrics collection and storage       |

---

# High-Level Architecture

```text
                        +----------------------+
                        |      Kafka UI        |
                        |      Port 8080       |
                        +-----------+----------+
                                    |

 ----------------------------------------------------------------------
 |                         Kafka Cluster                               |
 ----------------------------------------------------------------------
 |                |                     |                              |
+--------+    +--------+           +--------+                    +------------+
|Kafka 1 |    |Kafka 2 |           |Kafka 3 |                    | Zookeeper |
|9092    |    |9093    |           |9094    |                    |2181       |
|7071    |    |7072    |           |7073    |                    +------------+
+--------+    +--------+           +--------+
     |              |                    |
     |              |                    |
     -------------------------------------------------
                             |
                    +----------------+
                    | Kafka Exporter |
                    |     9308       |
                    +--------+-------+
                             |
                    +----------------+
                    |  Prometheus    |
                    |     9090       |
                    +----------------+
```

---

# Services Explained

# 1. Zookeeper

## Purpose

Zookeeper manages:

- Broker coordination
- Cluster metadata
- Leader election
- Topic metadata
- Broker registration

---

## Configuration

```yaml
ZOOKEEPER_CLIENT_PORT: 2181
ZOOKEEPER_TICK_TIME: 2000
```

### Meaning

| Property              | Description                |
| --------------------- | -------------------------- |
| ZOOKEEPER_CLIENT_PORT | Port used by Kafka brokers |
| ZOOKEEPER_TICK_TIME   | Internal timing interval   |

---

# 2. Kafka Brokers

The cluster contains:

- kafka1
- kafka2
- kafka3

Each broker has:

- External listener
- Internal listener
- JMX monitoring
- Replication support

---

# Kafka Listener Architecture

## Internal Listener

Example:

```yaml
INTERNAL://0.0.0.0:29092
```

Used for:

- Broker-to-broker communication
- Internal Docker networking

---

## External Listener

Example:

```yaml
EXTERNAL://0.0.0.0:9092
```

Used for:

- External Kafka clients
- Producers
- Consumers
- Applications outside Docker

---

# Advertised Listeners

Example:

- Assuming that your current IP address is 192.168.1.85
- Replace this with your actual IP address

```yaml
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka1:29092,
  EXTERNAL://192.168.1.85:9092
```

This is one of the most important Kafka configurations.

---

## Why Advertised Listeners Matter

Kafka clients first connect to a bootstrap server.

Kafka then returns:

- Actual broker addresses
- Partition leaders
- Cluster metadata

If the advertised listener is wrong:

- Clients cannot connect
- Topic listing fails
- Producers fail
- Consumers disconnect

---

## Internal vs External Networking

| Listener Type | Used By                      |
| ------------- | ---------------------------- |
| INTERNAL      | Kafka brokers inside Docker  |
| EXTERNAL      | External systems and laptops |

---

# Replication Settings

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
```

---

## Explanation

### OFFSETS_TOPIC_REPLICATION_FACTOR

Replicates consumer offsets across all brokers.

---

### TRANSACTION_STATE_LOG_REPLICATION_FACTOR

Replicates transactional metadata.

---

### MIN_ISR

Minimum number of replicas that must acknowledge writes.

This improves reliability.

---

# Auto Topic Creation

```yaml
KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
```

Kafka automatically creates topics when producers publish to non-existing topics.

Useful for:

- Development
- Learning
- Testing

Not recommended for production.

---

# JMX Monitoring Setup

Each broker uses:

```yaml
KAFKA_OPTS: -javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent-0.20.0.jar
```

This enables:

- JVM metrics
- Kafka internal metrics
- Broker metrics
- Request metrics
- Replication metrics

---

# JMX Exporter Ports

| Broker | JMX Port |
| ------ | -------- |
| kafka1 | 7071     |
| kafka2 | 7072     |
| kafka3 | 7073     |

---

# JMX Exporter Configuration

The file:

```text
kafka-jmx.yaml
```

contains metric extraction rules.

---

# Metrics Collected

# Kafka Server Metrics

Examples:

- Messages in/sec
- Bytes in/sec
- Bytes out/sec
- Request counts

---

# Controller Metrics

Examples:

- Active controller count
- Preferred replica imbalance
- Offline partitions

---

# Network Metrics

Examples:

- Request rates
- Request latency
- Network throughput

---

# Replica Metrics

Examples:

- Under replicated partitions
- ISR shrink/expand events
- Replica lag

---

# JVM Metrics

Examples:

- Heap memory
- Non-heap memory
- Thread count
- Garbage collection

---

# Kafka UI

Kafka UI provides:

- Topic browsing
- Message viewing
- Consumer group monitoring
- Partition inspection
- Broker monitoring

---

# Access Kafka UI

## URL

```text
http://localhost:8080
```

Or:

```text
http://192.168.1.85:8080
```

---

# Kafka Exporter

Kafka Exporter exposes Kafka cluster metrics for Prometheus.

---

# Kafka Exporter Port

```text
9308
```

---

# Metrics Exposed by Kafka Exporter

Examples:

- Consumer lag
- Topic partitions
- Broker count
- Under replicated partitions
- Consumer group metrics
- Topic offsets

---

# Prometheus

Prometheus periodically scrapes metrics.

---

# Prometheus Configuration

File:

```text
prometheus.yaml
```

---

# Scrape Interval

```yaml
global:
  scrape_interval: 5s
```

Prometheus collects metrics every 5 seconds.

---

# Kafka Exporter Scrape Job

```yaml
job_name: 'kafka-exporter'
```

Prometheus scrapes:

```text
kafka-exporter:9308
```

---

# Kafka JMX Scrape Job

```yaml
job_name: 'kafka-jmx'
```

Prometheus scrapes:

- kafka1:7071
- kafka2:7072
- kafka3:7073

---

# Starting the Environment

## Start All Services

```bash
docker compose up -d
```

---

# Verify Containers

```bash
docker ps
```

Expected containers:

- zookeeper
- kafka1
- kafka2
- kafka3
- kafka-ui
- kafka-exporter
- prometheus

---

# Accessing Prometheus

## URL

```text
http://localhost:9090
```

Or:

```text
http://192.168.1.85:9090
```

---

# Verify Targets in Prometheus

Open:

```text
http://localhost:9090/targets
```

Expected targets:

| Target              | Status |
| ------------------- | ------ |
| kafka-exporter:9308 | UP     |
| kafka1:7071         | UP     |
| kafka2:7072         | UP     |
| kafka3:7073         | UP     |

---

# Useful Prometheus Queries

# Broker Count

```promql
kafka_brokers
```

---

# Messages Per Second

```promql
rate(kafka_server_brokertopicmetrics_messagesin_total[1m])
```

---

# Bytes In Per Second

```promql
rate(kafka_server_brokertopicmetrics_bytesin_total[1m])
```

---

# Bytes Out Per Second

```promql
rate(kafka_server_brokertopicmetrics_bytesout_total[1m])
```

---

# Under Replicated Partitions

```promql
kafka_server_replicamanager_underreplicatedpartitions
```

---

# JVM Heap Usage

```promql
jvm_memory_heap_used_bytes
```

---

# GC Collection Count

```promql
jvm_gc_collection_count
```

---

# Kafka Topic Operations

# Create Topic

```bash
docker exec kafka1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 3
```

---

# List Topics

```bash
docker exec kafka1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```

---

# Describe Topic

```bash
docker exec kafka1 kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders
```

---

# Produce Messages

```bash
docker exec -it kafka1 kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic orders
```

---

# Consume Messages

```bash
docker exec -it kafka2 kafka-console-consumer \
  --bootstrap-server localhost:9093 \
  --topic orders \
  --from-beginning
```

---

# Consumer Group Monitoring

## List Groups

```bash
docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list
```

---

## Describe Group

```bash
docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-processors
```

---

# Monitoring Scenarios

# Scenario 1: Monitor Consumer Lag

Observe:

- Lag increase
- Slow consumers
- Backpressure

Metrics:

```promql
kafka_consumergroup_lag
```

---

# Scenario 2: Monitor Broker Health

Observe:

- Offline brokers
- Under replicated partitions
- ISR issues

---

# Scenario 3: JVM Pressure

Observe:

- Heap growth
- GC frequency
- Thread count

---

# Scenario 4: High Throughput

Observe:

- Bytes in/out
- Request rate
- Network utilization

---

# Troubleshooting

# Problem: No Metrics in Prometheus

## Verify Targets

Open:

```text
http://localhost:9090/targets
```

Targets must be UP.

---

# Problem: Kafka UI Shows No Brokers

Verify:

```yaml
KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS
```

matches internal broker addresses.

---

# Problem: External Clients Cannot Connect

Verify:

```yaml
KAFKA_ADVERTISED_LISTENERS
```

contains the correct host IP.

---

# Problem: JMX Metrics Missing

Verify:

- JMX exporter JAR exists
- kafka-jmx.yaml exists
- Port mappings are correct
- KAFKA_OPTS is configured properly

---

# Problem: Consumer Lag Keeps Growing

Possible causes:

- Slow consumers
- Too few consumers
- Large messages
- Broker bottlenecks

---

# Learning Exercises

# Exercise 1

Generate heavy producer traffic.

Observe:

- Message rate
- Network traffic
- CPU usage

---

# Exercise 2

Kill one broker.

Observe:

- Leader election
- Replica reassignment
- Under replicated partitions

---

# Exercise 3

Start multiple consumers in same group.

Observe:

- Rebalancing
- Partition distribution
- Consumer lag

---

# Exercise 4

Monitor JVM memory.

Observe:

- Heap usage growth
- GC behavior
- Thread count

---

# Important Production Notes

This setup is excellent for:

- Learning Kafka
- Monitoring practice
- Kafka internals exploration
- Prometheus integration
- Grafana dashboards
- Consumer lag analysis
- Distributed systems education

However, for production systems, additional improvements are recommended:

- Persistent storage volumes
- TLS/SSL security
- SASL authentication
- Dedicated monitoring storage
- Grafana dashboards
- Alertmanager integration
- Log aggregation
- Resource limits
- Backup strategies

---

# Final Outcome

By using this environment, you can learn:

- Kafka cluster architecture
- Topic replication
- Partition leadership
- Consumer group balancing
- Kafka observability
- Prometheus monitoring
- JVM monitoring
- Kafka performance analysis
- Distributed system troubleshooting
- Fault tolerance and recovery
