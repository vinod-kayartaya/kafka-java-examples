
package com.training.kafka.avro.config;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class KafkaConsumerConfig {
    public static Properties create() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092");
        p.put("group.id", "avro-demo-group");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        p.put("auto.offset.reset", "earliest");
        return p;
    }
}
