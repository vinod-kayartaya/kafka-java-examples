package com.training.kafka.avro.consumer;

import com.training.kafka.avro.config.KafkaConsumerConfig;
import com.training.kafka.avro.model.Transaction;
import com.training.kafka.avro.util.KafkaUtil;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;

public class TransactionConsumer {
    public void consumeTransactions() {
        try (
                KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(KafkaConsumerConfig.create())) {
            consumer.subscribe(
                    Collections.singletonList(
                            KafkaUtil.TOPIC));

            System.out.println("Consumer started...");

            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(
                        Duration.ofSeconds(1));

                for (ConsumerRecord<String, byte[]> record : records) {
                    Transaction transaction = deserialize(record.value());

                    System.out.println("----------------------------------");
                    System.out.println("Offset      : " + record.offset());
                    System.out.println("Partition   : " + record.partition());
                    System.out.println("Key         : " + record.key());

                    System.out.println("Transaction Id : "
                            + transaction.getTransactionId());

                    System.out.println("Customer Id    : "
                            + transaction.getCustomerId());

                    System.out.println("Amount         : "
                            + transaction.getAmount());
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private Transaction deserialize(byte[] payload)
            throws Exception {
        SpecificDatumReader<Transaction> reader = new SpecificDatumReader<>(
                Transaction.class);

        BinaryDecoder decoder = DecoderFactory.get()
                .binaryDecoder(payload, null);

        return reader.read(null, decoder);
    }
}