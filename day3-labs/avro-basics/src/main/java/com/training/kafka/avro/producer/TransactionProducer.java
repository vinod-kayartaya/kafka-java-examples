package com.training.kafka.avro.producer;

import com.training.kafka.avro.config.KafkaProducerConfig;
import com.training.kafka.avro.model.Transaction;
import com.training.kafka.avro.util.KafkaUtil;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;

public class TransactionProducer {
    public void sendTransaction() {
        try (
                KafkaProducer<String, byte[]> producer = new KafkaProducer<>(KafkaProducerConfig.create())) {
            // Create Avro object
            Transaction transaction = Transaction.newBuilder()
                    .setTransactionId("TXN-1001")
                    .setCustomerId("CUST-101")
                    .setAmount(2500.00)
                    .build();

            // Serialize Avro object into byte[]
            byte[] payload = serialize(transaction);

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                    KafkaUtil.TOPIC,
                    transaction.getTransactionId(),
                    payload);

            producer.send(record);

            producer.flush();

            System.out.println("Transaction sent successfully");
            System.out.println(transaction);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private byte[] serialize(Transaction transaction) throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        SpecificDatumWriter<Transaction> writer = new SpecificDatumWriter<>(Transaction.class);

        BinaryEncoder encoder = EncoderFactory.get()
                .binaryEncoder(outputStream, null);

        writer.write(transaction, encoder);

        encoder.flush();

        return outputStream.toByteArray();
    }
}