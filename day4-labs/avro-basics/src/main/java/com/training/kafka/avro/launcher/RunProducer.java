package com.training.kafka.avro.launcher;

import com.training.kafka.avro.producer.TransactionProducer;

public class RunProducer {
    public static void main(String[] args) {
        new TransactionProducer()
                .sendTransaction();
    }
}