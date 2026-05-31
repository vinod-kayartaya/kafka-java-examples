package com.training.kafka.avro.launcher;

import com.training.kafka.avro.consumer.TransactionConsumer;

public class RunConsumer {
    public static void main(String[] args) {
        new TransactionConsumer()
                .consumeTransactions();
    }
}