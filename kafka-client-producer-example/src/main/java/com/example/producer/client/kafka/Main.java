package com.example.producer.client.kafka;

import com.example.producer.client.kafka.producer.AsyncSendProducer;
import com.example.producer.client.kafka.producer.ProducerWithCustomPartition;
import com.example.producer.client.kafka.producer.ProducerWithDefaultPartition;
import com.example.producer.client.kafka.producer.ProducerWithKeyWithDefaultPartition;
import com.example.producer.client.kafka.producer.SyncSendProducer;

public class Main {
    public static void main(String[] args) {
        // sync send
//        SyncSendProducer syncSendProducer = new SyncSendProducer();
//        syncSendProducer.syncSend();

        // async send
//        AsyncSendProducer asyncSendProducer = new AsyncSendProducer();
//        asyncSendProducer.asyncSend();

        // sync with no key with default partition
//        ProducerWithDefaultPartition producerWithDefaultPartition = new ProducerWithDefaultPartition();
//        producerWithDefaultPartition.syncSend();

        // sync with key with default partition
//        ProducerWithKeyWithDefaultPartition producerWithKeyWithDefaultPartition = new ProducerWithKeyWithDefaultPartition();
//        producerWithKeyWithDefaultPartition.syncSend();

        // sync with custom partition
//        ProducerWithCustomPartition producerWithCustomPartition = new ProducerWithCustomPartition();
//        producerWithCustomPartition.syncSend();
    }
}

