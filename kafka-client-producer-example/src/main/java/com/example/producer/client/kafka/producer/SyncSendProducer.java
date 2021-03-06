package com.example.producer.client.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

public class SyncSendProducer {
    public void syncSend() {
        // configuration
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        IntStream.range(0, 100)
                .forEach(i -> {
                    ProducerRecord<String, String> record = new ProducerRecord<>("mytopic", "my_value-" + i);
                    try {
                        // sync send
                        Future<RecordMetadata> send = producer.send(record);
                        RecordMetadata recordMetadata = send.get();
                        System.out.println("=============================");
                        System.out.println(LocalDateTime.now());
                        System.out.println("topic: " + recordMetadata.topic());
                        System.out.println("partition: " + recordMetadata.partition());
                        System.out.println("offset: " + recordMetadata.offset());

                        Thread.sleep(200L);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
    }
}

