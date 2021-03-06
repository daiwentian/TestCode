package com.example.producer.client.kafka.shutdown;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

public class IncrementalKeyProducer {
    public static void main(String[] args) {
        // configuration
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.Integer().serializer().getClass());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties);

        IntStream.range(0, 100)
                .forEach(i -> {
                    String value = RandomStringUtils.randomAlphabetic(5);
                    ProducerRecord<Integer, String> record = new ProducerRecord<>("s1", i,  value);
                    try {
                        // sync send
                        Future<RecordMetadata> send = producer.send(record);
                        RecordMetadata recordMetadata = send.get();
                        System.out.println("partition: " + recordMetadata.partition() +
                                ", topic: " + recordMetadata.topic() +
                                ", offset: " + recordMetadata.offset() +
                                ", key: " + i +
                                ", value: " + value);
                        Thread.sleep(1_000L);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
    }
}

