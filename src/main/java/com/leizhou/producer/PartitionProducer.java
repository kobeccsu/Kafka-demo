package com.leizhou.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class PartitionProducer {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.leizhou.partitioner.MyPartitioner");

        KafkaProducer<String, String> stringStringKafkaProducer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            stringStringKafkaProducer.send(new ProducerRecord<>("first", "hello Lei, " + i), (recordMetadata, e) -> {

                if (e == null) {
                    System.out.println(recordMetadata.offset() + " -- " + recordMetadata.partition());
                }
            });
        }
        stringStringKafkaProducer.close();
    }
}
