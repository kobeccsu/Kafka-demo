package com.leizhou.producer;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CallbackProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>("first",0, "ddd", "Morning lei, " + i), (recordMetadata, e) -> {

                if (e == null) {
                    System.out.println(recordMetadata.offset() + " -- " + recordMetadata.partition());
                }
            });
        }

        producer.close();
    }
}
