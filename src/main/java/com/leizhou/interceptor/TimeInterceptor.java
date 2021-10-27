package com.leizhou.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class TimeInterceptor implements ProducerInterceptor {
    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        return new ProducerRecord(producerRecord.topic(), producerRecord.partition(), producerRecord.key(),
                System.currentTimeMillis() + " , " + producerRecord.value());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
