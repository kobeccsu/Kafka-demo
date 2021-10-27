package com.leizhou.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor {
    int successCounter;
    int errorCounter;

    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        return producerRecord;
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        if(e == null){
            successCounter++;
        }else {
            errorCounter++;
        }
    }

    @Override
    public void close() {
        System.out.println("successfully " + successCounter);
        System.out.println("error " + errorCounter);
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
