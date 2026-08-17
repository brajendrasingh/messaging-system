package com.bksoft.kafka_producer.service;

import com.bksoft.kafka_producer.models.SensorData;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class SensorDataProducer {

    private final KafkaTemplate<String, SensorData> kafkaTemplate;

    @Value("${app.kafka.sensor-topic}")
    private String topic;

    public SensorDataProducer(KafkaTemplate<String, SensorData> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(SensorData event) {
        kafkaTemplate.send(topic, event.getSensorId(), event);
    }
}
