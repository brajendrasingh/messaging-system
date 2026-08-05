package com.bksoft.kafka_producer.service;

import com.bksoft.kafka_producer.models.Order;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {

    private final KafkaTemplate<String, Order> kafkaTemplate;

    @Value("${app.kafka.order-topic}")
    private String topic;

    public OrderProducer(KafkaTemplate<String, Order> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(Order event) {
        kafkaTemplate.send(topic,
                event.getOrderId(),   // Kafka Key
                event);
    }
}
