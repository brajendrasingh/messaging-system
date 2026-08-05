package com.bksoft.kafka_consumer.service;

import com.bksoft.kafka_consumer.model.Order;
import org.springframework.stereotype.Service;

@Service
public class OrderService {
    public void process(Order event) {
        // Save to database
        // Call payment service
        // Send notification
        // Update inventory
        System.out.println("Processing order: " + event.getOrderId());
    }
}
