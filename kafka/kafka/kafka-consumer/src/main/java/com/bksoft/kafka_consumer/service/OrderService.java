package com.bksoft.kafka_consumer.service;

import com.bksoft.kafka_consumer.model.Order;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Service
public class OrderService {
    public void process(Order event) {
        // Save to database
        // Call payment service
        // Send notification
        // Update inventory
        System.out.println("Processing order: " + event.getOrderId());
    }

    public List<Order> getAllOrders() {
        return List.of(new Order("orderId", "XYZ pvt. Ltd.", BigDecimal.valueOf(2345242.0), "Completed", "Business Monitor", 5, Instant.now()));
    }
}
