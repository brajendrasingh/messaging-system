package com.bksoft.kafka_producer.api.controller;

import com.bksoft.kafka_producer.api.request.OrderRequest;
import com.bksoft.kafka_producer.models.Order;
import com.bksoft.kafka_producer.service.OrderProducer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {
    private final OrderProducer producer;

    public OrderController(OrderProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public String createOrder(@RequestBody OrderRequest request) {
        String orderId = UUID.randomUUID().toString();
        Order event = new Order(orderId, request.getCustomerId(), request.getAmount(), "CREATED", LocalDateTime.now());
        producer.publish(event);
        return "Order Created: " + orderId;
    }
}
