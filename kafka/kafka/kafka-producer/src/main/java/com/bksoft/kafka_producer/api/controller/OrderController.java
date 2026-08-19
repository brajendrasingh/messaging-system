package com.bksoft.kafka_producer.api.controller;

import com.bksoft.kafka_producer.api.request.OrderRequest;
import com.bksoft.kafka_producer.models.Order;
import com.bksoft.kafka_producer.service.OrderProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api")
public class OrderController {
    private final OrderProducer producer;

    public OrderController(OrderProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/orders")
    public String createOrder(@RequestBody OrderRequest request) {
        String orderId = UUID.randomUUID().toString().replace("-", "");
        log.info("Creating order: orderId={}, customerId={}, amount={}, product={}, quantity={}, status={}", orderId, request.getCustomerId(), request.getAmount(), request.getProduct(), request.getQuantity(), request.getStatus());
        Order event = new Order(orderId, request.getCustomerId(), request.getAmount(), request.getStatus(), request.getProduct(), request.getQuantity(), Instant.now());
        producer.publish(event);
        log.info("Order published successfully: orderId={}", orderId);
        return "Order Created: " + orderId;
    }
}
