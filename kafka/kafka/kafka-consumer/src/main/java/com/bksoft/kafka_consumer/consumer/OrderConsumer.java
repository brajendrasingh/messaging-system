package com.bksoft.kafka_consumer.consumer;

import com.bksoft.kafka_consumer.model.Order;
import com.bksoft.kafka_consumer.service.OrderService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {
    private final OrderService orderService;

    public OrderConsumer(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(
            topics = "${app.kafka.order-topic}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(Order event) {
        System.out.println("--------------------------------");
        System.out.println("Order Received");
        System.out.println("Order Id   : " + event.getOrderId());
        System.out.println("CustomerId : " + event.getCustomerId());
        System.out.println("Amount     : " + event.getAmount());
        System.out.println("Status     : " + event.getStatus());
        System.out.println("--------------------------------");
        orderService.process(event);
    }
}
