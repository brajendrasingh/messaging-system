package com.bksoft.kafka_consumer.service;

import com.bksoft.kafka_consumer.entities.OrderEntity;
import com.bksoft.kafka_consumer.model.Order;
import com.bksoft.kafka_consumer.repositories.OrderRepository;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class OrderService {

    private final OrderRepository orderRepository;

    public OrderService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    public void process(Order event) {
        OrderEntity orderEntity = new OrderEntity();
        orderEntity.setOrderId(event.getOrderId());
        orderEntity.setProduct(event.getProduct());
        orderEntity.setQuantity(event.getQuantity());
        orderEntity.setAmount(event.getAmount());
        orderEntity.setStatus("Completed");
        orderEntity.setCustomerId(event.getCustomerId());
        orderEntity.setOrderDate(event.getOrderDate());
        orderRepository.save(orderEntity);
        // Call payment service
        // Send notification
        // Update inventory
        System.out.println("Processing order: " + event.getOrderId());
    }

    public void updateStatus(String orderId, String status) {
        OrderEntity order = orderRepository.findByorderId(orderId).orElseThrow(() -> new RuntimeException("Order not found: " + orderId));
        order.setStatus(status);
        orderRepository.save(order);
    }

    public List<Order> getAllOrders() {
        List<Order> response = new ArrayList<>();
        for (OrderEntity order : orderRepository.findAll()) {
            response.add(new Order(order.getOrderId(), order.getCustomerId(), order.getAmount(), order.getStatus(), order.getProduct(), order.getQuantity(), order.getOrderDate()));
        }
        return response;
        //return List.of(new Order("orderId", "XYZ pvt. Ltd.", BigDecimal.valueOf(2345242.0), "Completed", "Business Monitor", 5, Instant.now()));
    }
}
