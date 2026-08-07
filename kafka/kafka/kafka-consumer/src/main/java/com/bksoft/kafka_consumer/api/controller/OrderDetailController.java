package com.bksoft.kafka_consumer.api.controller;

import com.bksoft.kafka_consumer.api.response.OrderResponse;
import com.bksoft.kafka_consumer.model.Order;
import com.bksoft.kafka_consumer.service.OrderService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/api")
public class OrderDetailController {

    private final OrderService orderService;

    public OrderDetailController(OrderService orderService) {
        this.orderService = orderService;
    }

    @GetMapping("/orders")
    public List<OrderResponse> getOrders() {
        List<OrderResponse> responseList = new ArrayList<>();
        for (Order order : orderService.getAllOrders()) {
            responseList.add(new OrderResponse(order.getOrderId(), order.getCustomerId(), order.getAmount(), order.getStatus(), order.getProduct(), order.getQuantity(), order.getOrderDate()));
        }
        return responseList;
    }
}
