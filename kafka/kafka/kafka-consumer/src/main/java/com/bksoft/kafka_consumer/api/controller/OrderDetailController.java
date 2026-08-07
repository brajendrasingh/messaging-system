package com.bksoft.kafka_consumer.api.controller;

import com.bksoft.kafka_consumer.api.response.OrderResponse;
import com.bksoft.kafka_consumer.model.Order;
import com.bksoft.kafka_consumer.service.OrderService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    @PutMapping("/{orderId}/status")
    public ResponseEntity<String> updateStatus(@PathVariable String orderId, @RequestBody Map<String,String> request) {
        String status = request.get("status");
        orderService.updateStatus(orderId, status);
        return ResponseEntity.ok("Status updated");
    }
}
