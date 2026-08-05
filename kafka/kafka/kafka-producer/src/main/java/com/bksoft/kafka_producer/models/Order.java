package com.bksoft.kafka_producer.models;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public class Order {
    private String orderId;
    private String customerId;
    private BigDecimal amount;
    private String status;
    private LocalDateTime createdAt;

    public Order() {
    }

    public Order(String orderId,
                 String customerId,
                 BigDecimal amount,
                 String status,
                 LocalDateTime createdAt) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.status = status;
        this.createdAt = createdAt;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }
}
