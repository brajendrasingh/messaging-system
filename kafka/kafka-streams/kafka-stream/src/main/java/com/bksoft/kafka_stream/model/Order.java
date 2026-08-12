package com.bksoft.kafka_stream.model;

import java.math.BigDecimal;
import java.time.Instant;

public class Order {
    private String orderId;
    private String customerId;
    private BigDecimal amount;
    private String status;
    private String product;
    private Integer quantity;
    private Instant orderDate;

    public Order() {
    }

    public Order(String orderId, String customerId, BigDecimal amount, String status, String product, Integer quantity, Instant orderDate) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.status = status;
        this.product = product;
        this.quantity = quantity;
        this.orderDate = orderDate;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
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

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    public Instant getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(Instant orderDate) {
        this.orderDate = orderDate;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", amount=" + amount +
                ", status='" + status + '\'' +
                ", product='" + product + '\'' +
                ", quantity=" + quantity +
                ", orderDate=" + orderDate +
                '}';
    }
}

