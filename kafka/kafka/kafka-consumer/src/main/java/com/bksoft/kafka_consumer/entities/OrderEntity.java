package com.bksoft.kafka_consumer.entities;

import jakarta.persistence.*;
import lombok.Data;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Entity
@Table(name = "orders")
public class OrderEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String orderId;
    private String customerId;
    private BigDecimal amount;
    private String status;
    private String product;
    private Integer quantity;
    private Instant orderDate;

}
