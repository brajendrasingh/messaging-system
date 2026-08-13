package com.bksoft.kafka_stream.config;

import com.bksoft.kafka_stream.model.Order;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;

@Configuration
public class OrderStreamProcessor {

    @Value("${app.kafka.order-topic}")
    private String orderTopic;

    @Value("${app.kafka.high-value-order-topic}")
    private String highValueOrderTopic;

    @Bean
    public KStream<String, Order> processOrders(StreamsBuilder builder) {
        KStream<String, Order> orders = builder.stream(orderTopic);
        orders.filter((key, order) -> order.getAmount().compareTo(BigDecimal.valueOf(1000)) > 0)
        .mapValues(order -> {
            if ("NEW".equalsIgnoreCase(order.getStatus())) {
                order.setStatus("PROCESSING");
            }
            return order;
        })
        .peek((key, order) -> System.out.println(key + " -> " + order))
        .to(highValueOrderTopic);

        return orders;
    }
}