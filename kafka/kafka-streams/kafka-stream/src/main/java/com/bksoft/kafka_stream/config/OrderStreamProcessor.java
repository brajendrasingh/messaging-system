package com.bksoft.kafka_stream.config;

import com.bksoft.kafka_stream.model.Order;
import com.bksoft.kafka_stream.model.OrderAverage;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import java.math.BigDecimal;
import java.time.Duration;

@Configuration
public class OrderStreamProcessor {
    private JacksonJsonSerde<Order> orderSerde = new JacksonJsonSerde<>(Order.class);

    @Value("${app.kafka.order-topic}")
    private String orderTopic;

    @Value("${app.kafka.high-value-order-topic}")
    private String highValueOrderTopic;

    @Bean
    public KStream<String, Order> processOrders(StreamsBuilder builder) {
        orderSerde.ignoreTypeHeaders();
        KStream<String, Order> orders = builder.stream(orderTopic, Consumed.with(Serdes.String(), orderSerde));
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

    @Bean
    public KTable<Windowed<String>, Double> ordersAvgPrice(StreamsBuilder builder) {
        orderSerde.ignoreTypeHeaders();
        KStream<String, Order> orders = builder.stream(orderTopic, Consumed.with(Serdes.String(), orderSerde));

        KTable<Windowed<String>, Double> result = orders.filter((key, order) -> order != null && order.getAmount() != null)
                .peek((key, order) -> System.out.println("Payload: " + key + " -> " + order))
                // Put all orders into one group
                .selectKey((key, order) -> "ALL")
                // Group orders
                .groupByKey(Grouped.with(Serdes.String(), new JacksonJsonSerde<>(Order.class)))
                // 5-minute tumbling window
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                .aggregate(OrderAverage::new, (key, order, average) -> {
                            average.sum += order.getAmount().doubleValue();
                            average.count++;
                            return average;
                        }, Materialized.with(Serdes.String(), new JacksonJsonSerde<>(OrderAverage.class))
                ).mapValues(OrderAverage::getAverage); // Convert OrderAverage to average Double

        // Print the result whenever the KTable is updated
        result.toStream()
                .peek((windowedKey, average) -> System.out.println("Window: " + windowedKey.window().startTime() + " - " + windowedKey.window().endTime() + " | Average Order Amount = " + average))
                .to("average-order-price");

        return result;
    }
}