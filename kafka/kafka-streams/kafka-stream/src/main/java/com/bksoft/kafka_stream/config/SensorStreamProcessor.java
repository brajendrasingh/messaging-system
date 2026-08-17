package com.bksoft.kafka_stream.config;

import com.bksoft.kafka_stream.model.SensorData;
import com.bksoft.kafka_stream.model.SensorDataAggregate;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class SensorStreamProcessor {
    @Value("${app.kafka.sensor-topic}")
    private String sensorTopic;

    @Value("${app.kafka.high-temperatures-topic}")
    private String highTemperaturesTopic;

    @Value("${app.kafka.high-temperatures-5min-topic}")
    private String highTemperature5Min;

    @Bean
    public KStream<String, SensorData> processSensorData(StreamsBuilder builder) {
        KStream<String, SensorData> sensors = builder.stream(sensorTopic);
        sensors.filter((key, sensor) -> sensor.getTemperatures().stream().anyMatch(t -> t.getValue() != null && t.getValue() > 1.0))
                .peek((key, sensor) -> System.out.println(key + " -> " + sensor))
                .to(highTemperaturesTopic);

        return sensors;
    }

    @Bean
    public KStream<String, SensorData> processSensors(StreamsBuilder builder) {
        KStream<String, SensorData> sensors = builder.stream(sensorTopic);
        sensors.selectKey((key, sensor) -> sensor.getSensorId())  // Use sensorId as the Kafka Streams key
                // Create 5-minute tumbling windows
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                // Process all SensorData records received during the 5-minute window
                .aggregate(SensorDataAggregate::new, (sensorId, sensor, aggregate) -> {
                            aggregate.add(sensor);
                            return aggregate;
                        }, Materialized.as("sensor-5-minute-window-store")
                )
                // Convert Windowed<String> back to String
                .toStream()
                .filter((windowedKey, aggregate) -> aggregate.hasHighTemperature())
                .map((windowedKey, aggregate) -> KeyValue.pair(windowedKey.key(), aggregate.toSensorData()))
                .peek((key, sensor) -> System.out.println("5-minute window -> " + key + " -> " + sensor))
                .to(highTemperature5Min);

        return sensors;
    }
}
