package com.bksoft.kafka_stream.streamprocessor;

import com.bksoft.kafka_stream.model.SensorData;
import com.bksoft.kafka_stream.model.SensorDataAggregate;
import com.bksoft.kafka_stream.model.Temperature;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import java.time.Duration;

@Configuration
public class SensorStreamProcessor {
    private JacksonJsonSerde<SensorData> sensorSerde = new JacksonJsonSerde<>(SensorData.class);

    @Value("${app.kafka.sensor-topic}")
    private String sensorTopic;

    @Value("${app.kafka.sensor-temperatures-anomalies-topic}")
    private String temperaturesAnomaliesTopic;

    @Value("${app.kafka.sensor-threshold-breaches-topic}")
    private String thresholdBreachesTopic;

    @Value("${app.kafka.avg-temperature-in-5min-topic}")
    private String averageTemperatureIn5Minutes;

    @Value("${app.kafka.avg-humidity-in-5min-topic}")
    private String averageHumidityIn5Minutes;

    @Bean // average sensor humidity in 5 minutes
    public KStream<Windowed<String>, SensorDataAggregate> averageHumidityIn5Minutes(StreamsBuilder builder) {
        sensorSerde.ignoreTypeHeaders();
        KStream<String, SensorData> sensors = builder.stream(sensorTopic, Consumed.with(Serdes.String(), sensorSerde));
        KStream<Windowed<String>, SensorDataAggregate> averageHumidity = sensors.selectKey((key, sensor) -> sensor.getSensorId())  // Use sensorId as the Kafka Streams key
                .groupByKey() // Create 5-minute tumbling windows
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                // Process all SensorData records received during the 5-minute window
                .aggregate(SensorDataAggregate::new, (sensorId, sensor, aggregate) -> {
                    aggregate.add(sensor);
                    return aggregate;
                }, Materialized.with(Serdes.String(), new JacksonJsonSerde<>(SensorDataAggregate.class)))
                .toStream().peek((key, aggregate) -> System.out.println("5-minute average humidity: Sensor -> " + key + " average -> " + aggregate.getAverageHumidity()));

        averageHumidity.to(averageHumidityIn5Minutes);

        return averageHumidity;
    }

    @Bean // average sensor temperature in 5 minutes
    public KStream<Windowed<String>, SensorDataAggregate> averageTemperatureIn5Minutes(StreamsBuilder builder) {
        sensorSerde.ignoreTypeHeaders();
        KStream<String, SensorData> sensors = builder.stream(sensorTopic, Consumed.with(Serdes.String(), sensorSerde));
        KStream<Windowed<String>, SensorDataAggregate> averageTemperature = sensors.selectKey((key, sensor) -> sensor.getSensorId())  // Use sensorId as the Kafka Streams key
                .groupByKey() // Create 5-minute tumbling windows
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                // Process all SensorData records received during the 5-minute window
                .aggregate(SensorDataAggregate::new, (sensorId, sensor, aggregate) -> {
                    aggregate.add(sensor);
                    return aggregate;
                }, Materialized.with(Serdes.String(), new JacksonJsonSerde<>(SensorDataAggregate.class)))
                .toStream().peek((key, aggregate) -> System.out.println("5-minute average temperature: Sensor -> " + key + " average -> " + aggregate.getAverageTemperature()));

        averageTemperature.to(averageTemperatureIn5Minutes);

        return averageTemperature;

    }

    @Bean //Threshold breach: Allowed temperature range is: 1°C - 3°C
    public KStream<String, SensorData> detectSensorThresholdBreaches(StreamsBuilder builder) {
        sensorSerde.ignoreTypeHeaders();
        KStream<String, SensorData> sensors = builder.stream(sensorTopic, Consumed.with(Serdes.String(), sensorSerde));
        KStream<String, SensorData> breaches = sensors.filter((key, sensor) -> sensor != null && sensor.getTemperatures() != null
                        && sensor.getTemperatures().stream().anyMatch(t -> t.getValue() != null && (t.getValue() < 1.0 || t.getValue() > 3.0)))
                .peek((key, sensor) -> System.out.println("Threshold breach: Sensor -> " + key + " -> " + sensor));

        breaches.to(thresholdBreachesTopic, Produced.with(Serdes.String(), sensorSerde));
        return sensors;
    }

    @Bean //Temperature Anomalies
    public KStream<String, SensorData> detectTemperatureAnomalies(StreamsBuilder builder) {
        sensorSerde.ignoreTypeHeaders();
        KStream<String, SensorData> sensors = builder.stream(sensorTopic, Consumed.with(Serdes.String(), sensorSerde));
        KStream<String, SensorData> anomalies = sensors.selectKey((key, sensor) -> sensor.getSensorId())
                .filter((key, sensor) -> sensor != null && sensor.getTemperatures() != null && !sensor.getTemperatures().isEmpty())
                .filter((key, sensor) -> {
                    Temperature temperature = sensor.getTemperatures().get(0);
                    if (temperature.getValue() == null) {
                        return false;
                    }
                    double value = temperature.getValue();
                    return value < 0.0 || value > 3.0;
                }).peek((key, sensor) -> System.out.println("Temperature Anomalies detected: Sensor -> " + key + ", Temperature -> " + sensor.getTemperatures().get(0).getValue()));
        anomalies.to(temperaturesAnomaliesTopic, Produced.with(Serdes.String(), sensorSerde));
        return sensors;
    }
}
