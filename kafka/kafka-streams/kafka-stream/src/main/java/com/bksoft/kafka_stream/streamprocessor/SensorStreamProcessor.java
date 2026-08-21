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

    @Bean
    public KStream<String, SensorData> sensorStream(StreamsBuilder builder) {
        sensorSerde.ignoreTypeHeaders();
        return builder.stream(sensorTopic, Consumed.with(Serdes.String(), sensorSerde)).selectKey((key, sensor) -> sensor.getSensorId());// Use sensorId as the Kafka Streams key
    }

    @Bean // average sensor humidity in 5 minutes
    public KStream<Windowed<String>, SensorDataAggregate> averageHumidityIn5Minutes(KStream<String, SensorData> sensorStream) {
        KStream<Windowed<String>, SensorDataAggregate> averageHumidity = sensorStream.groupByKey() // Create 5-minute tumbling windows
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                // Process all SensorData records received during the 5-minute window
                .aggregate(SensorDataAggregate::new, (sensorId, sensor, aggregate) -> {
                    aggregate.add(sensor);
                    return aggregate;
                }, Materialized.with(Serdes.String(), new JacksonJsonSerde<>(SensorDataAggregate.class)))
                .toStream().peek((key, aggregate) -> System.out.println("5 Minute average humidity: SensorId -> " + key.key() + ", Average -> " + aggregate.getAverageHumidity()));

        averageHumidity.to(averageHumidityIn5Minutes);

        return averageHumidity;
    }

    @Bean // average sensor temperature in 5 minutes
    public KStream<Windowed<String>, SensorDataAggregate> averageTemperatureIn5Minutes(KStream<String, SensorData> sensorStream) {
        KStream<Windowed<String>, SensorDataAggregate> averageTemperature = sensorStream.groupByKey() // Create 5-minute tumbling windows
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                // Process all SensorData records received during the 5-minute window
                .aggregate(SensorDataAggregate::new, (sensorId, sensor, aggregate) -> {
                    aggregate.add(sensor);
                    return aggregate;
                }, Materialized.with(Serdes.String(), new JacksonJsonSerde<>(SensorDataAggregate.class)))
                .toStream().peek((key, aggregate) -> System.out.println("5 Minute average temperature: SensorId -> " + key.key() + ", Average -> " + aggregate.getAverageTemperature()));

        averageTemperature.to(averageTemperatureIn5Minutes);

        return averageTemperature;

    }

    @Bean //Threshold breach: Allowed temperature range is: 10°C - 40°C
    public KStream<String, SensorData> detectSensorThresholdBreaches(KStream<String, SensorData> sensorStream) {
        KStream<String, SensorData> breaches = sensorStream.filter((key, sensor) -> sensor != null && sensor.getTemperatures() != null
                        && sensor.getTemperatures().stream().anyMatch(t -> t.getValue() != null && (t.getValue() < 10.0 || t.getValue() > 40.0)))
                .peek((key, sensor) -> System.out.println("Threshold breach: SensorId -> " + key + ", Sensor -> " + sensor));

        breaches.to(thresholdBreachesTopic, Produced.with(Serdes.String(), sensorSerde));
        return breaches;
    }

    @Bean //Temperature Anomalies
    public KStream<String, SensorData> detectTemperatureAnomalies(KStream<String, SensorData> sensorStream) {
        KStream<String, SensorData> anomalies = sensorStream.filter((key, sensor) -> sensor != null && sensor.getTemperatures() != null && !sensor.getTemperatures().isEmpty())
                .filter((key, sensor) -> {
                    Temperature temperature = sensor.getTemperatures().get(0);
                    if (temperature.getValue() == null) {
                        return false;
                    }
                    double value = temperature.getValue();
                    return value < 10.0 || value > 40.0;
                }).peek((key, sensor) -> System.out.println("Temperature Anomalies detected: SensorId -> " + key + ", Temperature -> " + sensor.getTemperatures().get(0).getValue()));
        anomalies.to(temperaturesAnomaliesTopic, Produced.with(Serdes.String(), sensorSerde));
        return anomalies;
    }
}
