package com.bksoft.kafka_stream.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class SensorDataAggregate {

    private String sensorId;

    private List<Temperature> temperatures = new ArrayList<>();

    private Double humiditySum = 0.0;

    private int count = 0;

    private Instant firstTimestamp;

    private Instant lastTimestamp;

    public void add(SensorData sensor) {

        this.sensorId = sensor.getSensorId();

        if (sensor.getTemperatures() != null) {
            temperatures.addAll(sensor.getTemperatures());
        }

        if (sensor.getHumidity() != null) {
            humiditySum += sensor.getHumidity();
        }

        count++;

        if (firstTimestamp == null ||
                sensor.getTimestamp().isBefore(firstTimestamp)) {
            firstTimestamp = sensor.getTimestamp();
        }

        if (lastTimestamp == null ||
                sensor.getTimestamp().isAfter(lastTimestamp)) {
            lastTimestamp = sensor.getTimestamp();
        }
    }

    public boolean hasHighTemperature() {

        return temperatures.stream()
                .anyMatch(t ->
                        t.getValue() != null &&
                                t.getValue() > 1.0
                );
    }

    public SensorData toSensorData() {

        SensorData sensorData = new SensorData();

        sensorData.setSensorId(sensorId);
        sensorData.setTemperatures(temperatures);

        if (count > 0) {
            sensorData.setHumidity(humiditySum / count);
        }

        sensorData.setTimestamp(lastTimestamp);

        return sensorData;
    }
}
