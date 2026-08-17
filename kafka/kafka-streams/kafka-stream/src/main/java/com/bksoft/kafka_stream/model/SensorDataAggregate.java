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

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public List<Temperature> getTemperatures() {
        return temperatures;
    }

    public void setTemperatures(List<Temperature> temperatures) {
        this.temperatures = temperatures;
    }

    public Double getHumiditySum() {
        return humiditySum;
    }

    public void setHumiditySum(Double humiditySum) {
        this.humiditySum = humiditySum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public Instant getFirstTimestamp() {
        return firstTimestamp;
    }

    public void setFirstTimestamp(Instant firstTimestamp) {
        this.firstTimestamp = firstTimestamp;
    }

    public Instant getLastTimestamp() {
        return lastTimestamp;
    }

    public void setLastTimestamp(Instant lastTimestamp) {
        this.lastTimestamp = lastTimestamp;
    }

    @Override
    public String toString() {
        return "SensorDataAggregate{" +
                "sensorId='" + sensorId + '\'' +
                ", temperatures=" + temperatures +
                ", humiditySum=" + humiditySum +
                ", count=" + count +
                ", firstTimestamp=" + firstTimestamp +
                ", lastTimestamp=" + lastTimestamp +
                '}';
    }
}
