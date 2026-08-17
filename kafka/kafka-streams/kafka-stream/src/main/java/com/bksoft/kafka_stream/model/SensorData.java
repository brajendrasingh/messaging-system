package com.bksoft.kafka_stream.model;

import java.time.Instant;
import java.util.List;

public class SensorData {
    private String sensorId;
    private List<Temperature> temperatures;
    private Double humidity;
    private Instant timestamp;

    public SensorData() {
    }

    public SensorData(String sensorId, List<Temperature> temperatures, Double humidity, Instant timestamp) {
        this.sensorId = sensorId;
        this.temperatures = temperatures;
        this.humidity = humidity;
        this.timestamp = timestamp;
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

    public Double getHumidity() {
        return humidity;
    }

    public void setHumidity(Double humidity) {
        this.humidity = humidity;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "SensorData{" +
                "sensorId='" + sensorId + '\'' +
                ", temperatures=" + temperatures +
                ", humidity=" + humidity +
                ", timestamp=" + timestamp +
                '}';
    }
}