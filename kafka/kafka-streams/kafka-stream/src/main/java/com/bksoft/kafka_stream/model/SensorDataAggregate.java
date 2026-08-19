package com.bksoft.kafka_stream.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class SensorDataAggregate {

    private String sensorId;

    private List<Temperature> temperatures = new ArrayList<>();

    private List<Double> humidities = new ArrayList<>();

    private Double humiditySum = 0.0;

    private int humidityCount = 0;

    private Double temperatureSum = 0.0;

    private int temperatureCount = 0;

    private Instant firstTimestamp;

    private Instant lastTimestamp;

    public void add(SensorData sensor) {
        this.sensorId = sensor.getSensorId();
        //Temperature Aggregation
        if (sensor.getTemperatures() != null) {
            sensor.getTemperatures().forEach(t -> {
                if (t.getValue() != null) {
                    temperatures.add(t);
                    temperatureSum += t.getValue();
                    temperatureCount++;
                }
            });
        }
        //Humidity Aggregation
        if (sensor.getHumidity() != null) {
            humidities.add(sensor.getHumidity());
            humiditySum += sensor.getHumidity();
            humidityCount++;
        }
        if (sensor.getTimestamp() != null) {
            if (firstTimestamp == null || sensor.getTimestamp().isBefore(firstTimestamp)) {
                firstTimestamp = sensor.getTimestamp();
            }

            if (lastTimestamp == null || sensor.getTimestamp().isAfter(lastTimestamp)) {
                lastTimestamp = sensor.getTimestamp();
            }
        }
    }

    public boolean hasHighTemperature() {
        return temperatures.stream().anyMatch(t -> t.getValue() != null && t.getValue() > 1.0);
    }

    public SensorData toSensorData() {
        SensorData sensorData = new SensorData();

        sensorData.setSensorId(sensorId);
        sensorData.setTemperatures(temperatures);

        if (humidityCount > 0) {
            sensorData.setHumidity(humiditySum / humidityCount);
        }

        sensorData.setTimestamp(lastTimestamp);

        return sensorData;
    }

    public double getAverageTemperature() {
        if (temperatureCount == 0) {
            return 0.0;
        }
        return temperatureSum / temperatureCount;
    }

    public double getAverageHumidity() {
        if (humidityCount == 0) {
            return 0.0;
        }
        return humiditySum / humidityCount;
    }

    public boolean isTemperatureAnomaly(double temperature) {
        if (temperatureCount == 0) {
            return false;
        }
        double average = getAverageTemperature();
        if (average == 0.0) {
            return false;
        }
        double deviation = Math.abs(temperature - average);
        return deviation > average * 0.50;
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

    public int getHumidityCount() {
        return humidityCount;
    }

    public void setHumidityCount(int humidityCount) {
        this.humidityCount = humidityCount;
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

    public List<Double> getHumidities() {
        return humidities;
    }

    public void setHumidities(List<Double> humidities) {
        this.humidities = humidities;
    }

    public Double getTemperatureSum() {
        return temperatureSum;
    }

    public void setTemperatureSum(Double temperatureSum) {
        this.temperatureSum = temperatureSum;
    }

    public int getTemperatureCount() {
        return temperatureCount;
    }

    public void setTemperatureCount(int temperatureCount) {
        this.temperatureCount = temperatureCount;
    }

    @Override
    public String toString() {
        return "SensorDataAggregate{" +
                "sensorId='" + sensorId + '\'' +
                ", temperatures=" + temperatures +
                ", humidities=" + humidities +
                ", humiditySum=" + humiditySum +
                ", humidityCount=" + humidityCount +
                ", temperatureSum=" + temperatureSum +
                ", temperatureCount=" + temperatureCount +
                ", firstTimestamp=" + firstTimestamp +
                ", lastTimestamp=" + lastTimestamp +
                '}';
    }
}
