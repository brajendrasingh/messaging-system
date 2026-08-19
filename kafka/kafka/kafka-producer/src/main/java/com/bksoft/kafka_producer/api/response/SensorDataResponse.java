package com.bksoft.kafka_producer.api.response;

public class SensorDataResponse {
    private String sensorId;
    private String status;

    public SensorDataResponse() {
    }

    public SensorDataResponse(String sensorId, String status) {
        this.sensorId = sensorId;
        this.status = status;
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
