package com.bksoft.kafka_producer.api.controller;

import com.bksoft.kafka_producer.api.request.SensorDataRequest;
import com.bksoft.kafka_producer.api.response.SensorDataResponse;
import com.bksoft.kafka_producer.models.SensorData;
import com.bksoft.kafka_producer.service.SensorDataProducer;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/api")
public class SensorController {

    private final SensorDataProducer producer;

    public SensorController(SensorDataProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/sensors")
    public ResponseEntity<SensorDataResponse> createSensorData(@RequestBody SensorDataRequest request) {
        String sensorId = UUID.randomUUID().toString().replace("-", "");

        SensorData event = new SensorData(sensorId, request.getTemperatures(), request.getHumidity(), Instant.now());

        producer.publish(event);

        return ResponseEntity.status(HttpStatus.CREATED).body(new SensorDataResponse());
    }
}
