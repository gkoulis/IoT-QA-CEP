package org.softwareforce.iotvm.gateway.service;

import jakarta.validation.Valid;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.gateway.configuration.Constants;
import org.softwareforce.iotvm.gateway.model.SensorTelemetryEvent;
import org.softwareforce.iotvm.gateway.service.model.PushSensorTelemetryEventRequest;
import org.softwareforce.iotvm.gateway.service.model.PushSensorTelemetryEventResult;
import org.softwareforce.iotvm.shared.event.IdentifiersIBO;
import org.softwareforce.iotvm.shared.event.MeasurementIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryRawEventIBO;
import org.softwareforce.iotvm.shared.event.TimestampsIBO;

/**
 * Service for managing sensor telemetry events. This service implements the post (HTTP) to push
 * (event bus) pattern.
 *
 * @author Dimitris Gkoulis
 */
public class SensorTelemetryEventService {

  private final Logger log = LoggerFactory.getLogger(SensorTelemetryEventService.class);

  private final KafkaProducer<String, SpecificRecord> kafkaProducer;

  /* ------------ Constructors ------------ */

  public SensorTelemetryEventService(KafkaProducer<String, SpecificRecord> kafkaProducer) {
    this.kafkaProducer = kafkaProducer;
  }

  /* ------------ Logic ------------ */

  public PushSensorTelemetryEventResult pushSensorTelemetryEvent(
      @Valid PushSensorTelemetryEventRequest pushSensorTelemetryEventRequest) {
    log.info("Request to push SensorTelemetryEvent : {}", pushSensorTelemetryEventRequest);
    // TODO Validations (javax @Valid) and Assertions (util Assert).
    // TODO Assertions.

    final SensorTelemetryEvent sensorTelemetryEvent =
        pushSensorTelemetryEventRequest.getSensorTelemetryEvent();

    final Map<String, String> correlationIds = new HashMap<>();
    final String sensorId = sensorTelemetryEvent.getSensorId();
    final List<MeasurementIBO> measurementIBOList =
        sensorTelemetryEvent.getMeasurements().stream()
            .map(
                i ->
                    MeasurementIBO.newBuilder()
                        .setName(i.getName())
                        .setValue(i.getValue())
                        .setUnit(i.getUnit())
                        .build())
            .toList();
    final Map<String, Long> timestamps = new HashMap<>();
    if (sensorTelemetryEvent.getTimestamp() == null) {
      timestamps.put("sensed", null);
    } else {
      timestamps.put("sensed", sensorTelemetryEvent.getTimestamp());
    }
    timestamps.put("received", pushSensorTelemetryEventRequest.getReceivedTimestamp());
    timestamps.put("pushed", Instant.now().toEpochMilli());
    // `defaultTimestamp` is intentionally null.
    // This will force event engine to choose a timestamp.
    final TimestampsIBO timestampsIBO =
        TimestampsIBO.newBuilder().setDefaultTimestamp(null).setTimestamps(timestamps).build();
    final IdentifiersIBO identifiersIBO =
        IdentifiersIBO.newBuilder()
            .setClientSideId(UUID.randomUUID().toString())
            .setCorrelationIds(correlationIds)
            .build();
    final Map<String, Object> additional = new HashMap<>(sensorTelemetryEvent.getAdditional());

    final SensorTelemetryRawEventIBO event =
        SensorTelemetryRawEventIBO.newBuilder()
            .setSensorId(sensorId)
            .setMeasurements(measurementIBOList)
            .setTimestamps(timestampsIBO)
            .setIdentifiers(identifiersIBO)
            .setAdditional(additional)
            .build();

    final ProducerRecord<String, SpecificRecord> producerRecord =
        new ProducerRecord<>(
            Constants.SENSOR_TELEMETRY_RAW_EVENT_TOPIC, Constants.ANY_SENSOR, event);

    // TODO Control send frequency
    // (configure to send message as soon as possible, ideally, immediately).
    // More information: https://www.conduktor.io/kafka/complete-kafka-producer-with-java/
    this.kafkaProducer.send(producerRecord);
    // this.kafkaProducer.flush();

    final PushSensorTelemetryEventResult pushSensorTelemetryEventResult =
        new PushSensorTelemetryEventResult();
    pushSensorTelemetryEventResult.setSensorTelemetryEvent(sensorTelemetryEvent);
    return pushSensorTelemetryEventResult;
  }
}
