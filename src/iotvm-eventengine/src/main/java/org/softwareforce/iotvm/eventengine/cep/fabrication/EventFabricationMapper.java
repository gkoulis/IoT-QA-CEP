package org.softwareforce.iotvm.eventengine.cep.fabrication;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.softwareforce.iotvm.eventengine.cep.Constants;
import org.softwareforce.iotvm.eventengine.cep.PhysicalQuantity;
import org.softwareforce.iotvm.shared.event.IdentifiersIBO;
import org.softwareforce.iotvm.shared.event.MeasurementIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.TimestampsIBO;

/**
 * Mapper for mapping {@link OutputEvent} to {@link SensorTelemetryMeasurementEventIBO}.
 *
 * @author Dimitris Gkoulis
 * @createdAt Wednesday 13 March 2024
 */
public final class EventFabricationMapper {

  /* ------------ Constructors ------------ */

  private EventFabricationMapper() {}

  /* ------------ Logic ------------ */

  public static SensorTelemetryMeasurementEventIBO map(
      final OutputEvent outputEvent, final PhysicalQuantity physicalQuantity) {
    final MeasurementIBO measurementIBO =
        MeasurementIBO.newBuilder()
            .setName(physicalQuantity.getName())
            .setValue(outputEvent.getValue())
            .setUnit(physicalQuantity.getUnit())
            .build();

    final long nowTimestamp = Instant.now().toEpochMilli();
    final Map<String, Long> timestamps = new HashMap<>();
    timestamps.put(Constants.FABRICATED, nowTimestamp);
    final TimestampsIBO timestampsIBO =
        TimestampsIBO.newBuilder()
            .setDefaultTimestamp(outputEvent.getTimestampMs())
            .setTimestamps(timestamps)
            .build();

    final String clientSideId = UUID.randomUUID().toString();
    final Map<String, String> correlationIds = new HashMap<>();
    final IdentifiersIBO identifiersIBO =
        IdentifiersIBO.newBuilder()
            .setClientSideId(clientSideId)
            .setCorrelationIds(correlationIds)
            .build();

    final Map<String, Object> additional = new HashMap<>();
    additional.put("eventFabricationMethod", outputEvent.getMethod().name());
    additional.put("eventFabricationDistance", outputEvent.getDistance());

    return SensorTelemetryMeasurementEventIBO.newBuilder()
        .setSensorId(outputEvent.getSensorId())
        .setMeasurement(measurementIBO)
        .setTimestamps(timestampsIBO)
        .setIdentifiers(identifiersIBO)
        .setAdditional(additional)
        .build();
  }
}
