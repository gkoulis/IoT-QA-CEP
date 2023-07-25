package org.softwareforce.iotvm.eventengine.experimental;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import org.softwareforce.iotvm.eventengine.cep.Constants;
import org.softwareforce.iotvm.eventengine.cep.PhysicalQuantity;
import org.softwareforce.iotvm.shared.event.IdentifiersIBO;
import org.softwareforce.iotvm.shared.event.MeasurementIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.TimestampsIBO;

/**
 * Service for forecasting future values for a {@link SensorTelemetryMeasurementEventIBO}. These
 * events are considered as fabricated events.
 *
 * @author Dimitris Gkoulis
 */
public class SensorTelemetryMeasurementEventForecastingService {

  private static final Random RANDOM = new Random();

  /* ------------ Constructors ------------ */

  public SensorTelemetryMeasurementEventForecastingService() {}

  /* ------------ Internals ------------ */

  private SensorTelemetryMeasurementEventIBO newInstance(
      final String sensorId, final PhysicalQuantity physicalQuantity, Double value) {
    final MeasurementIBO measurementIBO =
        MeasurementIBO.newBuilder()
            .setName(physicalQuantity.getName())
            .setValue(value)
            .setUnit(physicalQuantity.getUnit())
            .build();

    final long defaultTimestamp = Instant.now().toEpochMilli();
    final Map<String, Long> timestamps = new HashMap<>();
    timestamps.put(Constants.FABRICATED, defaultTimestamp);
    final TimestampsIBO timestampsIBO =
        TimestampsIBO.newBuilder()
            .setDefaultTimestamp(defaultTimestamp)
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

    return SensorTelemetryMeasurementEventIBO.newBuilder()
        .setSensorId(sensorId)
        .setMeasurement(measurementIBO)
        .setTimestamps(timestampsIBO)
        .setIdentifiers(identifiersIBO)
        .setAdditional(additional)
        .build();
  }

  /* ------------ Logic ------------ */

  /**
   * Performs a flexible forecasting for the future value of a physical quantity of a specific
   * sensor. The forecast is performed by a service which is treated as black box. An important fact
   * is that the black box will try to perform the forecasting even if the overall data quality of
   * the dataset is low (for instance missing values are imputed or interpolated).
   *
   * @param sensorId the ID of the sensor.
   * @param physicalQuantity the physical quantity to predict.
   * @return an {@link Optional} with a {@link SensorTelemetryMeasurementEventIBO} if the
   *     forecasting was successful, otherwise an empty optional.
   */
  public Optional<SensorTelemetryMeasurementEventIBO> forecast(
      final String sensorId, final PhysicalQuantity physicalQuantity) {
    final double randomValue = RANDOM.nextDouble((30.0D - 20.0D) + 1) + 20.0D;
    return Optional.of(this.newInstance(sensorId, physicalQuantity, randomValue));
  }
}
