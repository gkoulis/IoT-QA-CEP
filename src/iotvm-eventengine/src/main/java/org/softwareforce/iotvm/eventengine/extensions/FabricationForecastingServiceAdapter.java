package org.softwareforce.iotvm.eventengine.extensions;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.eventengine.cep.Constants;
import org.softwareforce.iotvm.eventengine.cep.PhysicalQuantity;
import org.softwareforce.iotvm.shared.event.IdentifiersIBO;
import org.softwareforce.iotvm.shared.event.MeasurementIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.TimestampsIBO;
import org.softwareforce.iotvm.shared.extensions.fabrication_forecasting.spec.FabricationForecastingService;
import org.softwareforce.iotvm.shared.extensions.fabrication_forecasting.spec.ForecastException;
import org.softwareforce.iotvm.shared.extensions.fabrication_forecasting.spec.ForecastRequest;
import org.softwareforce.iotvm.shared.extensions.fabrication_forecasting.spec.ForecastResponse;
import org.softwareforce.iotvm.shared.extensions.fabrication_forecasting.spec.ForecastScope;

/**
 * Adapter for connecting application with {@link FabricationForecastingService}.
 *
 * @author Dimitris Gkoulis
 */
public class FabricationForecastingServiceAdapter {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FabricationForecastingServiceAdapter.class);

  /* ------------ Constructors ------------ */

  public FabricationForecastingServiceAdapter() {}

  /* ------------ Internals ------------ */

  private SensorTelemetryMeasurementEventIBO newInstance(
      final String sensorId,
      final PhysicalQuantity physicalQuantity,
      final ForecastResponse forecastResponse,
      final long defaultTimestamp) {
    final MeasurementIBO measurementIBO =
        MeasurementIBO.newBuilder()
            .setName(physicalQuantity.getName())
            .setValue(forecastResponse.getValue())
            .setUnit(physicalQuantity.getUnit())
            .build();

    final long nowTimestamp = Instant.now().toEpochMilli();
    final Map<String, Long> timestamps = new HashMap<>();
    timestamps.put(Constants.FABRICATED, nowTimestamp);
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
    additional.put("forecastResponseValue", forecastResponse.value);
    additional.put("forecastResponseStartTimestamp", forecastResponse.startTimestamp);
    additional.put("forecastResponseEndTimestamp", forecastResponse.endTimestamp);
    additional.put("forecastResponseMetricsTimeSteps", forecastResponse.metrics.get("timeSteps"));
    additional.put(
        "forecastResponseMetricsTimeDifference", forecastResponse.metrics.get("timeDifference"));
    additional.put(
        "forecastResponseMetricsCompleteness", forecastResponse.metrics.get("completeness"));

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
   * @param topicName the name of the topic. This will limit the forecasting aggregation records.
   * @param frequencyInSeconds the period interval, aka time series frequency, in seconds.
   * @param startTimestamp the start timestamp in milliseconds.
   * @param endTimestamp the end timestamp in milliseconds.
   * @param stepsAhead the maximum future steps to look for a forecasted value.
   * @return an {@link Optional} with a {@link SensorTelemetryMeasurementEventIBO} if the
   *     forecasting was successful, otherwise an empty optional.
   */
  public Optional<SensorTelemetryMeasurementEventIBO> forecast(
      final PhysicalQuantity physicalQuantity,
      final String sensorId,
      final String topicName,
      final long frequencyInSeconds,
      final long startTimestamp,
      final long endTimestamp,
      final long stepsAhead) {

    final ForecastScope forecastScope =
        new ForecastScope(physicalQuantity.getName(), sensorId, topicName, frequencyInSeconds);
    final ForecastRequest forecastRequest =
        new ForecastRequest(startTimestamp, endTimestamp, stepsAhead);
    final ForecastResponse forecastResponse;

    try {
      final FabricationForecastingService.Client client =
          ExtensionsClientsFactory.getInstance()
              .getFabricationForecastingServiceClient()
              .orElse(null);
      if (client == null) {
        return Optional.empty();
      }
      forecastResponse = client.forecast(forecastScope, forecastRequest);
    } catch (ForecastException ex) {
      LOGGER.warn("forecast ({}) failed. Reason: {}", forecastScope, ex.getReason());
      return Optional.empty();
    } catch (TException ex) {
      LOGGER.error("forecast ({}) failed.", forecastScope, ex);
      return Optional.empty();
    }

    // Just before the time window closes.
    final long defaultTimestamp = endTimestamp - 1;
    return Optional.of(
        this.newInstance(sensorId, physicalQuantity, forecastResponse, defaultTimestamp));
  }
}
