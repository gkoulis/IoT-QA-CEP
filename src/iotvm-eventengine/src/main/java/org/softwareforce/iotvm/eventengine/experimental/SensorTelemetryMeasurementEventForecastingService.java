package org.softwareforce.iotvm.eventengine.experimental;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * <p>TODO Rename to SensorTelemetryMeasurementEventForecastingClient
 *
 * @author Dimitris Gkoulis
 */
public class SensorTelemetryMeasurementEventForecastingService {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SensorTelemetryMeasurementEventForecastingService.class);

  private static final Random RANDOM = new Random();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @SuppressWarnings("Convert2Diamond")
  private static final TypeReference<Map<String, Object>> TYPE_REFERENCE =
      new TypeReference<Map<String, Object>>() {};

  /* ------------ Constructors ------------ */

  public SensorTelemetryMeasurementEventForecastingService() {}

  /* ------------ Internals ------------ */

  private SensorTelemetryMeasurementEventIBO newInstance(
      final String sensorId,
      final PhysicalQuantity physicalQuantity,
      ForecastResult forecastResult) {
    final MeasurementIBO measurementIBO =
        MeasurementIBO.newBuilder()
            .setName(physicalQuantity.getName())
            .setValue(forecastResult.measurement())
            .setUnit(physicalQuantity.getUnit())
            .build();

    final long nowTimestamp = Instant.now().toEpochMilli();
    // Set the forecast window start timestamp..
    final long defaultTimestamp = forecastResult.forecastWindowStartTimestamp();
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
    additional.put("accuracy", 0.99D);
    // TODO add real metrics.
    additional.put("forecastWindowStartTimestamp", forecastResult.forecastWindowStartTimestamp());
    additional.put("forecastWindowEndTimestamp", forecastResult.forecastWindowEndTimestamp());

    return SensorTelemetryMeasurementEventIBO.newBuilder()
        .setSensorId(sensorId)
        .setMeasurement(measurementIBO)
        .setTimestamps(timestampsIBO)
        .setIdentifiers(identifiersIBO)
        .setAdditional(additional)
        .build();
  }

  private ForecastResult requestForecast(
      final PhysicalQuantity physicalQuantity,
      final String sensorId,
      final String topicName,
      final String frequency,
      final long startTimestamp,
      final long endTimestamp)
      throws IOException, InterruptedException, ClassCastException, IllegalStateException {
    final Map<String, Object> requestBodyData = new HashMap<>();
    requestBodyData.put("sensor_id", sensorId);
    requestBodyData.put("physical_quantity", physicalQuantity.getName());
    requestBodyData.put("topic_name", topicName);
    requestBodyData.put("frequency", frequency);
    requestBodyData.put("window_start_timestamp", startTimestamp);
    requestBodyData.put("window_end_timestamp", endTimestamp);
    final String body = OBJECT_MAPPER.writeValueAsString(requestBodyData);
    final var client = HttpClient.newHttpClient();
    final var request =
        HttpRequest.newBuilder(
                URI.create("http://localhost:5000/forecast-sensor-telemetry-measurement-value"))
            .header("accept", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
    final var response = client.send(request, BodyHandlers.ofString());
    final Map<String, Object> responseBodyData =
        OBJECT_MAPPER.readValue(response.body(), TYPE_REFERENCE);
    final double measurement = (double) responseBodyData.get("measurement");
    final long forecastWindowStartTimestamp =
        (long) responseBodyData.get("forecast_window_start_timestamp");
    final long forecastWindowEndTimestamp =
        (long) responseBodyData.get("forecast_window_end_timestamp");
    return new ForecastResult(
        measurement, forecastWindowStartTimestamp, forecastWindowEndTimestamp);
  }

  private Optional<ForecastResult> requestForecastSafely(
      final PhysicalQuantity physicalQuantity,
      final String sensorId,
      final String topicName,
      final String frequency,
      final long startTimestamp,
      final long endTimestamp) {
    try {
      return Optional.of(
          this.requestForecast(
              physicalQuantity, sensorId, topicName, frequency, startTimestamp, endTimestamp));
    } catch (Exception ex) {
      LOGGER.error(
          "Forecast failed! ({}, {}, {}, {}, {}, {})",
          physicalQuantity,
          sensorId,
          topicName,
          frequency,
          startTimestamp,
          endTimestamp,
          ex);
      return Optional.empty();
    }
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
      final PhysicalQuantity physicalQuantity,
      final String sensorId,
      final String topicName,
      final String frequency,
      final long startTimestamp,
      final long endTimestamp,
      final long timeWindowSize) {
    final ForecastResult forecastResult =
        this.requestForecastSafely(
                physicalQuantity, sensorId, topicName, frequency, startTimestamp, endTimestamp)
            .orElse(null);
    if (forecastResult == null) {
      return Optional.empty();
    }
    return Optional.of(this.newInstance(sensorId, physicalQuantity, forecastResult));
  }

  /* ------------ Class ------------ */

  private record ForecastResult(
      double measurement, long forecastWindowStartTimestamp, long forecastWindowEndTimestamp) {}
}
