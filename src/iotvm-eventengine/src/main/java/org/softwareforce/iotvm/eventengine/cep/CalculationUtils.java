package org.softwareforce.iotvm.eventengine.cep;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;

/**
 * Calculation utilities: data quality, statistical functions, and more.
 *
 * @author Dimitris Gkoulis
 */
public final class CalculationUtils {

  /* ------------ Constructors ------------ */

  private CalculationUtils() {}

  /* ------------ Internals ------------ */

  public static double calculateTimeWindowedDecayedTimeliness(
      final long distance, final long maxDistance, final double alpha) {
    assert distance >= 0;
    assert maxDistance >= 0;
    assert alpha >= 0 && alpha <= 1;

    if (distance == 0) {
      // It is in the current time window.
      return 1.0;
    } else if (distance > maxDistance) {
      // It is too old to be considered timely.
      return 0.0; // Event is too old to be considered timely
    } else {
      // Calculate the decayed timeliness.
      return Math.pow(alpha, distance);
    }
  }

  /* ------------ Methods ------------ */

  public static double calculateMSE(List<Double> yTrue, List<Double> yPred) {
    double mse = 0.0;
    for (int i = 0; i < yTrue.size(); i++) {
      mse += Math.pow(yTrue.get(i) - yPred.get(i), 2);
    }
    return mse / (yTrue.size());
  }

  public static Optional<Double> calculateAverage(
      List<SensorTelemetryMeasurementEventIBO> eventList) {
    final int size = eventList.size();
    if (size == 0) {
      return Optional.empty();
    }
    final double sum = eventList.stream().mapToDouble(i -> i.getMeasurement().getValue()).sum();
    final double average = sum / size;
    return Optional.of(average);
  }

  @SuppressWarnings({"RedundantCast", "UnnecessaryLocalVariable"})
  public static double calculateCompleteness1(
      int minimumNumberOfContributingSensors, List<SensorTelemetryMeasurementEventIBO> eventList) {
    double real = (double) minimumNumberOfContributingSensors;
    double expected = (double) eventList.size();
    return 1D - ((real - expected) / real);
  }

  @SuppressWarnings("RedundantCast")
  public static double calculateTimeliness1(
      Long startTimestamp, Long endTimestamp, List<SensorTelemetryMeasurementEventIBO> eventList) {
    if (eventList.isEmpty()) {
      return 0.0D;
    }
    double real = (double) eventList.size();
    double timely = 0.0D;
    for (final SensorTelemetryMeasurementEventIBO event : eventList) {
      if ((event.getTimestamps().getDefaultTimestamp() >= startTimestamp
              && event.getTimestamps().getDefaultTimestamp() <= endTimestamp)
          && !event.getTimestamps().getTimestamps().containsKey(Constants.FABRICATED)) {
        // TODO endTimestamp - 1 (EVERYWHERE - γράψε docs, ποιος το αποφασίζει, κτλ.)
        timely = timely + 1D;
      }
    }
    return 1D - ((real - timely) / real);
  }

  /**
   * Calculate the timeliness as the mean of the degrees of timeliness with optional exponential
   * decay.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>Setting {@code alpha = 0} returns a degree of timeliness of 100 if {@code distance == 0},
   *       otherwise returns 0%.
   *   <li>Setting {@code alpha = 1} returns a degree of timeliness of 100% as long as {@code
   *       distance <= maxDistance}.
   * </ul>
   *
   * <p>TODO write java docs please
   *
   * @param timeWindowStartTimestampMs
   * @param timeWindowEndTimestampMs
   * @param timeWindowSizeMs
   * @param maxDistances
   * @param defaultMaxDistance
   * @param alphas
   * @param defaultAlpha
   * @param eventList
   * @return
   */
  public static double calculateTimeliness(
      long timeWindowStartTimestampMs,
      long timeWindowEndTimestampMs,
      long timeWindowSizeMs,
      Map<String, Long> maxDistances,
      long defaultMaxDistance,
      Map<String, Double> alphas,
      double defaultAlpha,
      List<SensorTelemetryMeasurementEventIBO> eventList) {
    // IMPORTANT NOTICE: startTimestampMs and endTimestampMs are inclusive!
    // For instance, for a time-window with duration of 1 second we expect:
    // TODO add valid examples.
    // 2024-01-01T00:00:00.0000Z - 2024-01-01T00:00:0000Z

    assert timeWindowEndTimestampMs > timeWindowStartTimestampMs;
    assert (timeWindowEndTimestampMs - timeWindowStartTimestampMs) == (timeWindowSizeMs - 1);

    Preconditions.checkState(maxDistances != null);
    for (final Long maxDistance : maxDistances.values()) {
      Preconditions.checkState(maxDistance != null);
      Preconditions.checkState(maxDistance >= 0);
    }
    // Preconditions.checkState(defaultMaxDistance != null);
    Preconditions.checkState(defaultMaxDistance >= 0);

    Preconditions.checkState(alphas != null);
    for (final Double alpha : alphas.values()) {
      Preconditions.checkState(alpha != null);
      Preconditions.checkState(alpha >= 0 && alpha <= 1);
    }
    // Preconditions.checkState(defaultAlpha != null);
    Preconditions.checkState(defaultAlpha >= 0 && defaultAlpha <= 1);

    final Map<String, Double> degreeOfTimelinessMap = new HashMap<>();

    for (final SensorTelemetryMeasurementEventIBO event : eventList) {
      final Long defaultTimestamp = event.getTimestamps().getDefaultTimestamp();
      final boolean isFabricated =
          event.getTimestamps().getTimestamps().containsKey(Constants.FABRICATED);

      final String method = (String) event.getAdditional().get("eventFabricationMethod");
      Long distance = (Long) event.getAdditional().get("eventFabricationDistance");
      // We trust `method` and `distance`. No need to recalculate or event to validate again.

      if (isFabricated) {
        Preconditions.checkState(method != null);
        Preconditions.checkState(distance != null);
      } else {
        Preconditions.checkState(method == null);
        Preconditions.checkState(distance == null);
        distance = 0L;
      }

      // A fabricated event conceptually belongs to the current time window.
      // Therefore, we always expect to find a defaultTimestamp within the time window
      // (possibly equal to timeWindowStartTimestampMs).
      // We use other fields to check its origin (including the original event's timestamp).
      Preconditions.checkState(
          defaultTimestamp >= timeWindowStartTimestampMs
              && defaultTimestamp <= timeWindowEndTimestampMs);

      final double alpha = alphas.getOrDefault(method, defaultAlpha);
      final long maxDistance = maxDistances.getOrDefault(method, defaultMaxDistance);
      final double degreeOfTimeliness =
          calculateTimeWindowedDecayedTimeliness(distance, maxDistance, alpha);
      degreeOfTimelinessMap.put(event.getSensorId(), degreeOfTimeliness);
    }

    if (degreeOfTimelinessMap.isEmpty()) {
      return 0.0D;
    }
    final double sum =
        degreeOfTimelinessMap.values().stream().mapToDouble(Double::doubleValue).sum();
    final int size = degreeOfTimelinessMap.size();
    return sum / size;
  }
}
