package org.softwareforce.iotvm.eventengine.cep.fabrication;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Experimental Event Fabrication Service.
 *
 * @author Dimitris Gkoulis
 * @createdAt Wednesday 13 March 2024
 */
public final class EventFabricationService {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventFabricationService.class);

  private final long timeWindowSizeMs;
  private final Map<String, TimeWindowedTimeSeries> timeSeriesBySensorId;

  /* ------------ Constructors ------------ */

  /**
   * Constructs a {@link EventFabricationService}.
   *
   * @param timeWindowSizeMs the size (milliseconds) of the time-window.
   * @param sensorIds the identifiers of the sensors. This is contextual information and must be
   *     provided in advance.
   */
  public EventFabricationService(final long timeWindowSizeMs, final Set<String> sensorIds) {
    this.timeWindowSizeMs = timeWindowSizeMs;
    this.timeSeriesBySensorId = new HashMap<>();
    for (final String sensorId : sensorIds) {
      this.timeSeriesBySensorId.put(
          sensorId, new TimeWindowedTimeSeries(sensorId, this.timeWindowSizeMs));
    }
  }

  /* ------------ Internal ------------ */

  private Optional<Long> isCandidate(
      final String sensorId,
      final long timeWindowStartTimestampMs,
      final long minTimeSeriesSize,
      final long maxDistance) {
    final TimeWindowedTimeSeries ts = this.timeSeriesBySensorId.get(sensorId);

    // TimeWindowedTimeSeries is empty, so we can do nothing.
    if (ts.getPoints().isEmpty()) {
      return Optional.empty();
    }

    // TimeWindowedTimeSeries is not empty, but it has not the required number of points.
    if (ts.getPoints().size() < minTimeSeriesSize) {
      return Optional.empty();
    }

    final TimeWindowedTimeSeries.TimeWindow lastTimeWindow = ts.getPoints().lastKey();
    // final TimeWindowedTimeSeries.Point lastPoint = ts.getPoints().get(lastTimeWindow);

    final TimeWindowedTimeSeries.TimeWindow timeWindow =
        new TimeWindowedTimeSeries.TimeWindow(timeWindowStartTimestampMs, this.timeWindowSizeMs);
    final long distance = timeWindow.distanceFrom(lastTimeWindow);

    // We expect the timeWindow (CEP) to be greater than the lastTimeWindow
    // (TimeWindowedTimeSeries).
    if (distance > 0) {
      if (distance > maxDistance) {
        // We cannot use it.
        return Optional.empty();
      } else {
        // We can use it.
        return Optional.of(distance);
      }
    } else {
      LOGGER.error(
          "Found sensorId {} with zero or negative distance from the last point!", sensorId);
      return Optional.empty();
    }
  }

  private void ensureCandidateTimeConsistency(
      final String candidateSensorId,
      final long candidateDistance,
      final long timeWindowStartTimestampMs) {
    final TimeWindowedTimeSeries ts = this.timeSeriesBySensorId.get(candidateSensorId);
    final TimeWindowedTimeSeries.TimeWindow lastTimeWindow = ts.getPoints().lastKey();

    final TimeWindowedTimeSeries.TimeWindow timeWindow =
        new TimeWindowedTimeSeries.TimeWindow(timeWindowStartTimestampMs, this.timeWindowSizeMs);
    final long distance = timeWindow.distanceFrom(lastTimeWindow);

    if (candidateDistance != distance) {
      throw new IllegalStateException(
          "candidateDistance "
              + candidateDistance
              + " is not equal to distance "
              + distance
              + " (timeWindowStartTimestampMs"
              + timeWindowStartTimestampMs
              + ")");
    }
  }

  /* ------------ API ------------ */

  public boolean updateTimeWindowedTimeSeries(
      final String sensorId, double value, long timestampMs) {
    if (!this.timeSeriesBySensorId.containsKey(sensorId)) {
      LOGGER.warn("Sensor ID : {} is not registered! Aborting.", sensorId);
      return false;
    }
    this.timeSeriesBySensorId.get(sensorId).update(timestampMs, value);
    return true; // TODO Or return the value of the Time-Series update (i.e., boolean)?
  }

  public Set<OutputEvent> performEventFabrication(
      final List<InputEvent> inputEventList,
      final long timeWindowStartTimestampMs,
      final int minimumNumberOfContributingSensors,
      final int stepsBehind,
      final int stepsAhead) {
    final Set<OutputEvent> outputEventSet = new HashSet<>();

    // --------------------------------------------------

    final boolean naiveEnabled = stepsBehind > 0;
    final boolean simpleExponentialSmoothingEnabled = stepsAhead > 0;

    if (!naiveEnabled && !simpleExponentialSmoothingEnabled) {
      return outputEventSet;
    }

    // Initializations.
    // --------------------------------------------------

    final Set<InputEvent> inputEventSet = new HashSet<>(inputEventList);
    final Set<String> registeredSensorIdSet = this.timeSeriesBySensorId.keySet();
    // null InputEvent means the event may must be fabricated.
    final Map<String, InputEvent> inputEventBySensorId = new HashMap<>();
    final Set<String> existingRegisteredSensorIdSet = new HashSet<>();
    final Set<String> missingRegisteredSensorIdSet = new HashSet<>();
    final Set<String> remainingMissingRegisteredSensorIdSet = new HashSet<>();

    // Log duplicates.
    // --------------------------------------------------

    if (inputEventSet.size() != inputEventList.size()) {
      // TODO Log.
    }

    // For each registered Sensor ID, find its instance if exists.
    // Otherwise, mark it as missing.
    // NOTICE: the non-registered Sensor IDs (and their corresponding instances)
    // are ignored (but logged in the following lines).
    // --------------------------------------------------

    for (final String registeredSensorId : registeredSensorIdSet) {
      InputEvent inputEvent = null;

      for (final InputEvent inputEventTemp : inputEventSet) {
        if (registeredSensorId.equals(inputEventTemp.getSensorId())) {
          inputEvent = inputEventTemp;
          break;
        }
      }

      inputEventBySensorId.put(registeredSensorId, inputEvent);

      if (inputEvent == null) {
        missingRegisteredSensorIdSet.add(registeredSensorId);
        remainingMissingRegisteredSensorIdSet.add(registeredSensorId);
      } else {
        // TODO assert timestampMs, timeWindowStartTimestampMs and value are the same.
        existingRegisteredSensorIdSet.add(registeredSensorId);
        inputEventSet.remove(inputEvent);
      }
    }

    // Log the non-registered sensor IDs.
    // --------------------------------------------------

    // TODO Log.

    // Check if event fabrication should not be performed.
    // --------------------------------------------------

    if (existingRegisteredSensorIdSet.size() >= minimumNumberOfContributingSensors) {
      return outputEventSet;
    }

    // Find the number of the events that should be fabricated
    // to meet the minimum number of contributing sensors requirements.
    // --------------------------------------------------

    final int shouldFabricateCount =
        minimumNumberOfContributingSensors - existingRegisteredSensorIdSet.size();
    int successfulFabricationCount = 0;

    // Naive
    // 1. prepare candidates for event fabrication (use the remaining)
    // 2. sort them by their distance from their last point
    // 3. perform fabrication
    // 4. if fabrication was successful remove from remaining
    // --------------------------------------------------

    if (naiveEnabled) {

      final List<Candidate> candidates1 = new ArrayList<>();
      for (final String sensorId : remainingMissingRegisteredSensorIdSet) {
        final Long distance =
            this.isCandidate(sensorId, timeWindowStartTimestampMs, 1, stepsBehind).orElse(null);
        if (distance == null) {
          continue;
        }
        candidates1.add(new Candidate(sensorId, distance));
      }

      Collections.sort(candidates1);

      for (final Candidate candidate : candidates1) {
        final String candidateSensorId = candidate.sensorId;
        final long candidateDistance = candidate.distance;

        final TimeWindowedTimeSeries ts = this.timeSeriesBySensorId.get(candidateSensorId);
        final TimeWindowedTimeSeries.TimeWindow lastTimeWindow = ts.getPoints().lastKey();
        final TimeWindowedTimeSeries.Point lastPoint = ts.getPoints().get(lastTimeWindow);
        final Double value = lastPoint.getValue();

        // TODO Make optional.
        this.ensureCandidateTimeConsistency(
            candidateSensorId, candidateDistance, timeWindowStartTimestampMs);

        final OutputEvent outputEvent =
            new OutputEvent(
                candidateSensorId,
                value,
                EventFabricationMethod.NAIVE,
                candidateDistance,
                timeWindowStartTimestampMs);

        outputEventSet.add(outputEvent);
        remainingMissingRegisteredSensorIdSet.remove(candidateSensorId);
        successfulFabricationCount = successfulFabricationCount + 1;

        if (successfulFabricationCount >= shouldFabricateCount) {
          break;
        }
      }
    }

    // Check if the requirement is met.
    // --------------------------------------------------

    if (successfulFabricationCount >= shouldFabricateCount) {
      return outputEventSet;
    }

    // SimpleExponentialSmoothing
    // --------------------------------------------------

    if (simpleExponentialSmoothingEnabled) {

      final List<Candidate> candidates1 = new ArrayList<>();
      for (final String sensorId : remainingMissingRegisteredSensorIdSet) {
        final Long distance =
            this.isCandidate(sensorId, timeWindowStartTimestampMs, 2, stepsAhead).orElse(null);
        if (distance == null) {
          continue;
        }
        candidates1.add(new Candidate(sensorId, distance));
      }

      Collections.sort(candidates1);

      for (final Candidate candidate : candidates1) {
        System.out.println(
            "Performing EXPONENTIAL_SMOOTHING_WITH_LINEAR_TREND: " + candidate.toString());

        final String candidateSensorId = candidate.sensorId;
        final long candidateDistance = candidate.distance;

        final TimeWindowedTimeSeries ts = this.timeSeriesBySensorId.get(candidateSensorId);
        final List<Double> series =
            ts.getPoints().values().stream().map(TimeWindowedTimeSeries.Point::getValue).toList();

        /*
        final double alpha = 0.1;
        final double beta = 0.1;
        final int horizon = stepsAhead;
        final ExponentialSmoothingWithLinearTrend forecaster =
            new ExponentialSmoothingWithLinearTrend(alpha, beta, horizon);
        final List<Double> forecasts = forecaster.forecast(series);
        */

        double[] alphaRange = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
        double[] betaRange = {0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
        final int horizon = stepsAhead;
        final ExponentialSmoothingWithLinearTrendOptimization optimizer = new ExponentialSmoothingWithLinearTrendOptimization(alphaRange[0], betaRange[0], horizon);
        optimizer.optimizeParameters(series, alphaRange, betaRange);
        final List<Double> forecasts = optimizer.getBestForecasts();

        assert series.size() + stepsAhead == forecasts.size();
        final Double value = forecasts.get(forecasts.size() - 1);

        // TODO Make optional.
        this.ensureCandidateTimeConsistency(
            candidateSensorId, candidateDistance, timeWindowStartTimestampMs);

        final OutputEvent outputEvent =
            new OutputEvent(
                candidateSensorId,
                value,
                EventFabricationMethod.EXPONENTIAL_SMOOTHING_WITH_LINEAR_TREND,
                candidateDistance,
                timeWindowStartTimestampMs);

        outputEventSet.add(outputEvent);
        remainingMissingRegisteredSensorIdSet.remove(candidateSensorId);
        successfulFabricationCount = successfulFabricationCount + 1;

        if (successfulFabricationCount >= shouldFabricateCount) {
          break;
        }
      }
    }

    return outputEventSet;
  }

  private static final class Candidate implements Comparable<Candidate> {
    private final String sensorId;
    private final long distance;

    private Candidate(String sensorId, long distance) {
      this.sensorId = sensorId;
      this.distance = distance;
    }

    @Override
    public int compareTo(Candidate other) {
      return Long.compare(other.distance, this.distance);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Candidate candidate = (Candidate) o;
      return Objects.equals(sensorId, candidate.sensorId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(sensorId);
    }

    @Override
    public String toString() {
      final StringBuffer sb = new StringBuffer("Candidate{");
      sb.append("sensorId='").append(sensorId).append('\'');
      sb.append(", distance=").append(distance);
      sb.append('}');
      return sb.toString();
    }
  }
}
