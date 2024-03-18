package org.softwareforce.iotvm.eventengine.cep;

import java.util.ArrayList;
import java.util.List;
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

  @SuppressWarnings("RedundantCast")
  public static double calculateTimeliness2(
      Long startTimestamp,
      Long endTimestamp,
      int pastWindowsLookup,
      List<SensorTelemetryMeasurementEventIBO> eventList) {
    // TODO Now we have distances!!!!!!!!!!!!!!! and differences!!!!!!

    final long diff = endTimestamp - startTimestamp;
    final List<Long> changePoints = new ArrayList<>();
    for (int i = 1; i <= pastWindowsLookup; i++) {
      changePoints.add(startTimestamp - (diff * i));
    }

    if (eventList.isEmpty()) {
      return 0.0D;
    }
    double real = (double) eventList.size();
    double timely = 0.0D;
    for (final SensorTelemetryMeasurementEventIBO event : eventList) {
      // TODO γίνεται και πιο απλό -> βάλε το τωρινό time window στα change points (με index 0).
      if (event.getTimestamps().getTimestamps().containsKey(Constants.FABRICATED)) {
        timely = timely + 0D;
      } else if (event.getTimestamps().getDefaultTimestamp() >= startTimestamp
          && event.getTimestamps().getDefaultTimestamp() <= endTimestamp) {
        timely = timely + 1D;
      } else {
        double temp = 0D;
        int index = 1;
        for (final long changePoint : changePoints) {
          if (event.getTimestamps().getDefaultTimestamp() < changePoint) {
            index++;
            continue;
          }
          temp = 1D - ((1D / pastWindowsLookup) * index);
          // TODO add to docs
          // ΠΡΟΣΟΧΗ: όταν το pastWindowsLookup είναι 1,
          // το penalty για index = 1, είναι 0.
          // το penalty για index = 2, είναι -1
          break;
        }
        timely = timely + temp;
      }
    }
    return 1D - ((real - timely) / real);
  }
}
