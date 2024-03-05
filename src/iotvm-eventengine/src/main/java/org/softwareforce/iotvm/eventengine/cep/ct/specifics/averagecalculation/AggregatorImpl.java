package org.softwareforce.iotvm.eventengine.cep.ct.specifics.averagecalculation;

import java.time.Instant;
import java.util.Map;
import org.apache.kafka.streams.kstream.Aggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.eventengine.cep.CalculationUtils;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageAggregateIBO;

/**
 * Applies the computational logic to the aggregate.
 *
 * <p>{@code apply} method is executed each time a new event occurs in the window.
 *
 * @author Dimitris Gkoulis
 */
public class AggregatorImpl
    implements Aggregator<
        String,
        SensorTelemetryMeasurementEventIBO,
        SensorTelemetryMeasurementsAverageAggregateIBO> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AggregatorImpl.class);

  @Override
  public SensorTelemetryMeasurementsAverageAggregateIBO apply(
      String key,
      SensorTelemetryMeasurementEventIBO ibo,
      SensorTelemetryMeasurementsAverageAggregateIBO aggregate) {
    LOGGER.trace("aggregate.Aggregator.apply({})", key);

    final Long now = Instant.now().toEpochMilli();

    final Map<String, Object> additional = aggregate.getAdditional();

    if (!additional.containsKey("aggregationApplicationsCount")) {
      additional.put("aggregationApplicationsCount", 0L);
    }
    if (!additional.containsKey("aggregationApplicationsInsertionsCount")) {
      additional.put("aggregationApplicationsInsertionsCount", 0L);
    }
    if (!additional.containsKey("aggregationApplicationsReplacementsCount")) {
      additional.put("aggregationApplicationsReplacementsCount", 0L);
    }

    long aggregationApplicationsCount = (long) additional.get("aggregationApplicationsCount");
    long aggregationApplicationsInsertionsCount =
        (long) additional.get("aggregationApplicationsInsertionsCount");
    long aggregationApplicationsReplacementsCount =
        (long) additional.get("aggregationApplicationsReplacementsCount");

    aggregationApplicationsCount++;

    final String sensorId = ibo.getSensorId();
    if (aggregate.getEvents().containsKey(sensorId)) {
      final SensorTelemetryMeasurementEventIBO existingIBO = aggregate.getEvents().get(sensorId);
      if (ibo.getTimestamps().getDefaultTimestamp()
          >= existingIBO.getTimestamps().getDefaultTimestamp()) {
        aggregate.getEvents().put(sensorId, ibo);
        aggregationApplicationsReplacementsCount++;
      }
    } else {
      aggregate.getEvents().put(sensorId, ibo);
      aggregationApplicationsInsertionsCount++;
    }

    final long start = System.nanoTime();
    aggregate
        .getAverage()
        .setValue(
            CalculationUtils.calculateAverage(aggregate.getEvents().values().stream().toList())
                .orElse((double) Short.MIN_VALUE));
    final long end = System.nanoTime();
    final long duration = end - start;
    additional.put("aggregationApplicationDuration", duration); // Keep only the last.

    if (aggregate.getTimestamps().getTimestamps().containsKey("firstAggregationApplication")) {
      if (aggregate.getTimestamps().getTimestamps().get("firstAggregationApplication") == null) {
        LOGGER.warn(
            "SensorTelemetryMeasurementsAverageAggregateIBO timestamps entry"
                + " `firstAggregationApplication` is null which is unexpected. Removing.");
        aggregate.getTimestamps().getTimestamps().remove("firstAggregationApplication");
      }
    }

    if (!aggregate.getTimestamps().getTimestamps().containsKey("firstAggregationApplication")) {
      aggregate.getTimestamps().getTimestamps().put("firstAggregationApplication", now);
    }
    aggregate.getTimestamps().getTimestamps().put("lastAggregationApplication", now);

    additional.put("aggregationApplicationsCount", aggregationApplicationsCount);
    additional.put(
        "aggregationApplicationsInsertionsCount", aggregationApplicationsInsertionsCount);
    additional.put(
        "aggregationApplicationsReplacementsCount", aggregationApplicationsReplacementsCount);
    aggregate.setAdditional(additional);

    return aggregate;
  }
}
