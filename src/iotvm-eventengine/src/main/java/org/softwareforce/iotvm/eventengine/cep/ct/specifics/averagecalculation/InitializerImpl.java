package org.softwareforce.iotvm.eventengine.cep.ct.specifics.averagecalculation;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.streams.kstream.Initializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.eventengine.cep.PhysicalQuantity;
import org.softwareforce.iotvm.shared.event.MeasurementIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageAggregateIBO;
import org.softwareforce.iotvm.shared.event.TimestampsIBO;

/**
 * Initializes the aggregate for each time window. The aggregate is a class that collects the last
 * measurement from each sensor and calculates the average value.
 *
 * @author Dimitris Gkoulis
 * @see SensorTelemetryMeasurementsAverageAggregateIBO
 */
public class InitializerImpl
    implements Initializer<SensorTelemetryMeasurementsAverageAggregateIBO> {

  private static final Logger LOGGER = LoggerFactory.getLogger(InitializerImpl.class);

  private final PhysicalQuantity physicalQuantity;

  public InitializerImpl(PhysicalQuantity physicalQuantity) {
    this.physicalQuantity = physicalQuantity;
  }

  @Override
  public SensorTelemetryMeasurementsAverageAggregateIBO apply() {
    LOGGER.trace("aggregate.Initializer.apply()");
    final Map<String, SensorTelemetryMeasurementEventIBO> events = new HashMap<>();
    final Map<String, Long> timestamps = new HashMap<>();
    timestamps.put("aggregationInitialization", Instant.now().toEpochMilli());
    final Map<String, Object> additional = new HashMap<>();
    return SensorTelemetryMeasurementsAverageAggregateIBO.newBuilder()
        .setAverage(
            MeasurementIBO.newBuilder()
                .setName(this.physicalQuantity.getName())
                .setValue(0.0D)
                .setUnit(this.physicalQuantity.getUnit())
                .build())
        .setEvents(events)
        .setTimestamps(
            TimestampsIBO.newBuilder().setDefaultTimestamp(null).setTimestamps(timestamps).build())
        .setAdditional(additional)
        .build();
  }
}
