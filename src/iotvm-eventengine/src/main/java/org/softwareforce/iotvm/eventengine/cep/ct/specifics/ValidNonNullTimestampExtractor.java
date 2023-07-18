package org.softwareforce.iotvm.eventengine.cep.ct.specifics;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.shared.event.SensorTelemetryEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryRawEventIBO;

/**
 * {@link TimestampExtractor} for {@link SensorTelemetryEventIBO and {@link
 * SensorTelemetryMeasurementEventIBO}}. Requires a valid, non-null timestamp.
 *
 * @author Dimitris Gkoulis
 */
public class ValidNonNullTimestampExtractor implements TimestampExtractor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ValidNonNullTimestampExtractor.class);

  @Override
  public long extract(ConsumerRecord<Object, Object> consumerRecord, long partitionTime) {
    final Object object = consumerRecord.value();
    final String eventType;
    final String clientSideId;
    final Long timestamp;
    if (object instanceof SensorTelemetryRawEventIBO) {
      eventType = "SensorTelemetryRawEventIBO";
      clientSideId = ((SensorTelemetryRawEventIBO) object).getIdentifiers().getClientSideId();
      timestamp = ((SensorTelemetryRawEventIBO) object).getTimestamps().getDefaultTimestamp();
    } else if (object instanceof SensorTelemetryEventIBO) {
      eventType = "SensorTelemetryEventIBO";
      clientSideId = ((SensorTelemetryEventIBO) object).getIdentifiers().getClientSideId();
      timestamp = ((SensorTelemetryEventIBO) object).getTimestamps().getDefaultTimestamp();
    } else if (object instanceof SensorTelemetryMeasurementEventIBO) {
      eventType = "SensorTelemetryMeasurementEventIBO";
      clientSideId =
          ((SensorTelemetryMeasurementEventIBO) object).getIdentifiers().getClientSideId();
      timestamp =
          ((SensorTelemetryMeasurementEventIBO) object).getTimestamps().getDefaultTimestamp();
    } else if (object instanceof SensorTelemetryMeasurementsAverageEventIBO) {
      eventType = "SensorTelemetryMeasurementsAverageEventIBO";
      clientSideId =
          ((SensorTelemetryMeasurementsAverageEventIBO) object).getIdentifiers().getClientSideId();
      timestamp =
          ((SensorTelemetryMeasurementsAverageEventIBO) object)
              .getTimestamps()
              .getDefaultTimestamp();
    } else {
      throw new IllegalArgumentException("type of object " + object + " is unknown!");
    }
    if (timestamp == null) {
      throw new IllegalStateException(
          "timestamp of " + eventType + " : " + clientSideId + " is null which is unexpected!");
    }
    return timestamp;
  }
}
