package org.softwareforce.iotvm.eventengine.cep.ct.specifics;

import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareforce.iotvm.eventengine.cep.util.TimestampExtractorUtil;
import org.softwareforce.iotvm.shared.event.SensorTelemetryEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryRawEventIBO;

/**
 * {@link TimestampExtractor} for {@link SensorTelemetryEventIBO} and {@link
 * SensorTelemetryMeasurementEventIBO}.
 *
 * @author Dimitris Gkoulis
 */
public class FlexibleTimestampExtractor implements TimestampExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlexibleTimestampExtractor.class);

  @Override
  public long extract(ConsumerRecord<Object, Object> consumerRecord, long partitionTime) {
    final Object object = consumerRecord.value();
    final Long timestamp;

    if (object instanceof SensorTelemetryRawEventIBO) {
      timestamp =
          TimestampExtractorUtil.get(
              ((SensorTelemetryRawEventIBO) object).getTimestamps(), List.of(partitionTime));
    } else if (object instanceof SensorTelemetryEventIBO) {
      timestamp =
          TimestampExtractorUtil.get(
              ((SensorTelemetryEventIBO) object).getTimestamps(), List.of(partitionTime));
    } else if (object instanceof SensorTelemetryMeasurementEventIBO) {
      timestamp =
          TimestampExtractorUtil.get(
              ((SensorTelemetryMeasurementEventIBO) object).getTimestamps(),
              List.of(partitionTime));
    } else {
      throw new IllegalArgumentException("type of object " + object + " is unknown!");
    }

    //noinspection ConstantValue
    if (timestamp == null) {
      throw new IllegalStateException("timestamp is null which is unreachable!");
    }

    return timestamp;
  }
}
