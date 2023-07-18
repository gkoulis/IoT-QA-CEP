package org.softwareforce.iotvm.gateway.kafka;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.softwareforce.iotvm.shared.event.SensorTelemetryEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageAggregateIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryRawEventIBO;

/**
 * Kafka serialization and deserialization.
 *
 * @author Dimitris Gkoulis
 */
@Deprecated
public final class KafkaSerDe {

  private static final Map<String, String> SERDE_CONFIG =
      Collections.singletonMap(
          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
          KafkaConstants.SCHEMA_REGISTRY_URL);

  public static final Serde<String> STRING_SERDE = new Serdes.StringSerde();
  public static final Serde<SensorTelemetryRawEventIBO> SENSOR_TELEMETRY_RAW_EVENT_IBO_SERDE =
      new SpecificAvroSerde<>();
  public static final Serde<SensorTelemetryEventIBO> SENSOR_TELEMETRY_EVENT_IBO_SERDE =
      new SpecificAvroSerde<>();
  public static final Serde<SensorTelemetryMeasurementEventIBO>
      SENSOR_TELEMETRY_MEASUREMENT_EVENT_IBO_SERDE = new SpecificAvroSerde<>();
  public static final Serde<SensorTelemetryMeasurementsAverageAggregateIBO>
      SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_AGGREGATE_IBO_SERDE = new SpecificAvroSerde<>();
  public static final Serde<SensorTelemetryMeasurementsAverageEventIBO>
      SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_IBO_SERDE = new SpecificAvroSerde<>();

  static {
    SENSOR_TELEMETRY_RAW_EVENT_IBO_SERDE.configure(SERDE_CONFIG, false);
    SENSOR_TELEMETRY_EVENT_IBO_SERDE.configure(SERDE_CONFIG, false);
    SENSOR_TELEMETRY_MEASUREMENT_EVENT_IBO_SERDE.configure(SERDE_CONFIG, false);
    SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_AGGREGATE_IBO_SERDE.configure(SERDE_CONFIG, false);
    SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_IBO_SERDE.configure(SERDE_CONFIG, false);
  }

  /* ------------ Constructors ------------ */

  private KafkaSerDe() {}
}
