package org.softwareforce.iotvm.eventengine.cep;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.softwareforce.iotvm.shared.event.SensorTelemetryEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageAggregateIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryRawEventIBO;

/**
 * All-in-one configuration.
 *
 * @author Dimitris Gkoulis
 */
public final class Constants {

  /** TODO Replace: get {@code Sensor} instances by greenhouse or area (better area). */
  public static final List<String> SENSOR_IDS =
      List.of("sensor-1", "sensor-2", "sensor-3", "sensor-4", "sensor-5", "sensor-6");

  /* ------------ Message Keys ------------ */

  public static final String ANY_SENSOR = "any_sensor";

  /* ------------ Topics ------------ */

  public static final String TOPIC_PREFIX = "ga";
  public static final String TOPIC_SUFFIX = "0001";
  public static final String TOPIC_SEPARATOR = ".";

  public static final String SENSOR_TELEMETRY_RAW_EVENT_LITERAL = "sensor_telemetry_raw_event";
  public static final String SENSOR_TELEMETRY_EVENT_LITERAL = "sensor_telemetry_event";
  public static final String SENSOR_TELEMETRY_MEASUREMENT_EVENT_LITERAL =
      "sensor_telemetry_measurement_event";
  public static final String SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_LITERAL =
      "sensor_telemetry_measurements_average_event";

  public static final String SENSOR_TELEMETRY_RAW_EVENT_TOPIC =
      TOPIC_PREFIX
          + TOPIC_SEPARATOR
          + SENSOR_TELEMETRY_RAW_EVENT_LITERAL
          + TOPIC_SEPARATOR
          + TOPIC_SUFFIX;
  public static final String SENSOR_TELEMETRY_EVENT_TOPIC =
      TOPIC_PREFIX
          + TOPIC_SEPARATOR
          + SENSOR_TELEMETRY_EVENT_LITERAL
          + TOPIC_SEPARATOR
          + TOPIC_SUFFIX;
  public static final String SENSOR_TELEMETRY_MEASUREMENT_EVENT_TOPIC =
      TOPIC_PREFIX
          + TOPIC_SEPARATOR
          + SENSOR_TELEMETRY_MEASUREMENT_EVENT_LITERAL
          + TOPIC_SEPARATOR
          + TOPIC_SUFFIX;
  public static final String SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_TOPIC =
      TOPIC_PREFIX
          + TOPIC_SEPARATOR
          + SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_LITERAL
          + TOPIC_SEPARATOR
          + TOPIC_SUFFIX;

  private static final Pattern SUFFIX_PATTERN = Pattern.compile("^[a-zA-Z0-9_]*$");

  /* ------------ Timestamps Keys ------------ */

  public static final String RECORD_TIMESTAMP = "recordTimestamp";
  public static final String SENSED = "sensed";
  public static final String RECEIVED = "received";
  public static final String PUSHED = "pushed";
  public static final String INGESTED = "ingested";
  public static final String SPLITTED_L1 = "splittedL1";
  public static final String SPLITTED_L1_REAL = "splittedL1Real";
  public static final String SPLITTED_L2 = "splittedL2";
  public static final String MERGED_L1 = "mergedL1";
  public static final String MERGED_L2 = "mergedL2";
  public static final String PERSISTED = "persisted";
  public static final String FABRICATED = "fabricated";

  /* ------------ IDs and Correlation IDs Keys ------------ */

  public static final String CLIENT_SIDE_ID = "clientSideId";
  public static final String PERSISTENCE_ID = "persistenceId";

  /* ------------ Key Prefixes ------------ */

  public static String keyForSensorTelemetryRawEventIBO(String key) {
    return SensorTelemetryRawEventIBO.getClassSchema().getName() + "." + key;
  }

  public static String keyForSensorTelemetryEventIBO(String key) {
    return SensorTelemetryEventIBO.getClassSchema().getName() + "." + key;
  }

  public static String keyForSensorTelemetryMeasurementEventIBO(String key) {
    return SensorTelemetryMeasurementEventIBO.getClassSchema().getName() + "." + key;
  }

  public static String keyForSensorTelemetryMeasurementsAverageEventIBO(String key) {
    return SensorTelemetryMeasurementsAverageEventIBO.getClassSchema().getName() + "." + key;
  }

  /* ------------ Serialization / Deserialization ------------ */

  // TODO Temporary disabled.
  //  private static final Map<String, String> SERDE_CONFIG =
  //      Collections.singletonMap(
  //          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
  //          KafkaConfiguration.SCHEMA_REGISTRY_URL);

  // TODO Temporary enabled.
  private static final Map<String, String> SERDE_CONFIG =
      Collections.singletonMap(
          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://localhost");

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

  private Constants() {}

  /* ------------ Topics: SENSOR_TELEMETRY_MEASUREMENT_EVENT_TOPIC ------------ */

  /**
   * Example topic {@code "ga.sensor_telemetry_measurement_event.0001.temperature"}.
   *
   * @param physicalQuantity the {@link PhysicalQuantity}.
   * @return the topic.
   */
  public static String getSensorTelemetryMeasurementEventTopic(PhysicalQuantity physicalQuantity) {
    return SENSOR_TELEMETRY_MEASUREMENT_EVENT_TOPIC
        + TOPIC_SEPARATOR
        + physicalQuantity.name().toLowerCase(Locale.ROOT);
  }

  /* ------------ Topics: SENSOR_TELEMETRY_MEASUREMENT_EVENT_TOPIC ------------ */

  /**
   * Example topic {@code "ga.sensor_telemetry_measurements_average_event.0001.temperature"}.
   *
   * @param physicalQuantity the {@link PhysicalQuantity}.
   * @return the topic.
   */
  public static String getSensorTelemetryMeasurementsAverageEventTopic(
      PhysicalQuantity physicalQuantity) {
    return SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_TOPIC
        + TOPIC_SEPARATOR
        + physicalQuantity.name().toLowerCase(Locale.ROOT);
  }

  /**
   * Example topic {@code
   * "ga.sensor_telemetry_measurements_average_event.0001.temperature.application_defined_suffix"}.
   *
   * @param physicalQuantity the {@link PhysicalQuantity}.
   * @param suffix the topic suffix. It must contain a-z or A-Z and underscores only.
   * @return the topic.
   */
  public static String getSensorTelemetryMeasurementsAverageEventTopic(
      PhysicalQuantity physicalQuantity, String suffix) {
    if (!SUFFIX_PATTERN.matcher(suffix).matches()) {
      throw new IllegalArgumentException("suffix " + suffix + " is not valid!");
    }
    return SENSOR_TELEMETRY_MEASUREMENTS_AVERAGE_EVENT_TOPIC
        + TOPIC_SEPARATOR
        + physicalQuantity.name().toLowerCase(Locale.ROOT)
        + TOPIC_SEPARATOR
        + suffix;
  }
}
