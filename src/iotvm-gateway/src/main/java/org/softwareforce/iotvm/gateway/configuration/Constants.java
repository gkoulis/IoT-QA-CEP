package org.softwareforce.iotvm.gateway.configuration;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.softwareforce.iotvm.shared.event.SensorTelemetryEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryMeasurementsAverageEventIBO;
import org.softwareforce.iotvm.shared.event.SensorTelemetryRawEventIBO;

/**
 * Constants.
 *
 * @author Dimitris Gkoulis
 */
public final class Constants {

  /* ------------ Literals ------------ */

  public static final String UNDEFINED = "undefined";

  /** TODO Replace: get {@code Sensor} instances by greenhouse or area. */
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
