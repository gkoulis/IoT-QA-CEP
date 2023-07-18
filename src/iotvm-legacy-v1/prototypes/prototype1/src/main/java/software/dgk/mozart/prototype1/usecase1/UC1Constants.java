package software.dgk.mozart.prototype1.usecase1;

/**
 * Application constants.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 5 February 2022
 * @lastModifiedAt Tuesday 15 February 2022
 * @since 1.0.0-PROTOTYPE.1
 */
public final class UC1Constants {

  private static final String TOPIC_PREFIX = "uc1-";

  /**
   * A {@link String} to represent that JSON payload has no value (in order to avoid {@code null}).
   */
  public static final String JSON_NONE_VALUE = "NONE";

  /**
   * L4 Transport receives MQTT message and transforms it into a <i>sensor generic event</i> which
   * is a business event with a defined yet polymorphic schema.
   */
  public static final String SENSOR_TELEMETRY_EVENT_TOPIC = TOPIC_PREFIX + "sensor-telemetry-event";

  /** Temperature update event. */
  public static final String TEMPERATURE_UPDATE_EVENT_TOPIC =
      TOPIC_PREFIX + "temperature-update-event";

  /** Moisture (water vapor content in air) update event. */
  public static final String MOISTURE_UPDATE_EVENT_TOPIC = TOPIC_PREFIX + "moisture-update-event";

  /** Humidity (water vapor content in air, associated with temperature) update event. */
  public static final String HUMIDITY_UPDATE_EVENT_TOPIC = TOPIC_PREFIX + "humidity-update-event";

  private UC1Constants() {}

  public static String prefixedTopic(String topic) {
    return TOPIC_PREFIX + topic;
  }

  public static String prefixedVersionedTopic(String topic, int version) {
    return TOPIC_PREFIX + topic + "-" + version;
  }
}
