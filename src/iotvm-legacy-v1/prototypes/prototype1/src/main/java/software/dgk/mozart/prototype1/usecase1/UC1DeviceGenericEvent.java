package software.dgk.mozart.prototype1.usecase1;

import java.io.Serializable;

/**
 * A device generic event for holding a physical quantity or a metric if it can be represented as a
 * <i>string-double</i> key-value pair.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 5 February 2022
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
public final class UC1DeviceGenericEvent implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String deviceId;
  private final String key;
  private final Double value;
  private final String hint;
  private final Long timestamp;

  /* Constructs an instance for Jackson. */
  private UC1DeviceGenericEvent() {
    this.deviceId = null;
    this.key = null;
    this.value = 0D;
    this.hint = "NONE";
    this.timestamp = 0L;
  }

  public UC1DeviceGenericEvent(
      String deviceId, String key, Double value, String hint, Long timestamp) {
    this.deviceId = deviceId;
    this.key = key;
    this.value = value;
    this.hint = hint;
    this.timestamp = timestamp;
  }

  public String getDeviceId() {
    return deviceId;
  }

  public String getKey() {
    return key;
  }

  public Double getValue() {
    return value;
  }

  public String getHint() {
    return hint;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  //  public Instant getTimestampAsInstant() {
  //    return Instant.ofEpochMilli(this.timestamp);
  //  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("UC1DeviceGenericEvent{");
    sb.append("deviceId='").append(deviceId).append('\'');
    sb.append(", key='").append(key).append('\'');
    sb.append(", value=").append(value);
    sb.append(", hint='").append(hint).append('\'');
    sb.append(", timestamp=").append(timestamp);
    sb.append('}');
    return sb.toString();
  }
}
