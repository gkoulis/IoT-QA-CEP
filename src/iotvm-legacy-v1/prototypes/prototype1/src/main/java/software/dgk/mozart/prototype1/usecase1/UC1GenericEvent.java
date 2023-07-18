package software.dgk.mozart.prototype1.usecase1;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A generic event.
 *
 * @author Dimitris Gkoulis
 * @createdAt Tuesday 9 February 2022
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
public class UC1GenericEvent implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String deviceId;
  private final String key;
  private final String hint;
  private final Map<String, Object> stringData;
  private final Map<String, Double> doubleData;
  private final Long timestamp;

  /* Constructs an instance for Jackson. */
  private UC1GenericEvent() {
    this.deviceId = null;
    this.key = null;
    this.hint = null;
    this.stringData = new HashMap<>();
    this.doubleData = new HashMap<>();
    this.timestamp = 0L;
  }

  public UC1GenericEvent(
      String deviceId,
      String key,
      String hint,
      Map<String, Object> stringData,
      Map<String, Double> doubleData,
      Long timestamp) {
    this.deviceId = deviceId;
    this.key = key;
    this.hint = hint;
    this.stringData = stringData;
    this.doubleData = doubleData;
    this.timestamp = timestamp;
  }

  public String getDeviceId() {
    return deviceId;
  }

  public String getKey() {
    return key;
  }

  public String getHint() {
    return hint;
  }

  public Map<String, Object> getStringData() {
    return stringData;
  }

  public Map<String, Double> getDoubleData() {
    return doubleData;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("UC1GenericEvent{");
    sb.append("deviceId='").append(deviceId).append('\'');
    sb.append(", key='").append(key).append('\'');
    sb.append(", hint='").append(hint).append('\'');
    sb.append(", stringData=").append(stringData);
    sb.append(", doubleData=").append(doubleData);
    sb.append(", timestamp=").append(timestamp);
    sb.append('}');
    return sb.toString();
  }
}
