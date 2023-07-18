package software.dgk.mozart.prototype1.usecase1;

import java.io.Serializable;

/**
 * A device specific event.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 5 February 2022
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
public final class UC1DeviceSpecificEvent implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String deviceId;
  private final Double temperature;
  private final String temperatureUnit;
  private final Double moisture;
  private final String moistureUnit;
  private final Long timestamp;

  /* Constructs an instance for Jackson. */
  private UC1DeviceSpecificEvent() {
    this.deviceId = null;
    this.temperature = 0D;
    this.temperatureUnit = "CELSIUS";
    this.moisture = 0D;
    this.moistureUnit = "PERCENTAGE";
    this.timestamp = 0L;
  }

  public UC1DeviceSpecificEvent(
      String deviceId,
      Double temperature,
      String temperatureUnit,
      Double moisture,
      String moistureUnit,
      Long timestamp) {
    this.deviceId = deviceId;
    this.temperature = temperature;
    this.temperatureUnit = temperatureUnit;
    this.moisture = moisture;
    this.moistureUnit = moistureUnit;
    this.timestamp = timestamp;
  }

  public String getDeviceId() {
    return deviceId;
  }

  public Double getTemperature() {
    return temperature;
  }

  public String getTemperatureUnit() {
    return temperatureUnit;
  }

  public Double getMoisture() {
    return moisture;
  }

  public String getMoistureUnit() {
    return moistureUnit;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  //  public Instant getTimestampAsInstant() {
  //    return Instant.ofEpochMilli(this.timestamp);
  //  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("UC1DeviceSpecificEvent{");
    sb.append("deviceId='").append(deviceId).append('\'');
    sb.append(", temperature=").append(temperature);
    sb.append(", temperatureUnit='").append(temperatureUnit).append('\'');
    sb.append(", moisture=").append(moisture);
    sb.append(", moistureUnit='").append(moistureUnit).append('\'');
    sb.append(", timestamp=").append(timestamp);
    sb.append('}');
    return sb.toString();
  }
}
